"""
Pipeline: dim_marketing_mapping — Tabela Mestra de Atribuicao (v2)
===================================================================
Profissionaliza a arquitetura de atribuicao do MultiBet.

IMPORTANTE: Este pipeline NAO dropa a tabela existente!
Faz ALTER TABLE para adicionar colunas novas e migra dados legados.

Etapas:
    1. ALTER TABLE: adiciona colunas v2 (affiliate_id, source_name, etc.)
    2. Migra dados legado: campaign_name -> partner_name, source -> source_name, etc.
    3. Descobre NOVOS IDs forenses via Athena (que nao estao na tabela ainda)
    4. Gera relatorio Excel de trackers Unmapped (para o Marketing validar)

Schema v1 (existente, 30 registros):
    tracker_id, campaign_name, source, confidence, mapping_logic

Schema v2 (adicionado):
    + affiliate_id, source_name, partner_name, evidence_logic, is_validated

Retrocompatibilidade: colunas v1 sao MANTIDAS (nao removidas).

Execucao:
    python pipelines/dim_marketing_mapping.py

Destino: Super Nova DB -> multibet.dim_marketing_mapping
Saida:   output/unmapped_trackers_para_marketing_YYYY-MM-DD.xlsx
"""

import sys
import os
import logging
from datetime import date

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.athena import query_athena
from db.supernova import execute_supernova, get_supernova_connection
import psycopg2.extras
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ======================================================================
# 1. DDL — ALTER TABLE (migracao segura, sem DROP)
# ======================================================================
ALTER_STATEMENTS = [
    # Adicionar novas colunas (idempotente)
    "ALTER TABLE multibet.dim_marketing_mapping ADD COLUMN IF NOT EXISTS affiliate_id   VARCHAR(50);",
    "ALTER TABLE multibet.dim_marketing_mapping ADD COLUMN IF NOT EXISTS source_name    VARCHAR(100);",
    "ALTER TABLE multibet.dim_marketing_mapping ADD COLUMN IF NOT EXISTS partner_name   VARCHAR(200);",
    "ALTER TABLE multibet.dim_marketing_mapping ADD COLUMN IF NOT EXISTS evidence_logic TEXT;",
    "ALTER TABLE multibet.dim_marketing_mapping ADD COLUMN IF NOT EXISTS is_validated   BOOLEAN DEFAULT FALSE;",
    "ALTER TABLE multibet.dim_marketing_mapping ADD COLUMN IF NOT EXISTS created_at     TIMESTAMPTZ DEFAULT NOW();",
    "ALTER TABLE multibet.dim_marketing_mapping ADD COLUMN IF NOT EXISTS updated_at     TIMESTAMPTZ DEFAULT NOW();",
]

MIGRATE_STATEMENTS = [
    # source_name <- source (coluna legado)
    "UPDATE multibet.dim_marketing_mapping SET source_name = source WHERE source_name IS NULL AND source IS NOT NULL;",
    # partner_name <- campaign_name (coluna legado)
    "UPDATE multibet.dim_marketing_mapping SET partner_name = campaign_name WHERE partner_name IS NULL AND campaign_name IS NOT NULL;",
    # evidence_logic <- mapping_logic (coluna legado)
    "UPDATE multibet.dim_marketing_mapping SET evidence_logic = mapping_logic WHERE evidence_logic IS NULL AND mapping_logic IS NOT NULL;",
    # affiliate_id <- tracker_id (default, sera ajustado manualmente depois)
    "UPDATE multibet.dim_marketing_mapping SET affiliate_id = tracker_id WHERE affiliate_id IS NULL;",
    # is_validated: TRUE se confidence contem 'Official'
    """UPDATE multibet.dim_marketing_mapping
       SET is_validated = CASE
           WHEN confidence LIKE '%Official%' THEN TRUE
           ELSE FALSE
       END
       WHERE is_validated IS NULL;""",
    # Garantir NOT NULL
    "UPDATE multibet.dim_marketing_mapping SET affiliate_id = tracker_id WHERE affiliate_id IS NULL;",
    "UPDATE multibet.dim_marketing_mapping SET source_name = COALESCE(source, 'unmapped') WHERE source_name IS NULL;",
    "UPDATE multibet.dim_marketing_mapping SET is_validated = FALSE WHERE is_validated IS NULL;",
    "UPDATE multibet.dim_marketing_mapping SET updated_at = NOW();",
]

INDEX_STATEMENTS = [
    "CREATE INDEX IF NOT EXISTS idx_dmm_source_name ON multibet.dim_marketing_mapping (source_name);",
    "CREATE INDEX IF NOT EXISTS idx_dmm_validated   ON multibet.dim_marketing_mapping (is_validated);",
    "CREATE INDEX IF NOT EXISTS idx_dmm_aff_id      ON multibet.dim_marketing_mapping (affiliate_id);",
]


# ======================================================================
# 2. Descoberta Forense — Athena: click IDs novos nao mapeados ainda
#    Exclui todos os tracker_ids que JA existem na tabela
# ======================================================================
SQL_FORENSIC_DISCOVERY = """
WITH
banner_sinais AS (
    SELECT
        CAST(c_affiliate_id AS VARCHAR) AS affiliate_id,
        COALESCE(NULLIF(TRIM(c_tracker_id), ''), CAST(c_affiliate_id AS VARCHAR)) AS tracker_id,

        COUNT_IF(regexp_like(lower(c_reference_url), 'gclid='))               AS cnt_gclid,
        COUNT_IF(regexp_like(lower(c_reference_url), 'fbclid='))              AS cnt_fbclid,
        COUNT_IF(regexp_like(lower(c_reference_url), 'ttclid='))              AS cnt_ttclid,
        COUNT_IF(regexp_like(lower(c_reference_url), 'kclid|kwai'))           AS cnt_kwai,
        COUNT(*)                                                               AS cnt_total,

        MAX(REGEXP_EXTRACT(c_reference_url, 'utm_source=([^&]+)', 1))         AS utm_source_ex,
        MAX(REGEXP_EXTRACT(c_reference_url, 'utm_medium=([^&]+)', 1))         AS utm_medium_ex,
        MAX(NULLIF(c_affiliate_name, ''))                                      AS affiliate_name

    FROM ecr_ec2.tbl_ecr_banner
    WHERE c_affiliate_id IS NOT NULL
      AND c_reference_url IS NOT NULL
      AND c_reference_url <> ''
    GROUP BY 1, 2
)

SELECT
    affiliate_id,
    tracker_id,
    affiliate_name,

    CASE
        WHEN cnt_gclid > 0 AND cnt_gclid >= cnt_fbclid
             AND cnt_gclid >= cnt_ttclid AND cnt_gclid >= cnt_kwai
            THEN 'google_ads'
        WHEN cnt_fbclid > 0 AND cnt_fbclid >= cnt_ttclid
             AND cnt_fbclid >= cnt_kwai
            THEN 'meta_ads'
        WHEN cnt_ttclid > 0 AND cnt_ttclid >= cnt_kwai
            THEN 'tiktok_ads'
        WHEN cnt_kwai > 0
            THEN 'kwai_ads'
        ELSE NULL
    END AS inferred_source,

    CONCAT(
        'Forense auto (', CAST(cnt_total AS VARCHAR), ' regs): ',
        CASE WHEN cnt_gclid  > 0 THEN CONCAT(CAST(cnt_gclid  AS VARCHAR), ' gclid, ')  ELSE '' END,
        CASE WHEN cnt_fbclid > 0 THEN CONCAT(CAST(cnt_fbclid AS VARCHAR), ' fbclid, ') ELSE '' END,
        CASE WHEN cnt_ttclid > 0 THEN CONCAT(CAST(cnt_ttclid AS VARCHAR), ' ttclid, ') ELSE '' END,
        CASE WHEN cnt_kwai   > 0 THEN CONCAT(CAST(cnt_kwai   AS VARCHAR), ' kwai, ')   ELSE '' END,
        'utm_source=', COALESCE(utm_source_ex, 'N/A'),
        ', utm_medium=', COALESCE(utm_medium_ex, 'N/A')
    ) AS evidence_logic

FROM banner_sinais
WHERE cnt_gclid > 0 OR cnt_fbclid > 0 OR cnt_ttclid > 0 OR cnt_kwai > 0
ORDER BY cnt_total DESC
"""


# ======================================================================
# 3. Relatorio Unmapped — trackers/affiliates sem mapeamento, com GGR
# ======================================================================
SQL_UNMAPPED_REPORT = """
WITH
activity AS (
    SELECT
        COALESCE(NULLIF(TRIM(e.c_tracker_id), ''), CAST(e.c_affiliate_id AS VARCHAR), 'sem_tracker') AS tracker_id,
        CAST(e.c_affiliate_id AS VARCHAR) AS affiliate_id,
        MAX(COALESCE(NULLIF(e.c_affiliate_name, ''), 'N/A')) AS affiliate_name,
        COUNT(DISTINCT e.c_ecr_id) AS qty_players
    FROM bireports_ec2.tbl_ecr e
    WHERE e.c_sign_up_time >= TIMESTAMP '2025-10-01'
    GROUP BY 1, 2
),

ggr AS (
    SELECT
        COALESCE(NULLIF(TRIM(u.c_tracker_id), ''), CAST(u.c_affiliate_id AS VARCHAR), 'sem_tracker') AS tracker_id,
        CAST(u.c_affiliate_id AS VARCHAR) AS affiliate_id,
        SUM(p.ggr) AS total_ggr
    FROM ps_bi.fct_player_activity_daily p
    JOIN ps_bi.dim_user u ON p.user_id = u.user_id
    WHERE p.activity_date >= DATE '2025-10-01'
    GROUP BY 1, 2
)

SELECT
    a.tracker_id,
    a.affiliate_id,
    a.affiliate_name,
    a.qty_players,
    COALESCE(g.total_ggr, 0) AS total_ggr
FROM activity a
LEFT JOIN ggr g ON a.tracker_id = g.tracker_id AND a.affiliate_id = g.affiliate_id
ORDER BY total_ggr DESC
"""


# ======================================================================
# Funcoes de execucao
# ======================================================================

def migrate_schema():
    """Adiciona colunas v2 e migra dados legados. NAO dropa nada."""
    log.info("ETAPA 1: ALTER TABLE — adicionando colunas v2...")

    for stmt in ALTER_STATEMENTS:
        try:
            execute_supernova(stmt, fetch=False)
        except Exception as e:
            # Coluna ja existe = OK, outro erro = propaga
            if "already exists" in str(e).lower() or "duplicate column" in str(e).lower():
                log.info(f"  Coluna ja existe, OK: {stmt[:60]}...")
            else:
                raise

    log.info("ETAPA 2: Migrando dados legados para colunas v2...")
    for stmt in MIGRATE_STATEMENTS:
        execute_supernova(stmt, fetch=False)

    log.info("ETAPA 3: Criando indices...")
    for stmt in INDEX_STATEMENTS:
        execute_supernova(stmt, fetch=False)

    # Contar registros migrados
    rows = execute_supernova(
        "SELECT COUNT(*) FROM multibet.dim_marketing_mapping WHERE source_name IS NOT NULL",
        fetch=True,
    )
    migrated = rows[0][0] if rows else 0
    log.info(f"  {migrated} registros migrados com sucesso.")
    return migrated


def discover_and_insert_new_forensic():
    """Descobre IDs forenses no Athena que NAO estao na tabela ainda."""
    log.info("ETAPA 4: Descobrindo novos IDs forenses via Athena...")

    # Buscar tracker_ids JA mapeados
    rows_mapped = execute_supernova(
        "SELECT DISTINCT tracker_id FROM multibet.dim_marketing_mapping",
        fetch=True,
    )
    mapped_set = {r[0] for r in (rows_mapped or [])}
    log.info(f"  Trackers ja mapeados no DB: {len(mapped_set)}")

    # Buscar todos os sinais forenses do Athena
    df = query_athena(SQL_FORENSIC_DISCOVERY, database="ecr_ec2")
    df = df[df["inferred_source"].notna()].copy()
    log.info(f"  Sinais forenses encontrados no Athena: {len(df)}")

    # Filtrar apenas os NOVOS (que nao estao mapeados)
    df_new = df[~df["tracker_id"].isin(mapped_set)].copy()
    log.info(f"  Novos (nao mapeados): {len(df_new)}")

    if df_new.empty:
        log.info("  Nenhum ID forense novo encontrado. Todos ja estao mapeados!")
        return 0

    insert_sql = """
        INSERT INTO multibet.dim_marketing_mapping
            (tracker_id, affiliate_id, source_name, source, partner_name,
             evidence_logic, is_validated, confidence, mapping_logic, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, FALSE, %s, %s, NOW(), NOW())
        ON CONFLICT (tracker_id) DO NOTHING
    """

    records = []
    for _, row in df_new.iterrows():
        tracker = str(row["tracker_id"])[:255]
        aff_id = str(row["affiliate_id"])[:50]
        src = str(row["inferred_source"])
        name = str(row["affiliate_name"])[:200] if pd.notna(row.get("affiliate_name")) else None
        evidence = str(row["evidence_logic"])[:2000] if pd.notna(row.get("evidence_logic")) else None
        confidence = "Medium (Auto-Forensic)"

        records.append((
            tracker,        # tracker_id
            aff_id,         # affiliate_id
            src,            # source_name
            src,            # source (legado, mesmo valor)
            name,           # partner_name
            evidence,       # evidence_logic
            confidence,     # confidence (legado)
            evidence,       # mapping_logic (legado, mesmo valor)
        ))

    tunnel, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, insert_sql, records, page_size=500)
        conn.commit()
        log.info(f"  {len(records)} novos IDs forenses inseridos (is_validated=FALSE).")
    finally:
        conn.close()
        tunnel.stop()

    return len(records)


def generate_unmapped_report():
    """Gera relatorio Excel dos trackers/affiliates ainda sem mapeamento."""
    log.info("ETAPA 5: Gerando relatorio Unmapped para Marketing...")

    # Buscar toda atividade no Athena
    df_activity = query_athena(SQL_UNMAPPED_REPORT, database="bireports_ec2")
    log.info(f"  Combinacoes tracker/affiliate com atividade: {len(df_activity)}")

    # Buscar IDs ja mapeados
    rows_mapped = execute_supernova(
        "SELECT DISTINCT tracker_id FROM multibet.dim_marketing_mapping",
        fetch=True,
    )
    mapped_set = {r[0] for r in (rows_mapped or [])}
    log.info(f"  IDs ja mapeados: {len(mapped_set)}")

    # Filtrar unmapped
    df_unmapped = df_activity[~df_activity["tracker_id"].isin(mapped_set)].copy()
    df_unmapped = df_unmapped.sort_values("total_ggr", ascending=False)
    log.info(f"  Trackers UNMAPPED: {len(df_unmapped)}")

    if df_unmapped.empty:
        log.info("  COBERTURA 100%! Todos os trackers estao mapeados!")
        return None

    # Calcular impacto
    ggr_unmapped = df_unmapped["total_ggr"].sum()
    ggr_total = df_activity["total_ggr"].sum()
    pct_unmapped = (ggr_unmapped / ggr_total * 100) if ggr_total > 0 else 0

    log.info(f"  GGR Unmapped: R$ {ggr_unmapped:,.2f} ({pct_unmapped:.1f}% do total)")

    # Salvar Excel
    out_dir = "c:/Users/NITRO/OneDrive - PGX/MultiBet/output"
    os.makedirs(out_dir, exist_ok=True)
    hoje = date.today().strftime("%Y-%m-%d")
    out_file = f"{out_dir}/unmapped_trackers_para_marketing_{hoje}.xlsx"

    df_unmapped["pct_ggr"] = (
        df_unmapped["total_ggr"] / ggr_total * 100
    ).round(2)

    # Aba: Resumo executivo
    resumo = pd.DataFrame({
        "Metrica": [
            "Total de trackers/affiliates com atividade",
            "Trackers JA mapeados (dim_marketing_mapping)",
            "Trackers UNMAPPED (neste arquivo)",
            "GGR Total (desde Out/2025)",
            "GGR dos Unmapped",
            "% GGR sem atribuicao",
            "Data de geracao",
            "",
            "ACAO NECESSARIA",
        ],
        "Valor": [
            f"{len(df_activity):,}",
            f"{len(mapped_set):,}",
            f"{len(df_unmapped):,}",
            f"R$ {ggr_total:,.2f}",
            f"R$ {ggr_unmapped:,.2f}",
            f"{pct_unmapped:.1f}%",
            hoje,
            "",
            "Favor validar os trackers na aba 'Top 20 Prioridade' e informar a fonte correta (Google Ads, Meta, Afiliado, etc.)",
        ],
    })

    # Aba: mapeamento atual (para referencia do Marketing)
    rows_current = execute_supernova(
        """SELECT tracker_id, source_name, partner_name, is_validated, evidence_logic
           FROM multibet.dim_marketing_mapping
           ORDER BY source_name, tracker_id""",
        fetch=True,
    )
    df_current = pd.DataFrame(
        rows_current or [],
        columns=["tracker_id", "source_name", "partner_name", "is_validated", "evidence_logic"],
    )

    with pd.ExcelWriter(out_file, engine="openpyxl") as writer:
        resumo.to_excel(writer, sheet_name="Resumo", index=False)
        df_unmapped.to_excel(writer, sheet_name="Unmapped por GGR", index=False)
        df_unmapped.head(20).to_excel(writer, sheet_name="Top 20 Prioridade", index=False)
        df_current.to_excel(writer, sheet_name="Mapeamento Atual", index=False)

    log.info(f"  Excel gerado: {out_file}")
    return out_file


def validate():
    """Validacao pos-migracao: mostra distribuicao completa."""
    rows = execute_supernova("""
        SELECT
            COALESCE(source_name, source, 'SEM FONTE') AS fonte,
            is_validated,
            COUNT(*) AS qty
        FROM multibet.dim_marketing_mapping
        GROUP BY 1, 2
        ORDER BY 1, 2
    """, fetch=True)

    total = sum(r[2] for r in rows)
    validated = sum(r[2] for r in rows if r[1] is True)
    forensic = total - validated

    print("\n" + "=" * 65)
    print("VALIDACAO — dim_marketing_mapping (pos-migracao)")
    print("=" * 65)
    print(f"{'source':<25} {'validated':<12} {'qty':>6}")
    print("-" * 47)
    for r in rows:
        print(f"{str(r[0])[:25]:<25} {str(r[1]):<12} {r[2]:>6}")
    print("-" * 47)
    print(f"{'TOTAL':<25} {'':<12} {total:>6}")
    print(f"  Oficiais (validated=TRUE):  {validated}")
    print(f"  Forenses (validated=FALSE): {forensic}")
    print("=" * 65)

    return total


# ======================================================================
# Main
# ======================================================================
def main():
    log.info("=" * 65)
    log.info("Pipeline dim_marketing_mapping v2 — Migracao + Enriquecimento")
    log.info("=" * 65)

    # 1. Migrar schema (ALTER TABLE, sem DROP)
    migrated = migrate_schema()

    # 2. Descobrir e inserir NOVOS IDs forenses
    qty_new = discover_and_insert_new_forensic()

    # 3. Validar estado final
    total = validate()

    # 4. Gerar relatorio Unmapped
    excel_path = generate_unmapped_report()

    # Resumo final
    print("\n" + "=" * 65)
    print("RESUMO EXECUTIVO")
    print("=" * 65)
    print(f"  Registros migrados (legado):    {migrated}")
    print(f"  Novos IDs forenses adicionados: {qty_new}")
    print(f"  Total na tabela:                {total}")
    if excel_path:
        print(f"\n  Relatorio Unmapped: {excel_path}")
        print("  -> Enviar ao Marketing para validacao dos trackers pendentes!")
    else:
        print("\n  COBERTURA 100%! Nenhum tracker unmapped.")
    print("=" * 65)


if __name__ == "__main__":
    main()
