"""
dim_marketing_mapping — Fonte Unica da Verdade (Canonical)
===========================================================
Oficializa a dim_marketing_mapping como tabela mestra de atribuicao.

Etapas:
    1. DDL Canonica: garante schema correto (affiliate_id, tracker_id, source_name,
       partner_name, is_validated, evidence)
    2. Carga IDs Oficiais: 5 IDs da Pragmatic (is_validated=TRUE)
    3. Auditoria de Orfaos: Top 50 affiliate_ids NAO mapeados, por GGR,
       com inferencia de fonte via click IDs
    4. Limpeza de Nomes Estranhos: IDs suspeitos (test, bot, internal)
    5. Excel final para Marketing + resumo

Execucao:
    python pipelines/dim_marketing_mapping_canonical.py
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

HOJE = date.today().strftime("%Y-%m-%d")


# ======================================================================
# ETAPA 1: DDL CANONICA
# ======================================================================
def etapa1_ddl():
    """Recria a tabela com schema canonico. DROP + CREATE limpo."""
    log.info("=" * 70)
    log.info("ETAPA 1: DDL Canonica — dim_marketing_mapping")
    log.info("=" * 70)

    # Backup dos dados existentes antes de recriar
    log.info("Fazendo backup dos dados existentes...")
    try:
        execute_supernova("""
            CREATE TABLE IF NOT EXISTS multibet.dim_marketing_mapping_bkp_20260319
            AS SELECT * FROM multibet.dim_marketing_mapping
        """, fetch=False)
        rows_bkp = execute_supernova(
            "SELECT COUNT(*) FROM multibet.dim_marketing_mapping_bkp_20260319", fetch=True
        )
        log.info(f"  Backup: {rows_bkp[0][0]} registros salvos em dim_marketing_mapping_bkp_20260319")
    except Exception as e:
        log.warning(f"  Backup falhou (tabela pode nao existir): {e}")

    # Dropar views dependentes antes de recriar a tabela
    log.info("Dropando views dependentes...")
    execute_supernova("DROP VIEW IF EXISTS multibet.vw_cohort_roi CASCADE;", fetch=False)
    execute_supernova("DROP VIEW IF EXISTS multibet.vw_acquisition_channel CASCADE;", fetch=False)
    execute_supernova("DROP VIEW IF EXISTS multibet.vw_attribution_metrics CASCADE;", fetch=False)

    # Recriar tabela com schema canonico
    log.info("Recriando tabela com schema canonico...")
    execute_supernova("DROP TABLE IF EXISTS multibet.dim_marketing_mapping CASCADE;", fetch=False)

    execute_supernova("""
        CREATE TABLE multibet.dim_marketing_mapping (
            affiliate_id        VARCHAR(50)     NOT NULL,
            tracker_id          VARCHAR(255)    NOT NULL,
            source_name         VARCHAR(100)    NOT NULL,
            partner_name        VARCHAR(200),
            is_validated        BOOLEAN         NOT NULL DEFAULT FALSE,
            evidence            TEXT,

            -- Retrocompatibilidade: pipelines leem "source"
            -- Preenchida via INSERT (mesmo valor de source_name)
            source              VARCHAR(100),

            created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
            updated_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

            PRIMARY KEY (affiliate_id, tracker_id)
        );
    """, fetch=False)

    execute_supernova(
        "CREATE INDEX IF NOT EXISTS idx_dmm_tracker ON multibet.dim_marketing_mapping (tracker_id);",
        fetch=False,
    )
    execute_supernova(
        "CREATE INDEX IF NOT EXISTS idx_dmm_source ON multibet.dim_marketing_mapping (source_name);",
        fetch=False,
    )
    execute_supernova(
        "CREATE INDEX IF NOT EXISTS idx_dmm_validated ON multibet.dim_marketing_mapping (is_validated);",
        fetch=False,
    )

    log.info("  Tabela recriada com schema canonico.")
    log.info("  Colunas: affiliate_id, tracker_id, source_name, partner_name, is_validated, evidence, source (generated)")


# ======================================================================
# ETAPA 2: CARGA DOS IDS OFICIAIS (Pragmatic)
# ======================================================================

# IDs oficiais da Pragmatic — partner_name conforme imagem do arquiteto
OFFICIAL_IDS = [
    # (affiliate_id, tracker_id, source_name, partner_name, evidence)
    ("0", "0", "organic", "Direct/None",
     "ID oficial Pragmatic: Affiliate 0 = trafego organico/direto sem atribuicao"),
    ("0", "sem_tracker", "organic", "Direct/None",
     "Tracker vazio = acesso direto/organico, affiliate_id=0"),

    ("468114", "468114", "google_ads", "[IN] MULTIBET APP",
     "ID oficial Pragmatic: Google Ads — Affiliate Performance (conta principal app)"),

    ("297657", "297657", "google_ads", "elisa_google",
     "ID oficial Pragmatic: Google Ads — Multi-channel Principal (elisa_google). Confirmado forense: URLs com gclid="),

    ("445431", "445431", "google_ads", "[TIN] Google_Eyal",
     "ID oficial Pragmatic: Google Ads — Main Legacy (TIN Google_Eyal). Confirmado forense: URLs com gclid= e gad_source=1"),

    ("464673", "464673", "meta_ads", "[TIN] Meta White",
     "ID oficial Pragmatic: Meta Ads — Reactivation/Slots (TIN Meta White). Confirmado forense: URLs com fbclid="),
]


def etapa2_carga_oficiais():
    """Insere os IDs oficiais com is_validated=TRUE."""
    log.info("=" * 70)
    log.info("ETAPA 2: Carga dos IDs Oficiais (Pragmatic)")
    log.info("=" * 70)

    insert_sql = """
        INSERT INTO multibet.dim_marketing_mapping
            (affiliate_id, tracker_id, source_name, source, partner_name, is_validated, evidence)
        VALUES (%s, %s, %s, %s, %s, TRUE, %s)
        ON CONFLICT (affiliate_id, tracker_id) DO UPDATE SET
            source_name = EXCLUDED.source_name,
            source = EXCLUDED.source,
            partner_name = EXCLUDED.partner_name,
            is_validated = TRUE,
            evidence = EXCLUDED.evidence,
            updated_at = NOW()
    """

    # Expandir tuplas para incluir source = source_name
    records = [(a, t, s, s, p, e) for a, t, s, p, e in OFFICIAL_IDS]

    tunnel, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, insert_sql, records, page_size=100)
        conn.commit()
        log.info(f"  {len(records)} IDs oficiais inseridos (is_validated=TRUE)")
    finally:
        conn.close()
        tunnel.stop()

    # Mostrar o que foi inserido
    print("\n  IDs Oficiais inseridos:")
    print(f"  {'affiliate_id':<15} {'source_name':<15} {'partner_name':<25} {'validated'}")
    print("  " + "-" * 65)
    for aff, trk, src, partner, _ in OFFICIAL_IDS:
        print(f"  {aff:<15} {src:<15} {partner:<25} TRUE")


# ======================================================================
# ETAPA 3: AUDITORIA DE ORFAOS — Top 50 affiliate_ids por GGR
# ======================================================================

SQL_ORPHAN_AUDIT = """
WITH
-- Registros e GGR por affiliate_id (bireports_ec2 para registros, ps_bi.dim_user para GGR)
reg AS (
    SELECT
        CAST(c_affiliate_id AS VARCHAR) AS affiliate_id,
        MAX(COALESCE(NULLIF(c_affiliate_name, ''), 'N/A')) AS affiliate_name,
        COUNT(DISTINCT c_ecr_id) AS qty_players
    FROM bireports_ec2.tbl_ecr
    WHERE c_sign_up_time >= TIMESTAMP '2025-10-01'
    GROUP BY 1
),

-- Sinais de click ID para inferencia de fonte
url_signals AS (
    SELECT
        CAST(c_affiliate_id AS VARCHAR) AS affiliate_id,
        COUNT_IF(regexp_like(lower(c_reference_url), 'gclid='))    AS cnt_gclid,
        COUNT_IF(regexp_like(lower(c_reference_url), 'fbclid='))   AS cnt_fbclid,
        COUNT_IF(regexp_like(lower(c_reference_url), 'ttclid='))   AS cnt_ttclid,
        COUNT_IF(regexp_like(lower(c_reference_url), 'kclid|kwai'))AS cnt_kwai,
        COUNT_IF(regexp_like(lower(c_reference_url), 'utm_source='))AS cnt_utm,
        COUNT_IF(regexp_like(lower(c_reference_url), 'afp=|afp1=|afp2=')) AS cnt_afp,
        COUNT_IF(regexp_like(lower(c_reference_url), 'source_id='))AS cnt_source_id,
        COUNT(*)                                                    AS cnt_urls,
        MAX(REGEXP_EXTRACT(c_reference_url, 'utm_source=([^&]+)', 1)) AS utm_source_ex,
        MAX(REGEXP_EXTRACT(c_reference_url, 'utm_medium=([^&]+)', 1)) AS utm_medium_ex,
        MAX(REGEXP_EXTRACT(c_reference_url, 'utm_campaign=([^&]+)', 1)) AS utm_campaign_ex
    FROM ecr_ec2.tbl_ecr_banner
    WHERE c_affiliate_id IS NOT NULL
    GROUP BY 1
)

SELECT
    r.affiliate_id,
    r.affiliate_name,
    r.qty_players,

    -- Inferencia de fonte
    CASE
        WHEN s.cnt_gclid > 0 AND s.cnt_gclid >= COALESCE(s.cnt_fbclid, 0)
            THEN 'google_ads'
        WHEN s.cnt_fbclid > 0 AND s.cnt_fbclid >= COALESCE(s.cnt_gclid, 0)
            THEN 'meta_ads'
        WHEN s.cnt_ttclid > 0 THEN 'tiktok_ads'
        WHEN s.cnt_kwai > 0 THEN 'kwai_ads'
        WHEN s.cnt_afp > 0 THEN 'affiliate_performance'
        WHEN s.cnt_source_id > 0 THEN 'affiliate_direct'
        WHEN s.cnt_utm > 0 THEN 'paid_other'
        ELSE 'unknown'
    END AS suggested_source,

    -- Confianca da inferencia
    CASE
        WHEN s.cnt_gclid > 0 OR s.cnt_fbclid > 0 THEN 'High (click_id)'
        WHEN s.cnt_afp > 0 THEN 'High (AFP)'
        WHEN s.cnt_utm > 0 THEN 'Medium (UTM)'
        WHEN s.cnt_source_id > 0 THEN 'Medium (source_id)'
        ELSE 'Low (sem sinal)'
    END AS confidence,

    -- Evidencia
    CONCAT(
        COALESCE(r.affiliate_name, ''),
        ' | URLs: ', CAST(COALESCE(s.cnt_urls, 0) AS VARCHAR),
        CASE WHEN s.cnt_gclid > 0 THEN CONCAT(' | gclid:', CAST(s.cnt_gclid AS VARCHAR)) ELSE '' END,
        CASE WHEN s.cnt_fbclid > 0 THEN CONCAT(' | fbclid:', CAST(s.cnt_fbclid AS VARCHAR)) ELSE '' END,
        CASE WHEN s.cnt_ttclid > 0 THEN CONCAT(' | ttclid:', CAST(s.cnt_ttclid AS VARCHAR)) ELSE '' END,
        CASE WHEN s.cnt_kwai > 0 THEN CONCAT(' | kwai:', CAST(s.cnt_kwai AS VARCHAR)) ELSE '' END,
        CASE WHEN s.cnt_afp > 0 THEN CONCAT(' | afp:', CAST(s.cnt_afp AS VARCHAR)) ELSE '' END,
        CASE WHEN s.utm_source_ex IS NOT NULL THEN CONCAT(' | utm_source=', s.utm_source_ex) ELSE '' END,
        CASE WHEN s.utm_medium_ex IS NOT NULL THEN CONCAT(' | utm_medium=', s.utm_medium_ex) ELSE '' END,
        CASE WHEN s.utm_campaign_ex IS NOT NULL THEN CONCAT(' | utm_campaign=', s.utm_campaign_ex) ELSE '' END
    ) AS evidence

FROM reg r
LEFT JOIN url_signals s ON r.affiliate_id = s.affiliate_id
WHERE r.affiliate_id NOT IN ('0', '468114', '297657', '445431', '464673')
ORDER BY r.qty_players DESC
LIMIT 50
"""


def etapa3_auditoria_orfaos():
    """Top 50 affiliate_ids nao mapeados, por volume de players, com inferencia de fonte."""
    log.info("=" * 70)
    log.info("ETAPA 3: Auditoria de Orfaos — Top 50 por volume")
    log.info("=" * 70)

    df = query_athena(SQL_ORPHAN_AUDIT, database="bireports_ec2")
    log.info(f"  {len(df)} affiliate_ids encontrados")

    # Mostrar no console
    print(f"\n  {'#':<4} {'aff_id':<10} {'name':<25} {'players':>8} {'source_sugerido':<22} {'confianca'}")
    print("  " + "-" * 95)
    for i, (_, r) in enumerate(df.iterrows(), 1):
        print(f"  {i:<4} {str(r['affiliate_id']):<10} {str(r['affiliate_name'])[:24]:<25} {r['qty_players']:>8,} {str(r['suggested_source']):<22} {str(r['confidence'])}")

    return df


# ======================================================================
# ETAPA 4: LIMPEZA DE NOMES ESTRANHOS
# ======================================================================

SQL_SUSPICIOUS_NAMES = """
SELECT
    CAST(c_affiliate_id AS VARCHAR) AS affiliate_id,
    MAX(COALESCE(NULLIF(c_affiliate_name, ''), 'N/A')) AS affiliate_name,
    COUNT(DISTINCT c_ecr_id) AS qty_players,
    MAX(c_reference_url) AS sample_url
FROM ecr_ec2.tbl_ecr_banner
WHERE c_affiliate_id IS NOT NULL
  AND (
      regexp_like(lower(COALESCE(c_affiliate_name, '')), 'test|bot|internal|qa|dev|staging|sandbox|demo|fake|dummy')
      OR regexp_like(lower(COALESCE(NULLIF(TRIM(c_tracker_id), ''), '')),
                     'test|bot|internal|qa|dev|staging|sandbox|demo|fake|dummy')
      OR regexp_like(lower(COALESCE(c_reference_url, '')),
                     'test|internal|staging|sandbox|localhost')
  )
GROUP BY 1
ORDER BY 3 DESC
LIMIT 30
"""


def etapa4_limpeza_suspeitos():
    """Lista IDs com nomes suspeitos (test, bot, internal, etc.)."""
    log.info("=" * 70)
    log.info("ETAPA 4: Limpeza de Nomes Estranhos")
    log.info("=" * 70)

    df = query_athena(SQL_SUSPICIOUS_NAMES, database="ecr_ec2")
    log.info(f"  {len(df)} IDs suspeitos encontrados")

    if df.empty:
        print("  Nenhum ID suspeito encontrado.")
        return df

    print(f"\n  {'aff_id':<10} {'name':<30} {'players':>8} {'sample_url'}")
    print("  " + "-" * 100)
    for _, r in df.iterrows():
        url = str(r['sample_url'])[:50] if pd.notna(r['sample_url']) else 'N/A'
        print(f"  {str(r['affiliate_id']):<10} {str(r['affiliate_name'])[:28]:<30} {r['qty_players']:>8,} {url}")

    return df


# ======================================================================
# ETAPA 5: EXCEL FINAL PARA MARKETING
# ======================================================================

def etapa5_excel(df_orphans, df_suspicious):
    """Gera Excel consolidado para enviar ao Marketing."""
    log.info("=" * 70)
    log.info("ETAPA 5: Gerando Excel para Marketing")
    log.info("=" * 70)

    out_dir = "c:/Users/NITRO/OneDrive - PGX/MultiBet/output"
    os.makedirs(out_dir, exist_ok=True)
    out_file = f"{out_dir}/mapeamento_marketing_canonical_{HOJE}.xlsx"

    # Aba 1: Resumo
    # Buscar estado atual da tabela
    rows_current = execute_supernova("""
        SELECT affiliate_id, tracker_id, source_name, partner_name, is_validated, evidence
        FROM multibet.dim_marketing_mapping
        ORDER BY is_validated DESC, source_name, affiliate_id
    """, fetch=True)
    df_current = pd.DataFrame(
        rows_current or [],
        columns=["affiliate_id", "tracker_id", "source_name", "partner_name", "is_validated", "evidence"],
    )

    total_official = len(df_current[df_current["is_validated"] == True])
    total_orphans = len(df_orphans) if df_orphans is not None else 0
    total_suspicious = len(df_suspicious) if df_suspicious is not None else 0

    players_orphans = df_orphans["qty_players"].sum() if df_orphans is not None and not df_orphans.empty else 0

    resumo = pd.DataFrame({
        "Item": [
            "IDs Oficiais mapeados (is_validated=TRUE)",
            "Total na tabela dim_marketing_mapping",
            "Orfaos (Top 50 affiliate_ids sem mapeamento)",
            "Players nos orfaos (Top 50)",
            "IDs suspeitos (test/bot/internal)",
            "",
            "ACAO PARA O MARKETING",
            "1. Validar aba 'IDs Oficiais' — corretos?",
            "2. Classificar aba 'Orfaos Top 50' — qual fonte?",
            "3. Aprovar exclusao da aba 'Suspeitos' — excluir do ROI?",
            "",
            "Data de geracao",
        ],
        "Valor": [
            total_official,
            len(df_current),
            total_orphans,
            f"{players_orphans:,}",
            total_suspicious,
            "",
            "",
            "Confirmar affiliate_id, source e partner_name",
            "Preencher coluna 'source_confirmado' e 'partner_confirmado'",
            "Marcar 'excluir=SIM' para IDs de teste",
            "",
            HOJE,
        ],
    })

    # Preparar aba de orfaos com colunas para o Marketing preencher
    if df_orphans is not None and not df_orphans.empty:
        df_orphans_export = df_orphans.copy()
        df_orphans_export["source_confirmado"] = ""  # Marketing preenche
        df_orphans_export["partner_confirmado"] = ""  # Marketing preenche
        df_orphans_export["observacao_marketing"] = ""
    else:
        df_orphans_export = pd.DataFrame()

    # Preparar aba de suspeitos
    if df_suspicious is not None and not df_suspicious.empty:
        df_suspicious_export = df_suspicious.copy()
        df_suspicious_export["excluir_do_roi"] = ""  # Marketing preenche (SIM/NAO)
        df_suspicious_export["motivo"] = ""
    else:
        df_suspicious_export = pd.DataFrame()

    with pd.ExcelWriter(out_file, engine="openpyxl") as writer:
        resumo.to_excel(writer, sheet_name="Resumo", index=False)
        df_current.to_excel(writer, sheet_name="IDs Oficiais (Tabela)", index=False)
        if not df_orphans_export.empty:
            df_orphans_export.to_excel(writer, sheet_name="Orfaos Top 50 (GGR)", index=False)
        if not df_suspicious_export.empty:
            df_suspicious_export.to_excel(writer, sheet_name="Suspeitos (test-bot)", index=False)

    log.info(f"  Excel gerado: {out_file}")
    log.info(f"  Abas: Resumo | IDs Oficiais | Orfaos Top 50 | Suspeitos")

    return out_file


# ======================================================================
# RECRIAR VIEWS DEPENDENTES
# ======================================================================
def recriar_views():
    """Recria as views que dependem da dim_marketing_mapping."""
    log.info("Recriando views dependentes...")

    # vw_cohort_roi
    execute_supernova("""
        CREATE OR REPLACE VIEW multibet.vw_cohort_roi AS
        SELECT
            c.month_of_ftd,
            c.source,
            COUNT(*) AS qty_players,
            ROUND(AVG(c.ftd_amount)::numeric, 2) AS avg_ftd_amount,
            ROUND(AVG(c.ggr_d0)::numeric, 2) AS avg_ggr_d0,
            ROUND(AVG(c.ggr_d7)::numeric, 2) AS avg_ltv_d7,
            ROUND(AVG(c.ggr_d30)::numeric, 2) AS avg_ltv_d30,
            ROUND(SUM(c.ggr_d0)::numeric, 2) AS total_ggr_d0,
            ROUND(SUM(c.ggr_d7)::numeric, 2) AS total_ggr_d7,
            ROUND(SUM(c.ggr_d30)::numeric, 2) AS total_ggr_d30,
            ROUND(SUM(c.is_2nd_depositor)::numeric / NULLIF(COUNT(*), 0) * 100, 2) AS pct_2nd_deposit,
            s.monthly_spend,
            CASE WHEN s.monthly_spend > 0
                 THEN ROUND(SUM(c.ggr_d30)::numeric / s.monthly_spend * 100, 2)
                 ELSE NULL END AS roi_d30_pct,
            CASE WHEN SUM(c.ggr_d30) > 0
                 THEN ROUND(s.monthly_spend / SUM(c.ggr_d30)::numeric, 2)
                 ELSE NULL END AS payback_ratio
        FROM multibet.agg_cohort_acquisition c
        LEFT JOIN (
            SELECT
                TO_CHAR(a.dt, 'YYYY-MM') AS month_ref,
                COALESCE(m.source, 'unmapped_orphans') AS source,
                SUM(a.marketing_spend) AS monthly_spend
            FROM multibet.fact_attribution a
            LEFT JOIN multibet.dim_marketing_mapping m ON a.c_tracker_id = m.tracker_id
            GROUP BY 1, 2
        ) s ON c.month_of_ftd = s.month_ref AND c.source = s.source
        GROUP BY c.month_of_ftd, c.source, s.monthly_spend
        ORDER BY c.month_of_ftd DESC, total_ggr_d30 DESC
    """, fetch=False)
    log.info("  vw_cohort_roi recriada")

    # vw_acquisition_channel
    execute_supernova("""
        CREATE OR REPLACE VIEW multibet.vw_acquisition_channel AS
        SELECT
            a.dt,
            CASE
                WHEN COALESCE(m.source, 'unmapped_orphans') = 'organic' THEN 'Direct / Organic'
                WHEN COALESCE(m.source, 'unmapped_orphans') IN ('google_ads', 'meta_ads', 'tiktok_kwai', 'instagram') THEN 'Paid Media'
                WHEN COALESCE(m.source, 'unmapped_orphans') IN ('influencers', 'portais_midia', 'affiliate_performance') THEN 'Partnerships'
                ELSE 'Unmapped'
            END AS channel_tier,
            COALESCE(m.source, 'unmapped_orphans') AS source,
            SUM(a.qty_registrations) AS qty_registrations,
            SUM(a.qty_ftds) AS qty_ftds,
            SUM(a.ggr) AS ggr,
            SUM(a.marketing_spend) AS marketing_spend,
            CASE WHEN SUM(a.qty_registrations) > 0
                 THEN ROUND(SUM(a.qty_ftds)::numeric / SUM(a.qty_registrations) * 100, 2)
                 ELSE NULL END AS ftd_rate,
            CASE WHEN SUM(a.marketing_spend) > 0
                 THEN ROUND(SUM(a.ggr)::numeric / SUM(a.marketing_spend), 4)
                 ELSE NULL END AS roas
        FROM multibet.fact_attribution a
        LEFT JOIN multibet.dim_marketing_mapping m ON a.c_tracker_id = m.tracker_id
        GROUP BY 1, 2, 3
    """, fetch=False)
    log.info("  vw_acquisition_channel recriada")


# ======================================================================
# MAIN
# ======================================================================
def main():
    log.info("*" * 70)
    log.info("dim_marketing_mapping — CANONICAL PIPELINE")
    log.info(f"Data: {HOJE}")
    log.info("*" * 70)

    # 1. DDL
    etapa1_ddl()

    # 2. IDs Oficiais
    etapa2_carga_oficiais()

    # 2b. Recriar views dependentes
    recriar_views()

    # 3. Auditoria de Orfaos
    df_orphans = etapa3_auditoria_orfaos()

    # 4. Limpeza de Suspeitos
    df_suspicious = etapa4_limpeza_suspeitos()

    # 5. Excel
    excel_path = etapa5_excel(df_orphans, df_suspicious)

    # RESUMO FINAL
    print("\n" + "=" * 70)
    print("RESUMO EXECUTIVO — dim_marketing_mapping Canonical")
    print("=" * 70)

    rows_count = execute_supernova(
        "SELECT COUNT(*), SUM(CASE WHEN is_validated THEN 1 ELSE 0 END) FROM multibet.dim_marketing_mapping",
        fetch=True,
    )
    total, validated = rows_count[0] if rows_count else (0, 0)

    print(f"  Tabela: multibet.dim_marketing_mapping")
    print(f"  Total de registros:  {total}")
    print(f"  Oficiais (validated): {validated}")
    print(f"  Orfaos encontrados:  {len(df_orphans) if df_orphans is not None else 0}")
    print(f"  Suspeitos:           {len(df_suspicious) if df_suspicious is not None else 0}")
    print(f"\n  Excel: {excel_path}")
    print(f"\n  PROXIMO PASSO:")
    print(f"  1. Enviar Excel ao Marketing/Head")
    print(f"  2. Marketing preenche colunas 'source_confirmado' e 'partner_confirmado'")
    print(f"  3. Rodar carga dos confirmados na dim_marketing_mapping")
    print("=" * 70)


if __name__ == "__main__":
    main()
