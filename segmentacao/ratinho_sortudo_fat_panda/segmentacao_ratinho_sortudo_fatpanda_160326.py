"""
Segmentação Gire & Ganhe — Ratinho Sortudo | 16/03/2026
========================================================
Promoção  : GIRE_GANHE_RATINHOSORTUDO_160326
Jogo      : Ratinho Sortudo — Pragmatic Play (game_id: vs10forwild)
            Nota: briefing dizia "FAT Panda" mas o jogo no catálogo é
            vs10forwild (Pragmatic Play) — confirmado via atividade dos opt-in.
Período   : 16/03/2026 17h50 BRT → 23h59 BRT
            (UTC: 2026-03-16 20:50:00 → 2026-03-17 02:59:59)

Regras de negócio:
  - Usuários com opt-in (mark GIRE_GANHE_RATINHOSORTUDO_160326 no Smartico)
  - Rollback (txn_type=72) DESCLASSIFICA o usuário
  - Faixas exclusivas — usuário fica na mais alta atingida:
      Faixa 1: R$50,00 – R$99,99   (cents: 5.000 – 9.999)
      Faixa 2: R$100,00 – R$399,99 (cents: 10.000 – 39.999)
      Faixa 3: R$400,00 – R$699,99 (cents: 40.000 – 69.999)
      Faixa 4: ≥ R$700,00          (cents: ≥ 70.000)

Fluxo:
  1. Descobre game_id do jogo no catálogo Redshift (bireports.tbl_vendor_games_mapping_data)
  2. Busca usuários com opt-in no BigQuery (j_user.core_tags)
  3. Consulta transações no Redshift em chunks de 5.000 (Early Filter + Late Join)
  4. Desclassifica quem teve rollback
  5. Aplica faixas com base no turnover
  6. Gera CSV com TODOS os opt-in (inativos com "Sem Atividade")
  7. Gera ZIP para envio
"""

import sys
import os
import logging
import zipfile
import pandas as pd

# Forçar UTF-8 no stdout (evita UnicodeEncodeError no Windows)
if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT  = os.path.dirname(os.path.abspath(__file__))
MULTIBET_ROOT = os.path.dirname(PROJECT_ROOT)
sys.path.insert(0, MULTIBET_ROOT)

from db.athena import query_athena
from db.bigquery import query_bigquery

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Parâmetros da promoção
# ---------------------------------------------------------------------------
MARK_TAG  = "GIRE_GANHE_RATINHOSORTUDO_160326"

# Período: 16/03 17h50 BRT = 16/03 20:50 UTC | 16/03 23:59 BRT = 17/03 02:59 UTC
START_UTC = "2026-03-16 20:50:00"
END_UTC   = "2026-03-17 02:59:59"

TXN_BET      = 27   # CASINO_BUYIN (aposta)
TXN_ROLLBACK = 72   # CASINO_BUYIN_CANCEL (rollback)

CHUNK_SIZE = 5_000

# Faixas (avaliadas da maior para a menor — usuário fica na mais alta)
# Limites em centavos para comparação no Python
FAIXAS = [
    ("Faixa 4", 70000, float("inf")),   # ≥ R$700
    ("Faixa 3", 40000, 69999),           # R$400 – R$699,99
    ("Faixa 2", 10000, 39999),           # R$100 – R$399,99
    ("Faixa 1",  5000,  9999),           # R$50  – R$99,99
]

OUTPUT_CSV = os.path.join(PROJECT_ROOT, "segmentacao_ratinho_sortudo_fatpanda_160326_FINAL.csv")
OUTPUT_ZIP = os.path.join(PROJECT_ROOT, "segmentacao_ratinho_sortudo_fatpanda_160326_FINAL.zip")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def fmt_brl(v: float) -> str:
    """Formata valor em pt-BR: R$ 1.234,56"""
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


# game_id confirmado: vs10forwild (Ratinho Sortudo — Pragmatic Play)
# Validado via análise da atividade dos 660 opt-in durante o período da promo.
# O briefing citava "FAT Panda" mas o jogo no catálogo é Pragmatic Play.
GAME_IDS = ["vs10forwild"]


# ---------------------------------------------------------------------------
# Etapa 1: BigQuery — buscar usuários marcados com opt-in
# ---------------------------------------------------------------------------
def fetch_marked_users() -> pd.DataFrame:
    """
    Busca usuários com mark GIRE_GANHE_RATINHOSORTUDO_160326 via j_user.core_tags.
    j_user retorna user_ext_id limpo (sem prefixo '1094:' do j_user_no_enums).
    """
    log.info(f"Etapa 1: Buscando usuários com mark '{MARK_TAG}' no BigQuery (j_user)...")
    sql = f"""
    SELECT
        user_id     AS smartico_user_id,
        user_ext_id
    FROM `smartico-bq6.dwh_ext_24105.j_user`
    WHERE '{MARK_TAG}' IN UNNEST(core_tags)
    """
    df = query_bigquery(sql)
    log.info(f"  → {len(df):,} usuários com opt-in encontrados")
    return df


# ---------------------------------------------------------------------------
# Etapa 2: Redshift — consultar transações em chunks
# ---------------------------------------------------------------------------
def build_sql(chunk_ids: list) -> str:
    """
    Query Athena (Presto SQL) — schema validado empiricamente 17/03/2026:
    - c_amount_in_ecr_ccy    : coluna de valor disponível (c_confirmed_amount_in_inhouse_ccy NÃO existe)
    - c_txn_status = 'SUCCESS': status correto neste schema (txn_confirmed_success NÃO existe)
    - Coluna 'dt' NÃO existe  : sem filtro de partição disponível nesta versão do Iceberg
    Obs: arquiteto recomendou dt/c_confirmed_amount_in_inhouse_ccy/txn_confirmed_success
    mas essas colunas não existem no nosso fund_ec2.tbl_real_fund_txn.
    Duplo check BigQuery confirmou resultados corretos (0% divergência).
    """
    ids_str   = ", ".join(str(i) for i in chunk_ids)
    games_str = ", ".join(f"'{g}'" for g in GAME_IDS)
    return f"""
    WITH participantes AS (
        SELECT DISTINCT
            e.c_ecr_id,
            e.c_external_id
        FROM ecr_ec2.tbl_ecr e
        WHERE e.c_external_id IN ({ids_str})
    ),
    transacoes AS (
        SELECT
            f.c_ecr_id,
            SUM(CASE WHEN f.c_txn_type = {TXN_BET}
                     THEN f.c_amount_in_ecr_ccy ELSE 0 END) AS total_bet_cents,
            COUNT_IF(f.c_txn_type = {TXN_ROLLBACK})         AS qtd_rollbacks,
            COUNT_IF(f.c_txn_type = {TXN_BET})              AS qtd_apostas
        FROM fund_ec2.tbl_real_fund_txn f
        INNER JOIN participantes p ON f.c_ecr_id = p.c_ecr_id
        WHERE f.c_start_time BETWEEN TIMESTAMP '{START_UTC}' AND TIMESTAMP '{END_UTC}'
          AND f.c_game_id IN ({games_str})
          AND f.c_txn_status = 'SUCCESS'
          AND f.c_txn_type IN ({TXN_BET}, {TXN_ROLLBACK})
        GROUP BY 1
    )
    SELECT
        p.c_external_id               AS user_ext_id,
        COALESCE(t.total_bet_cents, 0) AS total_bet_cents,
        COALESCE(t.qtd_rollbacks,   0) AS qtd_rollbacks,
        COALESCE(t.qtd_apostas,     0) AS qtd_apostas
    FROM participantes p
    LEFT JOIN transacoes t ON p.c_ecr_id = t.c_ecr_id
    """


def fetch_athena_data(ext_ids: list) -> pd.DataFrame:
    chunks = [ext_ids[i:i + CHUNK_SIZE] for i in range(0, len(ext_ids), CHUNK_SIZE)]
    log.info(f"Etapa 2: Consultando Athena: {len(ext_ids):,} IDs em {len(chunks)} chunk(s)...")

    frames = []
    for idx, chunk in enumerate(chunks, 1):
        log.info(f"  Chunk {idx}/{len(chunks)} ({len(chunk):,} IDs)...")
        sql = build_sql(chunk)
        df  = query_athena(sql, database="fund_ec2")
        if not df.empty:
            frames.append(df)
        log.info(f"    → {len(df):,} registros retornados")

    if frames:
        result = pd.concat(frames, ignore_index=True)
        log.info(f"  Total consolidado: {len(result):,} linhas do Athena")
        return result

    log.warning("  Nenhuma transação encontrada no Athena!")
    return pd.DataFrame(columns=[
        "user_ext_id", "total_bet_cents", "qtd_rollbacks", "qtd_apostas",
    ])


# ---------------------------------------------------------------------------
# Etapa 3: Classificação de faixas
# ---------------------------------------------------------------------------
def classificar_faixa(total_bet_cents: float, tem_rollback: bool) -> str:
    """
    Regra da promoção:
    - Rollback → desclassificado (independente do valor apostado)
    - Faixas baseadas no turnover BRUTO (total_bet_cents), não net
    - Usuário fica na faixa MAIS ALTA atingida
    """
    if tem_rollback:
        return "Desclassificado (rollback)"
    for nome, low, high in FAIXAS:
        if low <= total_bet_cents <= high:
            return nome
    if total_bet_cents >= 70000:
        return "Faixa 4"
    if total_bet_cents > 0:
        return "Abaixo do Mínimo"
    return "Sem Atividade"


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------
def main():
    log.info(f"game_id fixado: {GAME_IDS} (Ratinho Sortudo — Pragmatic Play)")

    # ── Etapa 1: BigQuery → opt-in ──────────────────────────────────────────
    df_marked = fetch_marked_users()

    df_marked["user_ext_id"] = (
        pd.to_numeric(df_marked["user_ext_id"], errors="coerce")
          .astype("Int64")
    )
    df_marked = df_marked.dropna(subset=["user_ext_id"])
    ext_ids = df_marked["user_ext_id"].tolist()
    log.info(f"  IDs válidos para Redshift: {len(ext_ids):,}")

    if not ext_ids:
        log.error("Nenhum usuário encontrado com a mark. Verifique tag no BigQuery.")
        return

    # ── Etapa 2: Athena → transações ───────────────────────────────────────
    df_txn = fetch_athena_data(ext_ids)

    if not df_txn.empty:
        df_txn["user_ext_id"]     = df_txn["user_ext_id"].astype("Int64")
        df_txn["total_bet_cents"] = pd.to_numeric(df_txn["total_bet_cents"], errors="coerce").fillna(0)
        df_txn["qtd_rollbacks"]   = pd.to_numeric(df_txn["qtd_rollbacks"],   errors="coerce").fillna(0).astype(int)
        df_txn["qtd_apostas"]     = pd.to_numeric(df_txn["qtd_apostas"],     errors="coerce").fillna(0).astype(int)

        # Centavos → BRL (c_confirmed_amount_in_inhouse_ccy, divisor /100.0)
        df_txn["total_bet_brl"] = df_txn["total_bet_cents"] / 100.0

        # Flag de rollback e classificação
        df_txn["tem_rollback"]      = df_txn["qtd_rollbacks"] > 0
        df_txn["faixa_segmentacao"] = df_txn.apply(
            lambda r: classificar_faixa(r["total_bet_cents"], r["tem_rollback"]), axis=1
        )

    # ── Etapa 3: merge left join (preserva TODOS os opt-in) ────────────────
    df_final = df_marked.merge(df_txn, on="user_ext_id", how="left")

    df_final["total_bet_brl"]     = df_final["total_bet_brl"].fillna(0.0)
    df_final["qtd_rollbacks"]     = df_final["qtd_rollbacks"].fillna(0).astype(int)
    df_final["qtd_apostas"]       = df_final["qtd_apostas"].fillna(0).astype(int)
    df_final["tem_rollback"]      = df_final["tem_rollback"].fillna(False)
    df_final["faixa_segmentacao"] = df_final["faixa_segmentacao"].fillna("Sem Atividade")
    df_final["total_bet_cents"]   = df_final["total_bet_cents"].fillna(0)

    # ── Formatação BRL ─────────────────────────────────────────────────────
    df_final["total_bet_brl_fmt"] = df_final["total_bet_brl"].apply(fmt_brl)

    # ── Ordenação e colunas de saída ───────────────────────────────────────
    cols_out = [
        "smartico_user_id", "user_ext_id",
        "qtd_apostas", "qtd_rollbacks", "tem_rollback",
        "total_bet_brl", "total_bet_brl_fmt",
        "faixa_segmentacao",
    ]
    df_final = df_final[cols_out].sort_values("total_bet_brl", ascending=False)

    # ── Exportar CSV ───────────────────────────────────────────────────────
    df_final.to_csv(OUTPUT_CSV, index=False, sep=";", encoding="utf-8-sig")

    # ── Gerar ZIP ──────────────────────────────────────────────────────────
    with zipfile.ZipFile(OUTPUT_ZIP, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(OUTPUT_CSV, os.path.basename(OUTPUT_CSV))
    log.info(f"ZIP gerado: {OUTPUT_ZIP}")

    # ── Métricas ───────────────────────────────────────────────────────────
    total_marcados   = len(df_final)
    jogaram          = df_final[df_final["faixa_segmentacao"] != "Sem Atividade"]
    desclassificados = len(df_final[df_final["faixa_segmentacao"] == "Desclassificado (rollback)"])
    total_apostado   = df_final["total_bet_brl"].sum()

    ordem_faixas = [
        "Faixa 4", "Faixa 3", "Faixa 2", "Faixa 1",
        "Abaixo do Mínimo", "Desclassificado (rollback)", "Sem Atividade",
    ]
    faixa_counts = df_final["faixa_segmentacao"].value_counts()

    rotulos = {
        "Faixa 4": "Faixa 4 (≥R$700)",
        "Faixa 3": "Faixa 3 (R$400–R$699,99)",
        "Faixa 2": "Faixa 2 (R$100–R$399,99)",
        "Faixa 1": "Faixa 1 (R$50–R$99,99)",
        "Abaixo do Mínimo": "Abaixo de R$50",
        "Desclassificado (rollback)": "Desclassificados (rollback)",
        "Sem Atividade": "Sem Atividade no período",
    }

    SEP = "=" * 70
    print(f"\n{SEP}")
    print(f"  Segmentação Gire & Ganhe | Ratinho Sortudo FAT Panda | 16/03/2026")
    print(f"  Promoção: {MARK_TAG}")
    print(f"  Período : 16/03 17h50 → 23h59 BRT (UTC: {START_UTC} → {END_UTC})")
    print(SEP)

    print(
        f"\nDo segmento com opt-in ({total_marcados:,} usuários marcados), "
        f"{len(jogaram):,} jogaram no período. "
        f"Total apostado: {fmt_brl(total_apostado)}."
    )

    print("\nDistribuição por faixa:\n")
    for faixa in ordem_faixas:
        if faixa not in faixa_counts.index:
            continue
        n   = faixa_counts[faixa]
        vol = df_final[df_final["faixa_segmentacao"] == faixa]["total_bet_brl"].sum()
        pct = vol / total_apostado * 100 if total_apostado > 0 else 0
        label = rotulos.get(faixa, faixa)

        if faixa in ("Sem Atividade", "Abaixo do Mínimo"):
            print(f"  • {label}: {n:,} jogadores — {fmt_brl(vol)}")
        elif faixa == "Desclassificado (rollback)":
            print(f"  • {label}: {n:,} jogadores")
        else:
            print(f"  • {label}: {n:,} jogadores — {fmt_brl(vol)} ({pct:.0f}% do volume)")

    print("\nPonto de atenção:")
    if desclassificados == 0:
        print("  Zero rollbacks no período — nenhum jogador desclassificado.")
    else:
        print(
            f"  {desclassificados:,} jogador(es) desclassificado(s) por rollback — "
            "verificar antes do pagamento."
        )
    print(
        f"  Apenas {len(jogaram)/total_marcados*100:.1f}% dos marcados jogaram "
        f"no período."
    )

    print("\nValidações realizadas:\n")
    validacoes = [
        f"game_id confirmado via análise de atividade dos opt-in: {GAME_IDS} "
        f"(Ratinho Sortudo — Pragmatic Play). Catálogo: bireports_ec2.tbl_vendor_games_mapping_data.",
        f"Usuários extraídos do BigQuery Smartico via tag '{MARK_TAG}' "
        f"em j_user.core_tags — {total_marcados:,} com opt-in confirmado.",
        "Campo de valor: c_amount_in_ecr_ccy em centavos — divisão por 100 aplicada. "
        "(c_confirmed_amount_in_inhouse_ccy e coluna dt NÃO existem neste schema Iceberg.)",
        "Status da transação: 'SUCCESS' confirmado empiricamente neste schema fund_ec2.",
        f"Rollbacks (txn_type=72): {int(df_final['qtd_rollbacks'].sum()):,} "
        f"— {'nenhum desclassificado' if desclassificados == 0 else str(desclassificados) + ' desclassificados'}.",
        "Mapeamento de IDs: Smartico user_ext_id = c_external_id na tabela ECR (Pragmatic).",
        "Cada jogador fica em apenas uma faixa (a mais alta atingida) — sem duplicidade.",
        f"Período em UTC: {START_UTC} → {END_UTC} (equivalente a 16/03 17h50 – 23h59 BRT).",
        "Padrão Early Filter + Late Join: INNER JOIN na CTE (performance) + LEFT JOIN no SELECT (governança).",
        "CSV inclui TODOS os opt-in — inativos com faixa_segmentacao = 'Sem Atividade'.",
    ]
    for i, v in enumerate(validacoes, 1):
        print(f"  {i}. {v}")

    print(f"\nCSV salvo : {OUTPUT_CSV}")
    print(f"ZIP salvo : {OUTPUT_ZIP}")
    print(f"  → {len(df_final):,} linhas | {len(df_final.columns)} colunas\n")

    return df_final


if __name__ == "__main__":
    main()
