"""
Segmentação Promo Relâmpago — Sugar Rush | 17/03/2026
=====================================================
Promoção  : PROMO_RELAMPAGO_170326
Jogo      : Sugar Rush — Pragmatic Play (game_id: vs20sugarrush, 204)
Período   : 17/03/2026 15h BRT → 19h BRT
            (UTC: 2026-03-17 18:00:00 → 2026-03-17 22:00:00)

Regras de negócio:
  - Usuários com opt-in (mark PROMO_RELAMPAGO_170326 no Smartico)
  - Rollback (txn_type=72) DESCLASSIFICA o usuário
  - Apostas ACUMULADAS entre variantes Sugar Rush (vs20sugarrush + 204)
  - Faixas exclusivas — usuário fica na mais alta atingida:
      Faixa 1: R$30,00 – R$99,99   (cents: 3.000 – 9.999)
      Faixa 2: R$100,00 – R$299,99 (cents: 10.000 – 29.999)
      Faixa 3: R$300,00 – R$499,99 (cents: 30.000 – 49.999)
      Faixa 4: ≥ R$500,00          (cents: ≥ 50.000)

Fluxo:
  1. BigQuery → busca usuários com opt-in (j_user.core_tags)
  2. Athena  → transações em chunks de 5.000 (Early Filter + Late Join)
  3. Desclassifica quem teve rollback
  4. Aplica faixas com base no turnover bruto acumulado
  5. Gera CSV + ZIP com TODOS os opt-in (inativos com "Sem Atividade")
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
MULTIBET_ROOT = os.path.dirname(os.path.dirname(PROJECT_ROOT))
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
MARK_TAG = "PROMO_RELAMPAGO_170326"

# Sugar Rush (Pragmatic Play) — 2 variantes no catálogo Athena
# vs20sugarrush = Pragmatic Play nativo | 204 = alea_pragmaticplay (mesmo jogo)
GAME_IDS = ["vs20sugarrush", "204"]
GAME_NAMES = {
    "vs20sugarrush": "Sugar Rush (Pragmatic Play)",
    "204": "Sugar Rush (alea_pragmaticplay)",
}

# Sugar Rush no catálogo Smartico (BigQuery) — para validação cruzada
SMR_GAME_ID = 45928724  # SUGAR RUSH no dm_casino_game_name

# Período: 17/03 15h BRT = 17/03 18:00 UTC | 17/03 19h BRT = 17/03 22:00 UTC
START_UTC = "2026-03-17 18:00:00"
END_UTC   = "2026-03-17 22:00:00"

TXN_BET      = 27   # CASINO_BUYIN (aposta)
TXN_ROLLBACK = 72   # CASINO_BUYIN_CANCEL (rollback)

CHUNK_SIZE = 5_000

# Faixas (avaliadas da maior para a menor — usuário fica na mais alta)
# Limites em centavos para comparação no Python
FAIXAS = [
    ("Faixa 4", 50000, float("inf")),   # ≥ R$500
    ("Faixa 3", 30000, 49999),           # R$300 – R$499,99
    ("Faixa 2", 10000, 29999),           # R$100 – R$299,99
    ("Faixa 1",  3000,  9999),           # R$30  – R$99,99
]

OUTPUT_CSV = os.path.join(PROJECT_ROOT, "segmentacao_promo_relampago_170326_FINAL.csv")
OUTPUT_ZIP = os.path.join(PROJECT_ROOT, "segmentacao_promo_relampago_170326_FINAL.zip")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def fmt_brl(v: float) -> str:
    """Formata valor em pt-BR: R$ 1.234,56"""
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


# ---------------------------------------------------------------------------
# Etapa 1: BigQuery — buscar usuários marcados com opt-in
# ---------------------------------------------------------------------------
def fetch_marked_users() -> pd.DataFrame:
    """
    Busca usuários com mark PROMO_RELAMPAGO_170326 via j_user.core_tags.
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
# Etapa 2: Athena — consultar transações em chunks
# ---------------------------------------------------------------------------
def build_sql(chunk_ids: list) -> str:
    """
    Query Athena (Presto SQL) — estrutura alinhada com o arquiteto:
    - CTE params: centraliza timestamps para auditabilidade
    - Divisão /100.0 no SQL: valor já sai em BRL
    - CASE WHEN classificação: faixas calculadas no SQL
    - c_start_time: coluna real (c_created_time NÃO existe neste schema)
    - c_amount_in_ecr_ccy: centavos | c_txn_status = 'SUCCESS'
    """
    ids_str   = ", ".join(str(i) for i in chunk_ids)
    games_str = ", ".join(f"'{g}'" for g in GAME_IDS)
    return f"""
    WITH params AS (
        SELECT
            TIMESTAMP '{START_UTC}' AS start_utc,
            TIMESTAMP '{END_UTC}'   AS end_utc
    ),
    participantes AS (
        SELECT DISTINCT
            c_ecr_id,
            c_external_id AS user_ext_id
        FROM ecr_ec2.tbl_ecr
        WHERE c_external_id IN ({ids_str})
    ),
    dados_brutos AS (
        SELECT
            f.c_ecr_id,
            SUM(CASE WHEN f.c_txn_type = {TXN_BET}
                     THEN f.c_amount_in_ecr_ccy ELSE 0 END) / 100.0 AS total_bet_brl,
            COUNT_IF(f.c_txn_type = {TXN_ROLLBACK})                  AS qtd_rollbacks,
            COUNT_IF(f.c_txn_type = {TXN_BET})                       AS qtd_apostas
        FROM fund_ec2.tbl_real_fund_txn f
        CROSS JOIN params p
        INNER JOIN participantes pt ON f.c_ecr_id = pt.c_ecr_id
        WHERE f.c_start_time BETWEEN p.start_utc AND p.end_utc
          AND f.c_game_id IN ({games_str})
          AND f.c_txn_status = 'SUCCESS'
          AND f.c_txn_type IN ({TXN_BET}, {TXN_ROLLBACK})
        GROUP BY 1
    ),
    classificacao AS (
        SELECT
            p.user_ext_id,
            COALESCE(d.total_bet_brl, 0)  AS volume_apostado,
            COALESCE(d.qtd_rollbacks, 0)  AS rollbacks,
            COALESCE(d.qtd_apostas, 0)    AS qtd_apostas,
            CASE
                WHEN COALESCE(d.qtd_rollbacks, 0) > 0 THEN 'Desclassificado (rollback)'
                WHEN COALESCE(d.total_bet_brl, 0) >= 500 THEN 'Faixa 4'
                WHEN COALESCE(d.total_bet_brl, 0) >= 300 THEN 'Faixa 3'
                WHEN COALESCE(d.total_bet_brl, 0) >= 100 THEN 'Faixa 2'
                WHEN COALESCE(d.total_bet_brl, 0) >= 30  THEN 'Faixa 1'
                WHEN COALESCE(d.total_bet_brl, 0) > 0    THEN 'Abaixo do Mínimo'
                ELSE 'Sem Atividade'
            END AS faixa_segmentacao
        FROM participantes p
        LEFT JOIN dados_brutos d ON p.c_ecr_id = d.c_ecr_id
    )
    SELECT
        user_ext_id,
        volume_apostado AS total_bet_brl,
        rollbacks       AS qtd_rollbacks,
        qtd_apostas,
        faixa_segmentacao
    FROM classificacao
    ORDER BY volume_apostado DESC
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
        "user_ext_id", "total_bet_brl", "qtd_rollbacks", "qtd_apostas",
        "faixa_segmentacao",
    ])


# ---------------------------------------------------------------------------
# Etapa 3: Classificação de faixas
# ---------------------------------------------------------------------------
def classificar_faixa(total_bet_cents: float, tem_rollback: bool) -> str:
    """
    Regra da promoção:
    - Rollback → desclassificado (independente do valor apostado)
    - Faixas baseadas no turnover BRUTO (total_bet_cents), não net
    - Usuário fica na faixa MAIS ALTA atingida (sem duplicidade)
    """
    if tem_rollback:
        return "Desclassificado (rollback)"
    for nome, low, high in FAIXAS:
        if low <= total_bet_cents <= high:
            return nome
    if total_bet_cents >= 50000:
        return "Faixa 4"
    if total_bet_cents > 0:
        return "Abaixo do Mínimo"
    return "Sem Atividade"


# ---------------------------------------------------------------------------
# Etapa 4: Validação cruzada Athena vs BigQuery (Smartico DW)
# ---------------------------------------------------------------------------
def validacao_cruzada_bigquery(smartico_user_ids: list) -> dict:
    """
    Consulta tr_casino_bet no BigQuery para o mesmo período e jogo,
    filtrando pelos mesmos usuários com opt-in.
    Retorna dict com métricas para comparação com os dados do Athena.
    """
    log.info("Etapa 4: Validação cruzada — consultando BigQuery (tr_casino_bet)...")

    ids_str = ", ".join(str(i) for i in smartico_user_ids)
    sql = f"""
    SELECT
        COUNT(DISTINCT b.user_id)                                        AS qtd_jogadores,
        SUM(CASE WHEN COALESCE(b.casino_is_rollback, FALSE) = FALSE
                 THEN b.casino_last_bet_amount ELSE 0 END)              AS total_bet_brl,
        SUM(CASE WHEN b.casino_is_rollback = TRUE
                 THEN 1 ELSE 0 END)                                     AS qtd_rollbacks,
        COUNT(CASE WHEN COALESCE(b.casino_is_rollback, FALSE) = FALSE
                   THEN 1 END)                                           AS qtd_apostas
    FROM `smartico-bq6.dwh_ext_24105.tr_casino_bet` b
    WHERE b.user_id IN ({ids_str})
      AND b.casino_last_bet_game_name = {SMR_GAME_ID}
      AND b.event_time BETWEEN TIMESTAMP '{START_UTC}'
                           AND TIMESTAMP '{END_UTC}'
    """
    df = query_bigquery(sql)

    result = {
        "qtd_jogadores": int(df["qtd_jogadores"].iloc[0]) if not df.empty else 0,
        "total_bet_brl": float(df["total_bet_brl"].iloc[0] or 0) if not df.empty else 0.0,
        "qtd_rollbacks": int(df["qtd_rollbacks"].iloc[0]) if not df.empty else 0,
        "qtd_apostas":   int(df["qtd_apostas"].iloc[0]) if not df.empty else 0,
    }
    log.info(f"  BigQuery: {result['qtd_jogadores']} jogadores, "
             f"{fmt_brl(result['total_bet_brl'])} apostado, "
             f"{result['qtd_rollbacks']} rollbacks")
    return result


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------
def main():
    log.info(f"game_ids: {GAME_IDS} (Sugar Rush — Pragmatic Play)")

    # -- Etapa 1: BigQuery → opt-in ----------------------------------------
    df_marked = fetch_marked_users()

    df_marked["user_ext_id"] = (
        pd.to_numeric(df_marked["user_ext_id"], errors="coerce")
          .astype("Int64")
    )
    df_marked = df_marked.dropna(subset=["user_ext_id"])
    ext_ids = df_marked["user_ext_id"].tolist()
    log.info(f"  IDs válidos para Athena: {len(ext_ids):,}")

    if not ext_ids:
        log.error("Nenhum usuário encontrado com a mark. Verifique tag no BigQuery.")
        return

    # -- Etapa 2: Athena → transações (classificação já feita no SQL) --------
    df_txn = fetch_athena_data(ext_ids)

    if not df_txn.empty:
        df_txn["user_ext_id"]     = df_txn["user_ext_id"].astype("Int64")
        df_txn["total_bet_brl"]   = pd.to_numeric(df_txn["total_bet_brl"],   errors="coerce").fillna(0)
        df_txn["qtd_rollbacks"]   = pd.to_numeric(df_txn["qtd_rollbacks"],   errors="coerce").fillna(0).astype(int)
        df_txn["qtd_apostas"]     = pd.to_numeric(df_txn["qtd_apostas"],     errors="coerce").fillna(0).astype(int)
        df_txn["tem_rollback"]    = df_txn["qtd_rollbacks"] > 0

    # -- Etapa 3: merge left join (preserva TODOS os opt-in) ----------------
    df_final = df_marked.merge(df_txn, on="user_ext_id", how="left")

    df_final["total_bet_brl"]     = df_final["total_bet_brl"].fillna(0.0)
    df_final["qtd_rollbacks"]     = df_final["qtd_rollbacks"].fillna(0).astype(int)
    df_final["qtd_apostas"]       = df_final["qtd_apostas"].fillna(0).astype(int)
    df_final["tem_rollback"]      = df_final["tem_rollback"].fillna(False)
    df_final["faixa_segmentacao"] = df_final["faixa_segmentacao"].fillna("Sem Atividade")

    # -- Formatação BRL ----------------------------------------------------
    df_final["total_bet_brl_fmt"] = df_final["total_bet_brl"].apply(fmt_brl)

    # -- Ordenação e colunas de saída --------------------------------------
    cols_out = [
        "smartico_user_id", "user_ext_id",
        "qtd_apostas", "qtd_rollbacks", "tem_rollback",
        "total_bet_brl", "total_bet_brl_fmt",
        "faixa_segmentacao",
    ]
    df_final = df_final[cols_out].sort_values("total_bet_brl", ascending=False)

    # -- Etapa 4: Validação cruzada Athena vs BigQuery -----------------------
    smartico_ids = df_marked["smartico_user_id"].tolist()
    bq_result = validacao_cruzada_bigquery(smartico_ids)

    # Métricas Athena para comparação (jogadores com atividade, excluindo rollback-only)
    athena_jogadores = len(jogaram := df_final[df_final["faixa_segmentacao"] != "Sem Atividade"])
    athena_total_bet = df_final["total_bet_brl"].sum()

    # Divergência
    diff_jogadores = abs(athena_jogadores - bq_result["qtd_jogadores"])
    diff_valor     = abs(athena_total_bet - bq_result["total_bet_brl"])
    pct_diff       = (diff_valor / athena_total_bet * 100) if athena_total_bet > 0 else 0

    log.info(f"  Validação cruzada: Athena={athena_jogadores} jogadores / "
             f"{fmt_brl(athena_total_bet)} | BigQuery={bq_result['qtd_jogadores']} jogadores / "
             f"{fmt_brl(bq_result['total_bet_brl'])} | "
             f"Diff={fmt_brl(diff_valor)} ({pct_diff:.2f}%)")

    # -- Exportar CSV ------------------------------------------------------
    df_final.to_csv(OUTPUT_CSV, index=False, sep=";", encoding="utf-8-sig")

    # -- Gerar ZIP ---------------------------------------------------------
    with zipfile.ZipFile(OUTPUT_ZIP, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(OUTPUT_CSV, os.path.basename(OUTPUT_CSV))
    log.info(f"ZIP gerado: {OUTPUT_ZIP}")

    # -- Métricas ----------------------------------------------------------
    total_marcados   = len(df_final)
    desclassificados = len(df_final[df_final["faixa_segmentacao"] == "Desclassificado (rollback)"])
    total_apostado = athena_total_bet

    ordem_faixas = [
        "Faixa 4", "Faixa 3", "Faixa 2", "Faixa 1",
        "Abaixo do Mínimo", "Desclassificado (rollback)", "Sem Atividade",
    ]
    faixa_counts = df_final["faixa_segmentacao"].value_counts()

    rotulos = {
        "Faixa 4": "Faixa 4 (>=R$500)",
        "Faixa 3": "Faixa 3 (R$300-R$499,99)",
        "Faixa 2": "Faixa 2 (R$100-R$299,99)",
        "Faixa 1": "Faixa 1 (R$30-R$99,99)",
        "Abaixo do Mínimo": "Abaixo de R$30",
        "Desclassificado (rollback)": "Desclassificados (rollback)",
        "Sem Atividade": "Sem Atividade no período",
    }

    SEP = "=" * 70
    print(f"\n{SEP}")
    print(f"  Segmentação Promo Relâmpago | Sugar Rush | 17/03/2026")
    print(f"  Promoção: {MARK_TAG}")
    print(f"  Período : 17/03 15h → 19h BRT (UTC: {START_UTC} → {END_UTC})")
    print(SEP)

    print(
        f"\nDo segmento com opt-in ({total_marcados:,} usuários marcados), "
        f"{athena_jogadores:,} jogaram Sugar Rush no período. "
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
            print(f"  - {label}: {n:,} jogadores — {fmt_brl(vol)}")
        elif faixa == "Desclassificado (rollback)":
            print(f"  - {label}: {n:,} jogadores")
        else:
            print(f"  - {label}: {n:,} jogadores — {fmt_brl(vol)} ({pct:.0f}% do volume)")

    print("\nPonto de atencao:")
    if desclassificados == 0:
        print("  Zero rollbacks no período — nenhum jogador desclassificado.")
    else:
        print(
            f"  {desclassificados:,} jogador(es) desclassificado(s) por rollback — "
            "verificar antes do pagamento."
        )
    print(
        f"  Apenas {athena_jogadores/total_marcados*100:.1f}% dos marcados jogaram "
        f"no período."
    )

    print("\nValidacoes realizadas:\n")
    validacoes = [
        f"game_ids confirmados no catálogo Athena (bireports_ec2.tbl_vendor_games_mapping_data): "
        f"vs20sugarrush (Pragmatic Play) + 204 (alea_pragmaticplay).",
        f"Usuários extraídos do BigQuery Smartico via tag '{MARK_TAG}' "
        f"em j_user.core_tags — {total_marcados:,} com opt-in confirmado.",
        "Campo de valor: c_amount_in_ecr_ccy em centavos — divisão por 100 aplicada.",
        "Status da transação: 'SUCCESS' confirmado empiricamente neste schema fund_ec2.",
        f"Rollbacks (txn_type=72): {int(df_final['qtd_rollbacks'].sum()):,} "
        f"— {'nenhum desclassificado' if desclassificados == 0 else str(desclassificados) + ' desclassificados'}.",
        "Mapeamento de IDs: Smartico user_ext_id = c_external_id na tabela ECR.",
        "Cada jogador fica em apenas uma faixa (a mais alta atingida) — sem duplicidade.",
        f"Período em UTC: {START_UTC} → {END_UTC} (equivalente a 17/03 15h – 19h BRT).",
        "Padrão Early Filter + Late Join: INNER JOIN na CTE (performance) + LEFT JOIN no SELECT (governança).",
        "CSV inclui TODOS os opt-in — inativos com faixa_segmentacao = 'Sem Atividade'.",
        f"Validação cruzada Athena vs BigQuery: {athena_jogadores} jogadores (Athena) vs "
        f"{bq_result['qtd_jogadores']} (BigQuery), diferença de {fmt_brl(diff_valor)} "
        f"({pct_diff:.2f}%) — {'dados consistentes' if pct_diff < 5 else 'ATENÇÃO: divergência > 5%'}.",
    ]
    for i, v in enumerate(validacoes, 1):
        print(f"  {i}. {v}")

    print(f"\nCSV salvo : {OUTPUT_CSV}")
    print(f"ZIP salvo : {OUTPUT_ZIP}")
    print(f"  → {len(df_final):,} linhas | {len(df_final.columns)} colunas\n")

    return df_final


if __name__ == "__main__":
    main()
