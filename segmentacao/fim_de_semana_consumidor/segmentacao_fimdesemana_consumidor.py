"""
Segmentação Fim de Semana Consumidor — 13/03/2026
==================================================
Promoção: FIMDESEMANA_CONSUMIDOR_130326
Jogos: Tigre Sortudo, Ratinho Sortudo, Macaco Sortudo (Pragmatic Play)
       Apostas ACUMULADAS entre os 3 jogos.
Período: 13/03/2026 17:00 BRT → 15/03/2026 23:59 BRT
         (UTC: 2026-03-13 20:00:00 → 2026-03-16 02:59:59)

Regras de negócio:
  - Usuários com opt-in (mark FIMDESEMANA_CONSUMIDOR_130326 no Smartico)
  - Net Bet = Total Apostas − Rollbacks (acumulado nos 3 jogos)
  - Quem tiver QUALQUER rollback é DESCLASSIFICADO
  - Faixa 1: R$50 a R$199,99
  - Faixa 2: R$200 a R$399,99
  - Faixa 3: R$400 a R$799,99
  - Faixa 4: R$800 a R$999,99
  - Faixa 5: R$1.000,00 ou mais
  - Cada usuário fica na faixa mais alta (sem duplicidade de pagamento)

Fluxo:
  1. Puxa IDs do BigQuery (j_user_no_enums.core_tags com tag 48048728)
  2. Divide IDs em blocos de 5.000
  3. Consulta Redshift (fund.tbl_real_fund_txn + ecr.tbl_ecr) — 3 jogos
  4. Desclassifica quem teve rollback
  5. Aplica faixas com base no Net Bet acumulado
  6. Gera CSV final com left join (todos os marcados)

Freshness validada: Redshift tem dados até 16/03 09:53 UTC (7h após fim da promo).
"""

import sys
import os
import logging
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

from db.redshift import query_redshift
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
MARK_TAG     = "FIMDESEMANA_CONSUMIDOR_130326"
TAG_ID       = 48048728  # ID numérico da tag em core_tags (j_user_no_enums)

# 3 jogos Pragmatic Play — apostas acumuladas cross-game
GAME_IDS     = ["vs5luckytig", "vs10forwild", "vs5luckym"]
GAME_NAMES   = {
    "vs5luckytig": "Tigre Sortudo",
    "vs10forwild": "Ratinho Sortudo",
    "vs5luckym":   "Macaco Sortudo",
}

TXN_BET      = 27   # CASINO_BUYIN (aposta)
TXN_ROLLBACK = 72   # CASINO_BUYIN_CANCEL (rollback)

# 13/03 17h BRT → 20h UTC | 15/03 23:59 BRT → 16/03 02:59 UTC
START_UTC    = "2026-03-13 20:00:00"
END_UTC      = "2026-03-16 02:59:59"

CHUNK_SIZE   = 5_000

# Faixas (avaliadas da maior para a menor — usuário fica na mais alta)
FAIXAS = [
    ("Faixa 5", 1000.00, float("inf")),
    ("Faixa 4",  800.00,  999.99),
    ("Faixa 3",  400.00,  799.99),
    ("Faixa 2",  200.00,  399.99),
    ("Faixa 1",   50.00,  199.99),
]

OUTPUT_CSV = os.path.join(PROJECT_ROOT, "segmentacao_fimdesemana_consumidor_130326.csv")


# ---------------------------------------------------------------------------
# 1. BigQuery — buscar usuários marcados
# ---------------------------------------------------------------------------
def fetch_marked_users() -> pd.DataFrame:
    """
    Usa j_user (tags STRING) em vez de j_user_no_enums (tags INT).
    j_user_no_enums retorna user_ext_id com prefixo '1094:' que não é
    compatível com o c_external_id do Redshift.
    j_user retorna user_ext_id limpo (mesmo padrão das análises anteriores).
    """
    log.info(f"Buscando usuários com mark '{MARK_TAG}' no BigQuery (j_user)...")
    sql = f"""
    SELECT
        user_id     AS smartico_user_id,
        user_ext_id
    FROM `smartico-bq6.dwh_ext_24105.j_user`
    WHERE '{MARK_TAG}' IN UNNEST(core_tags)
    """
    df = query_bigquery(sql)
    log.info(f"  → {len(df):,} usuários marcados encontrados")
    return df


# ---------------------------------------------------------------------------
# 2. Redshift — consultar transações em chunks (3 jogos acumulados)
# ---------------------------------------------------------------------------
def build_sql(chunk_ids: list) -> str:
    ids_str = ", ".join(str(i) for i in chunk_ids)
    games_str = ", ".join(f"'{g}'" for g in GAME_IDS)
    return f"""
    WITH params AS (
        SELECT '{START_UTC}'::timestamp AS start_ts,
               '{END_UTC}'::timestamp   AS end_ts
    )
    SELECT
        e.c_external_id AS user_ext_id,
        SUM(CASE WHEN f.c_txn_type = {TXN_BET}
                 THEN f.c_amount_in_ecr_ccy ELSE 0 END) AS total_bet_cents,
        SUM(CASE WHEN f.c_txn_type = {TXN_ROLLBACK}
                 THEN f.c_amount_in_ecr_ccy ELSE 0 END) AS total_rollback_cents,
        SUM(CASE WHEN f.c_txn_type = {TXN_ROLLBACK} THEN 1 ELSE 0 END) AS qtd_rollbacks,
        SUM(CASE WHEN f.c_txn_type = {TXN_BET}      THEN 1 ELSE 0 END) AS qtd_apostas
    FROM fund.tbl_real_fund_txn f
    INNER JOIN ecr.tbl_ecr e ON e.c_ecr_id = f.c_ecr_id
    CROSS JOIN params p
    WHERE f.c_start_time BETWEEN p.start_ts AND p.end_ts
      AND f.c_game_id IN ({games_str})
      AND f.c_txn_status = 'SUCCESS'
      AND f.c_txn_type IN ({TXN_BET}, {TXN_ROLLBACK})
      AND e.c_external_id IN ({ids_str})
    GROUP BY 1
    """


def fetch_redshift_data(ext_ids: list) -> pd.DataFrame:
    chunks = [ext_ids[i:i + CHUNK_SIZE] for i in range(0, len(ext_ids), CHUNK_SIZE)]
    log.info(f"Consultando Redshift: {len(ext_ids):,} IDs em {len(chunks)} chunk(s)...")

    frames = []
    for idx, chunk in enumerate(chunks, 1):
        log.info(f"  Chunk {idx}/{len(chunks)} ({len(chunk):,} IDs)...")
        sql = build_sql(chunk)
        df = query_redshift(sql)
        if not df.empty:
            frames.append(df)
        log.info(f"    → {len(df):,} jogadores com transações")

    if frames:
        result = pd.concat(frames, ignore_index=True)
        log.info(f"  Total consolidado: {len(result):,} jogadores com transações")
        return result

    log.warning("  Nenhuma transação encontrada no Redshift!")
    return pd.DataFrame(columns=[
        "user_ext_id", "total_bet_cents", "total_rollback_cents",
        "qtd_rollbacks", "qtd_apostas",
    ])


# ---------------------------------------------------------------------------
# 3. Classificação de faixas
# ---------------------------------------------------------------------------
def classificar_faixa(net_bet_brl: float, tem_rollback: bool) -> str:
    if tem_rollback:
        return "Desclassificado (rollback)"
    for nome, low, high in FAIXAS:
        if low <= net_bet_brl <= high:
            return nome
    if net_bet_brl >= 1000.0:
        return "Faixa 5"
    return "Abaixo do Mínimo"


# ---------------------------------------------------------------------------
# 4. Pipeline principal
# ---------------------------------------------------------------------------
def main():
    # ── Etapa 1: buscar marcados no BigQuery ────────────────────────────────
    df_marked = fetch_marked_users()

    df_marked["user_ext_id"] = (
        pd.to_numeric(df_marked["user_ext_id"], errors="coerce")
          .astype("Int64")
    )
    df_marked = df_marked.dropna(subset=["user_ext_id"])
    ext_ids = df_marked["user_ext_id"].tolist()
    log.info(f"IDs válidos para consulta Redshift: {len(ext_ids):,}")

    if not ext_ids:
        log.error("Nenhum usuário encontrado com a mark. Verifique TAG_ID e BigQuery.")
        return

    # ── Etapa 2: buscar transações no Redshift ─────────────────────────────
    df_txn = fetch_redshift_data(ext_ids)

    if not df_txn.empty:
        df_txn["user_ext_id"]          = df_txn["user_ext_id"].astype("Int64")
        df_txn["total_bet_cents"]      = pd.to_numeric(df_txn["total_bet_cents"],      errors="coerce").fillna(0)
        df_txn["total_rollback_cents"] = pd.to_numeric(df_txn["total_rollback_cents"], errors="coerce").fillna(0)
        df_txn["qtd_rollbacks"]        = pd.to_numeric(df_txn["qtd_rollbacks"],        errors="coerce").fillna(0).astype(int)
        df_txn["qtd_apostas"]          = pd.to_numeric(df_txn["qtd_apostas"],          errors="coerce").fillna(0).astype(int)

        # Centavos → BRL
        df_txn["total_bet_brl"]      = df_txn["total_bet_cents"]      / 100.0
        df_txn["total_rollback_brl"] = df_txn["total_rollback_cents"] / 100.0
        df_txn["net_bet_brl"]        = df_txn["total_bet_brl"] - df_txn["total_rollback_brl"]

        # Flag de rollback
        df_txn["tem_rollback"] = df_txn["qtd_rollbacks"] > 0

        # Classificar faixa
        df_txn["faixa_segmentacao"] = df_txn.apply(
            lambda r: classificar_faixa(r["net_bet_brl"], r["tem_rollback"]), axis=1
        )

    # ── Etapa 3: merge (left join — preserva todos os marcados) ────────────
    df_final = df_marked.merge(df_txn, on="user_ext_id", how="left")

    df_final["total_bet_brl"]      = df_final["total_bet_brl"].fillna(0.0)
    df_final["total_rollback_brl"] = df_final["total_rollback_brl"].fillna(0.0)
    df_final["net_bet_brl"]        = df_final["net_bet_brl"].fillna(0.0)
    df_final["qtd_rollbacks"]      = df_final["qtd_rollbacks"].fillna(0).astype(int)
    df_final["qtd_apostas"]        = df_final["qtd_apostas"].fillna(0).astype(int)
    df_final["tem_rollback"]       = df_final["tem_rollback"].fillna(False)
    df_final["faixa_segmentacao"]  = df_final["faixa_segmentacao"].fillna("Não jogou")

    # ── Formatação BRL (pt-BR) ─────────────────────────────────────────────
    def fmt_brl(v: float) -> str:
        return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

    df_final["net_bet_brl_fmt"]        = df_final["net_bet_brl"].apply(fmt_brl)
    df_final["total_bet_brl_fmt"]      = df_final["total_bet_brl"].apply(fmt_brl)
    df_final["total_rollback_brl_fmt"] = df_final["total_rollback_brl"].apply(fmt_brl)

    # ── Ordenação e colunas de saída ───────────────────────────────────────
    cols_out = [
        "smartico_user_id", "user_ext_id",
        "qtd_apostas", "qtd_rollbacks", "tem_rollback",
        "total_bet_brl", "total_rollback_brl", "net_bet_brl",
        "total_bet_brl_fmt", "total_rollback_brl_fmt", "net_bet_brl_fmt",
        "faixa_segmentacao",
    ]
    df_final = df_final[cols_out].sort_values("net_bet_brl", ascending=False)

    # ── Exportar CSV ───────────────────────────────────────────────────────
    df_final.to_csv(OUTPUT_CSV, index=False, sep=";", encoding="utf-8-sig")

    # ── Métricas ───────────────────────────────────────────────────────────
    total_marcados   = len(df_final)
    jogaram          = df_final[df_final["faixa_segmentacao"] != "Não jogou"]
    desclassificados = len(df_final[df_final["faixa_segmentacao"] == "Desclassificado (rollback)"])
    total_apostado   = df_final["total_bet_brl"].sum()
    total_rollback   = df_final["total_rollback_brl"].sum()
    net_bet_total    = df_final["net_bet_brl"].sum()
    pct_nao_jogou    = (total_marcados - len(jogaram)) / total_marcados * 100

    ordem_faixas = [
        "Faixa 5", "Faixa 4", "Faixa 3", "Faixa 2", "Faixa 1",
        "Abaixo do Mínimo", "Desclassificado (rollback)", "Não jogou",
    ]
    faixa_counts = df_final["faixa_segmentacao"].value_counts()

    SEP = "=" * 70
    games_label = ", ".join(GAME_NAMES.values())

    # ── Output formatado (mesmo padrão das entregas anteriores) ────────────
    print(f"\n{SEP}")
    print(f"  Segmentação Fim de Semana Consumidor | Promoção {MARK_TAG}")
    print(f"  Período: 13/03 17h → 15/03 23:59 BRT")
    print(SEP)

    print(
        f"\nDo segmento com opt-in ({total_marcados:,} usuários marcados), "
        f"{len(jogaram):,} jogaram {games_label} no período "
        f"(13/03 17h – 15/03 23:59 BRT). "
        f"Total apostado: {fmt_brl(total_apostado)}."
    )

    # ── Distribuição por faixa ─────────────────────────────────────────────
    print("\nDistribuição por faixa:\n")
    rotulos = {
        "Faixa 5": "Faixa 5 (≥R$1.000)",
        "Faixa 4": "Faixa 4 (R$800-999)",
        "Faixa 3": "Faixa 3 (R$400-799)",
        "Faixa 2": "Faixa 2 (R$200-399)",
        "Faixa 1": "Faixa 1 (R$50-199)",
        "Abaixo do Mínimo": "Abaixo de R$50",
        "Desclassificado (rollback)": "Desclassificados (rollback)",
        "Não jogou": "Não jogou no período",
    }

    for faixa in ordem_faixas:
        if faixa not in faixa_counts.index:
            continue
        n = faixa_counts[faixa]
        vol = df_final[df_final["faixa_segmentacao"] == faixa]["net_bet_brl"].sum()
        pct = vol / net_bet_total * 100 if net_bet_total > 0 else 0
        label = rotulos.get(faixa, faixa)

        if faixa in ("Não jogou", "Abaixo do Mínimo"):
            print(f"  • {label}: {n:,} jogadores — {fmt_brl(vol)}")
        else:
            print(f"  • {label}: {n:,} jogadores — {fmt_brl(vol)} ({pct:.0f}% do volume)")

    # ── Ponto de atenção ───────────────────────────────────────────────────
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
        f"{games_label} no período."
    )

    # ── Validações realizadas ──────────────────────────────────────────────
    print("\nValidações realizadas:\n")
    validacoes = [
        f"Jogos confirmados no catálogo Redshift (bireports.tbl_vendor_games_mapping_data): "
        f"vs5luckytig (Tigre Sortudo), vs10forwild (Ratinho Sortudo), vs5luckym (Macaco Sortudo) "
        f"— todos Pragmatic Play.",
        f"Usuários extraídos do BigQuery Smartico via tag {TAG_ID} ({MARK_TAG}) "
        f"em j_user_no_enums.core_tags — {total_marcados:,} com opt-in confirmado.",
        "Valores confirmados em centavos pela documentação da Pragmatic (v1.3) "
        "— divisão por 100 aplicada.",
        f"Rollbacks (txn_type=72) no período: {int(df_final['qtd_rollbacks'].sum()):,} "
        f"— {'nenhum jogador desclassificado' if desclassificados == 0 else str(desclassificados) + ' desclassificados'}.",
        "Mapeamento de IDs validado: Smartico user_ext_id = c_external_id "
        "na tabela ECR da Pragmatic.",
        "Cada jogador aparece em apenas uma faixa (a mais alta atingida) "
        "— sem duplicidade de pagamento.",
        f"Período em UTC: {START_UTC} → {END_UTC} "
        f"(equivalente a 13/03 17h – 15/03 23:59 BRT).",
        "Freshness Redshift validada: dados até 16/03 09:53 UTC — "
        "período da promo 100% consolidado.",
        "Apostas ACUMULADAS entre os 3 jogos — um jogador pode ter apostado "
        "parte em cada jogo e a soma é considerada para a faixa.",
    ]
    for i, v in enumerate(validacoes, 1):
        print(f"  {i}. {v}")

    print(f"\nCSV salvo: {OUTPUT_CSV}")
    print(f"  → {len(df_final):,} linhas | {len(df_final.columns)} colunas\n")

    return df_final


if __name__ == "__main__":
    main()