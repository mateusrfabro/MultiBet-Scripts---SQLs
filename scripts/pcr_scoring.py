"""
Player Credit Rating (PCR) — Scoring Pipeline v1.0
====================================================
Calcula o rating PCR (D, C, B, A, AA, AAA) para jogadores ativos nos ultimos 90 dias.

Fonte principal: ps_bi.fct_player_activity_daily (pre-agregado, BRL, UTC)
Fonte auxiliar:  ps_bi.dim_user (cadastro, is_test, affiliate)

Arquitetura:
  Camada 1: Player Value Score (PVS) — 9 componentes, score 0-100
  Camada 2: Rating — discretizacao do PVS em 6 niveis (D a AAA)

Saida: CSV com rating por jogador + resumo no terminal

Uso:
    python scripts/pcr_scoring.py
"""

import sys
import os
import logging
import numpy as np
import pandas as pd
from datetime import datetime

# Adiciona raiz do projeto ao path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from db.athena import query_athena

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ============================================================
# CONFIGURACAO
# ============================================================
JANELA_DIAS = 90          # ultimos 90 dias de atividade
OUTPUT_DIR = "reports"
SNAPSHOT_DATE = datetime.now().strftime("%Y-%m-%d")


def extrair_metricas_jogadores() -> pd.DataFrame:
    """
    Extrai metricas por jogador dos ultimos 30 dias usando ps_bi.

    Usa fct_player_activity_daily (112 cols, BRL, UTC) — camada pre-agregada.
    Valores ja estao em BRL (nao centavos).
    """
    log.info(f"Extraindo metricas de jogadores (ultimos {JANELA_DIAS} dias)...")

    sql = f"""
    -- PCR Scoring: metricas por jogador (ultimos {JANELA_DIAS} dias)
    -- Fonte: ps_bi.fct_player_activity_daily + dim_user
    -- Valores em BRL (pre-agregado pelo dbt)
    --
    -- DEFINICAO DE ATIVO (v1.1 — 09/04/2026):
    --   Ativo = quem APOSTOU (casino ou sportsbook) OU DEPOSITOU no periodo.
    --   Login sozinho NAO conta. Bonus emitido sem aposta NAO conta.
    --   Filtro aplicado via HAVING no aggregado.

    WITH player_metrics AS (
        SELECT
            f.player_id,

            -- GGR e NGR
            COALESCE(SUM(f.ggr_base), 0) AS ggr_total,
            COALESCE(SUM(f.ngr_base), 0) AS ngr_total,

            -- Casino
            COALESCE(SUM(f.casino_realbet_base), 0)  AS casino_bet,
            COALESCE(SUM(f.casino_real_win_base), 0)  AS casino_win,
            COALESCE(SUM(f.casino_realbet_count), 0)  AS casino_rounds,

            -- Sportsbook
            COALESCE(SUM(f.sb_realbet_base), 0)  AS sport_bet,
            COALESCE(SUM(f.sb_real_win_base), 0) AS sport_win,
            COALESCE(SUM(f.sb_realbet_count), 0) AS sport_bets,

            -- Depositos
            COALESCE(SUM(f.deposit_success_count), 0) AS num_deposits,
            COALESCE(SUM(f.deposit_success_base), 0)  AS total_deposits,

            -- Saques
            COALESCE(SUM(f.cashout_success_count), 0) AS num_cashouts,
            COALESCE(SUM(f.cashout_success_base), 0)  AS total_cashouts,

            -- Atividade
            COUNT(DISTINCT f.activity_date) AS days_active,
            MAX(f.activity_date) AS last_active_date,
            MIN(f.activity_date) AS first_active_date,

            -- Bonus
            COALESCE(SUM(f.bonus_issued_base), 0)     AS bonus_issued,
            COALESCE(SUM(f.bonus_turnedreal_base), 0)  AS bonus_turned_real,

            -- Turnover total (casino + sport)
            COALESCE(SUM(f.casino_realbet_base), 0) + COALESCE(SUM(f.sb_realbet_base), 0) AS turnover_total

        FROM ps_bi.fct_player_activity_daily f
        WHERE f.activity_date >= CURRENT_DATE - INTERVAL '{JANELA_DIAS}' DAY
          AND f.activity_date < CURRENT_DATE  -- exclui D-0 (parcial)
        GROUP BY f.player_id
        -- FILTRO CRITICO: somente quem apostou ou depositou
        HAVING COALESCE(SUM(f.casino_realbet_count), 0) > 0   -- apostou casino
            OR COALESCE(SUM(f.sb_realbet_count), 0) > 0       -- apostou sportsbook
            OR COALESCE(SUM(f.deposit_success_count), 0) > 0  -- depositou
    )
    SELECT
        m.*,
        u.external_id,
        u.registration_date,
        u.affiliate_id,
        u.is_test,

        -- Recencia: dias desde ultima atividade
        DATE_DIFF('day', CAST(m.last_active_date AS DATE), CURRENT_DATE) AS recency_days,

        -- Tipo de produto
        CASE
            WHEN m.casino_rounds > 0 AND m.sport_bets > 0 THEN 'MISTO'
            WHEN m.casino_rounds > 0 THEN 'CASINO'
            WHEN m.sport_bets > 0 THEN 'SPORT'
            ELSE 'OUTRO'
        END AS product_type

    FROM player_metrics m
    LEFT JOIN ps_bi.dim_user u ON m.player_id = u.ecr_id
    WHERE (u.is_test = false OR u.is_test IS NULL)
    """

    df = query_athena(sql, database="ps_bi")
    log.info(f"  -> {len(df):,} jogadores extraidos")
    return df


def normalizar_percentil(series: pd.Series, inverter: bool = False) -> pd.Series:
    """
    Normaliza uma serie para 0-100 usando percentil rank.
    Se inverter=True, valores menores recebem score maior.
    """
    ranks = series.rank(pct=True, method="average") * 100
    if inverter:
        ranks = 100 - ranks
    return ranks.clip(0, 100)


def calcular_pvs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula o Player Value Score (PVS) — Camada 1 do PCR.

    9 componentes ponderados (score 0-100):
      +25  GGR Total
      +15  Deposito Total
      +12  Recencia (mais recente = melhor)
      +10  Margem GGR/Turnover (menor margem = mais rodadas = melhor)
      +10  Numero de Depositos
      +8   Dias Ativos
      +5   Mix de Produto (misto = bonus)
      +5   Taxa de Atividade (dias ativos / janela)
      -10  Sensibilidade a Bonus (penalizador)
    """
    log.info("Calculando Player Value Score (PVS)...")

    result = df.copy()

    # --- Componentes positivos ---

    # 1. GGR Total (peso 25)
    result["score_ggr"] = normalizar_percentil(result["ggr_total"])

    # 2. Deposito Total (peso 15)
    result["score_deposit"] = normalizar_percentil(result["total_deposits"])

    # 3. Recencia — quanto menor (mais recente), melhor (peso 12)
    result["score_recencia"] = normalizar_percentil(result["recency_days"], inverter=True)

    # 4. Margem GGR/Turnover — menor margem = jogador recicla mais = melhor (peso 10)
    result["margem_ggr"] = np.where(
        result["turnover_total"] > 0,
        result["ggr_total"] / result["turnover_total"],
        0
    )
    result["score_margem"] = normalizar_percentil(result["margem_ggr"], inverter=True)

    # 5. Numero de Depositos (peso 10)
    result["score_num_dep"] = normalizar_percentil(result["num_deposits"])

    # 6. Dias Ativos (peso 8)
    result["score_dias_ativos"] = normalizar_percentil(result["days_active"])

    # 7. Mix de Produto — misto recebe bonus (peso 5)
    result["score_mix"] = result["product_type"].map({
        "MISTO": 100, "CASINO": 40, "SPORT": 40, "OUTRO": 0
    }).fillna(0)

    # 8. Taxa de Atividade = dias ativos / janela (peso 5)
    result["taxa_atividade"] = (result["days_active"] / JANELA_DIAS).clip(0, 1)
    result["score_atividade"] = result["taxa_atividade"] * 100

    # --- Componente penalizador ---

    # 9. Sensibilidade a Bonus = bonus_issued / depositos (peso -10)
    result["bonus_ratio"] = np.where(
        result["total_deposits"] > 0,
        result["bonus_issued"] / result["total_deposits"],
        0
    )
    # Quanto maior o ratio, maior a penalidade
    result["score_bonus_pen"] = normalizar_percentil(result["bonus_ratio"])

    # --- PVS Composto ---
    result["pvs"] = (
        result["score_ggr"]         * 0.25 +
        result["score_deposit"]     * 0.15 +
        result["score_recencia"]    * 0.12 +
        result["score_margem"]      * 0.10 +
        result["score_num_dep"]     * 0.10 +
        result["score_dias_ativos"] * 0.08 +
        result["score_mix"]         * 0.05 +
        result["score_atividade"]   * 0.05 -
        result["score_bonus_pen"]   * 0.10
    ).clip(0, 100)

    log.info(f"  -> PVS: min={result['pvs'].min():.1f}, "
             f"median={result['pvs'].median():.1f}, "
             f"max={result['pvs'].max():.1f}")

    return result


def atribuir_rating(df: pd.DataFrame) -> pd.DataFrame:
    """
    Atribui o rating PCR usando escala E-S (v1.2).

    Cortes por percentil (calibracao v1.2 — 09/04/2026):
      E = Bottom 25%  (engajamento minimo)
      D = 25-50%      (casual)
      C = 50-75%      (regular)
      B = 75-92%      (em crescimento)
      A = 92-99%      (VIP)
      S = Top 1%      (whale — exclusivo)
    """
    log.info("Atribuindo ratings PCR (escala E-S v1.2)...")

    result = df.copy()

    # Calcular percentis da distribuicao real
    p25 = result["pvs"].quantile(0.25)
    p50 = result["pvs"].quantile(0.50)
    p75 = result["pvs"].quantile(0.75)
    p92 = result["pvs"].quantile(0.92)
    p99 = result["pvs"].quantile(0.99)

    log.info(f"  Cortes PVS: E<{p25:.1f} | D<{p50:.1f} | C<{p75:.1f} | B<{p92:.1f} | A<{p99:.1f} | S>={p99:.1f}")

    # Atribuir rating
    conditions = [
        result["pvs"] >= p99,
        result["pvs"] >= p92,
        result["pvs"] >= p75,
        result["pvs"] >= p50,
        result["pvs"] >= p25,
    ]
    choices = ["S", "A", "B", "C", "D"]
    result["rating"] = np.select(conditions, choices, default="E")

    # Ordenar rating como categoria ordenada
    rating_order = ["E", "D", "C", "B", "A", "S"]
    result["rating"] = pd.Categorical(result["rating"], categories=rating_order, ordered=True)

    return result


def calcular_metricas_derivadas(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula metricas derivadas uteis para analise."""
    result = df.copy()

    # W/D Ratio (withdraw / deposit)
    result["wd_ratio"] = np.where(
        result["total_deposits"] > 0,
        result["total_cashouts"] / result["total_deposits"],
        0
    )

    # Net Deposit
    result["net_deposit"] = result["total_deposits"] - result["total_cashouts"]

    # GGR por dia ativo
    result["ggr_por_dia"] = np.where(
        result["days_active"] > 0,
        result["ggr_total"] / result["days_active"],
        0
    )

    return result


def gerar_resumo(df: pd.DataFrame) -> pd.DataFrame:
    """Gera tabela resumo por rating."""
    resumo = df.groupby("rating", observed=True).agg(
        jogadores=("player_id", "count"),
        ggr_total=("ggr_total", "sum"),
        ggr_medio=("ggr_total", "mean"),
        ggr_mediano=("ggr_total", "median"),
        ngr_medio=("ngr_total", "mean"),
        deposito_medio=("total_deposits", "mean"),
        num_dep_medio=("num_deposits", "mean"),
        dias_ativos_medio=("days_active", "mean"),
        recencia_media=("recency_days", "mean"),
        wd_ratio_medio=("wd_ratio", "mean"),
        pct_misto=("product_type", lambda x: (x == "MISTO").mean() * 100),
        pvs_min=("pvs", "min"),
        pvs_max=("pvs", "max"),
        pvs_medio=("pvs", "mean"),
    ).reset_index()

    # Adicionar % da base
    resumo["pct_base"] = (resumo["jogadores"] / resumo["jogadores"].sum() * 100).round(1)

    return resumo


def imprimir_resumo(resumo: pd.DataFrame, total_jogadores: int):
    """Imprime resumo formatado no terminal."""
    print("\n" + "=" * 90)
    print(f"  PLAYER CREDIT RATING (PCR) — Snapshot {SNAPSHOT_DATE}")
    print(f"  Base: {total_jogadores:,} jogadores ativos (ultimos {JANELA_DIAS} dias)")
    print("=" * 90)

    print(f"\n{'Rating':<8} {'Jogadores':>10} {'% Base':>7} {'GGR Total':>14} {'GGR/Jogador':>13} "
          f"{'Deps':>6} {'Dias':>6} {'Rec.':>5} {'% Misto':>8} {'PVS':>10}")
    print("-" * 90)

    for _, r in resumo.iterrows():
        print(f"{r['rating']:<8} {r['jogadores']:>10,} {r['pct_base']:>6.1f}% "
              f"R${r['ggr_total']:>12,.0f} R${r['ggr_medio']:>10,.0f} "
              f"{r['num_dep_medio']:>5.0f} {r['dias_ativos_medio']:>5.1f} "
              f"{r['recencia_media']:>4.0f}d {r['pct_misto']:>6.1f}% "
              f"{r['pvs_min']:.0f}-{r['pvs_max']:.0f}")

    # Totais
    total_ggr = resumo["ggr_total"].sum()
    print("-" * 90)
    print(f"{'TOTAL':<8} {total_jogadores:>10,} {'100.0%':>7} R${total_ggr:>12,.0f}")

    # Insights rapidos
    print("\n--- INSIGHTS ---")

    top_ratings = resumo[resumo["rating"].isin(["AAA", "AA", "A"])]
    top_pct = top_ratings["pct_base"].sum()
    top_ggr = top_ratings["ggr_total"].sum()
    top_ggr_pct = (top_ggr / total_ggr * 100) if total_ggr != 0 else 0

    print(f"  Ratings A+ (A, AA, AAA): {top_pct:.1f}% da base, {top_ggr_pct:.1f}% do GGR")

    neg_ratings = resumo[resumo["ggr_medio"] < 0]
    if len(neg_ratings) > 0:
        print(f"  Ratings com GGR negativo: {', '.join(neg_ratings['rating'].astype(str))}")

    misto_aaa = resumo[resumo["rating"] == "AAA"]["pct_misto"].values
    misto_d = resumo[resumo["rating"] == "D"]["pct_misto"].values
    if len(misto_aaa) > 0 and len(misto_d) > 0:
        print(f"  % Misto: AAA={misto_aaa[0]:.1f}% vs D={misto_d[0]:.1f}%")

    print()


def main():
    """Pipeline principal do PCR."""
    log.info("=" * 60)
    log.info("PCR SCORING PIPELINE v1.0")
    log.info("=" * 60)

    # 1. Extrair dados do Athena
    df = extrair_metricas_jogadores()

    if df.empty:
        log.error("Nenhum dado retornado do Athena. Abortando.")
        return

    # 2. Calcular metricas derivadas
    df = calcular_metricas_derivadas(df)

    # 3. Calcular PVS
    df = calcular_pvs(df)

    # 4. Atribuir ratings
    df = atribuir_rating(df)

    # 5. Gerar e imprimir resumo
    resumo = gerar_resumo(df)
    imprimir_resumo(resumo, len(df))

    # 6. Salvar CSV
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # CSV completo (todas as colunas de score + rating)
    cols_output = [
        "player_id", "external_id", "rating", "pvs",
        "ggr_total", "ngr_total", "total_deposits", "total_cashouts",
        "num_deposits", "days_active", "recency_days",
        "product_type", "casino_rounds", "sport_bets",
        "bonus_issued", "bonus_ratio", "wd_ratio", "net_deposit",
        "margem_ggr", "ggr_por_dia", "affiliate_id",
    ]
    cols_existentes = [c for c in cols_output if c in df.columns]

    csv_path = os.path.join(OUTPUT_DIR, f"pcr_ratings_{SNAPSHOT_DATE}.csv")
    df[cols_existentes].sort_values("pvs", ascending=False).to_csv(csv_path, index=False)
    log.info(f"CSV salvo: {csv_path} ({len(df):,} jogadores)")

    # CSV resumo
    resumo_path = os.path.join(OUTPUT_DIR, f"pcr_resumo_{SNAPSHOT_DATE}.csv")
    resumo.to_csv(resumo_path, index=False)
    log.info(f"Resumo salvo: {resumo_path}")

    # Legenda
    legenda_path = os.path.join(OUTPUT_DIR, f"pcr_ratings_{SNAPSHOT_DATE}_legenda.txt")
    with open(legenda_path, "w", encoding="utf-8") as f:
        f.write(f"PLAYER CREDIT RATING (PCR) — Legenda\n")
        f.write(f"Snapshot: {SNAPSHOT_DATE} | Janela: ultimos {JANELA_DIAS} dias\n")
        f.write(f"Fonte: ps_bi.fct_player_activity_daily + dim_user (Athena)\n\n")
        f.write("COLUNAS:\n")
        f.write("  player_id       = ID interno do jogador (ecr_id, 18 digitos)\n")
        f.write("  external_id     = ID externo (Smartico user_ext_id)\n")
        f.write("  rating          = Classificacao PCR: D, C, B, A, AA, AAA\n")
        f.write("  pvs             = Player Value Score (0-100)\n")
        f.write("  ggr_total       = Gross Gaming Revenue em BRL (apostas - ganhos)\n")
        f.write("  ngr_total       = Net Gaming Revenue em BRL (GGR - BTR - RCA)\n")
        f.write("  total_deposits  = Depositos totais em BRL\n")
        f.write("  total_cashouts  = Saques totais em BRL\n")
        f.write("  num_deposits    = Quantidade de depositos\n")
        f.write("  days_active     = Dias com atividade no periodo\n")
        f.write("  recency_days    = Dias desde ultima atividade\n")
        f.write("  product_type    = CASINO, SPORT ou MISTO (casino+sport)\n")
        f.write("  casino_rounds   = Rodadas de casino\n")
        f.write("  sport_bets      = Apostas esportivas\n")
        f.write("  bonus_issued    = Bonus emitido em BRL\n")
        f.write("  bonus_ratio     = Bonus / Depositos (0 = sem bonus, >1 = alto)\n")
        f.write("  wd_ratio        = Saques / Depositos (>1 = saca mais que deposita)\n")
        f.write("  net_deposit     = Depositos - Saques em BRL\n")
        f.write("  margem_ggr      = GGR / Turnover (margem da casa sobre o jogador)\n")
        f.write("  ggr_por_dia     = GGR / Dias ativos\n")
        f.write("  affiliate_id    = ID do afiliado de aquisicao\n\n")
        f.write("RATINGS:\n")
        f.write("  AAA = Whale saudavel (top 4%) — alto valor, risco minimo\n")
        f.write("  AA  = VIP (top 4-12%) — alto valor, monitoramento proativo\n")
        f.write("  A   = Premium (top 12-25%) — bom jogador, consistente\n")
        f.write("  B   = Engajado (25-45%) — em desenvolvimento\n")
        f.write("  C   = Regular (45-65%) — valor baixo\n")
        f.write("  D   = Casual (bottom 35%) — sem engajamento ou prejuizo\n\n")
        f.write("FORMULA PVS (0-100):\n")
        f.write("  +25 GGR Total | +15 Deposito | +12 Recencia | +10 Margem\n")
        f.write("  +10 Num Depositos | +8 Dias Ativos | +5 Mix Produto\n")
        f.write("  +5 Taxa Atividade | -10 Sensibilidade Bonus\n")
    log.info(f"Legenda salva: {legenda_path}")

    log.info("Pipeline PCR concluido com sucesso!")
    return df, resumo


if __name__ == "__main__":
    main()
