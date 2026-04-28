"""
BLOCO 1 — VALIDACAO: Metricas Financeiras 30d
==============================================

Objetivo: gerar 8 colunas de metricas 30d para os jogadores e comparar
com os valores do CSV que o Castrin enviou (Downloads/players_segmento_SA.csv)
ANTES de integrar no pipeline de producao.

Colunas geradas:
  - GGR_30D
  - NGR_30D
  - DEPOSIT_AMOUNT_30D
  - DEPOSIT_COUNT_30D
  - WITHDRAWAL_AMOUNT_30D
  - WITHDRAWAL_COUNT_30D
  - AVG_DEPOSIT_TICKET_30D
  - AVG_DEPOSIT_TICKET_LIFETIME

Fonte: ps_bi.fct_player_activity_daily (view dbt, BRL pre-agregado)

Validacao:
  Pega 50 player_ids aleatorios do CSV do Castrin e compara
  os valores 30d que ele tem vs o que calculamos. Tolerancia 1 BRL.
"""
import sys
from pathlib import Path
from datetime import date, timedelta

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from db.athena import query_athena

CSV_CASTRIN = r"C:/Users/NITRO/Downloads/players_segmento_SA.csv"
SNAPSHOT_DATE = date(2026, 4, 27)
# TESTE: Castrin pode estar usando 90d apesar do nome "30D".
# Pipeline PCR dele e janela 90d rolling.
import os
JANELA_DIAS = int(os.getenv("BLOCO1_JANELA_DIAS", "90"))
JANELA_30D_INICIO = SNAPSHOT_DATE - timedelta(days=JANELA_DIAS)
print(f"[CONFIG] Janela teste = {JANELA_DIAS}d")

SAMPLE_SIZE = 50  # players para validacao


def extrair_metricas_30d(player_ids: list) -> pd.DataFrame:
    """
    Roda 1 query Athena para os player_ids da amostra.
    Janela: 30d terminando em snapshot_date (D-1, exclui parcial).
    """
    ids_str = ", ".join(str(p) for p in player_ids)

    sql = f"""
    -- Bloco 1: metricas financeiras 30d para validacao
    -- Janela: {JANELA_30D_INICIO} a {SNAPSHOT_DATE - timedelta(days=1)} (D-1, exclui parcial)
    WITH metrics_30d AS (
        SELECT
            f.player_id,
            -- GGR / NGR
            COALESCE(SUM(f.ggr_base), 0) AS ggr_30d,
            COALESCE(SUM(f.ngr_base), 0) AS ngr_30d,
            -- Depositos
            COALESCE(SUM(f.deposit_success_count), 0) AS deposit_count_30d,
            COALESCE(SUM(f.deposit_success_base), 0)  AS deposit_amount_30d,
            -- Saques
            COALESCE(SUM(f.cashout_success_count), 0) AS withdrawal_count_30d,
            COALESCE(SUM(f.cashout_success_base), 0)  AS withdrawal_amount_30d
        FROM ps_bi.fct_player_activity_daily f
        WHERE f.activity_date >= DATE '{JANELA_30D_INICIO}'
          AND f.activity_date <  DATE '{SNAPSHOT_DATE}'
          AND f.player_id IN ({ids_str})
        GROUP BY f.player_id
    ),
    metrics_lifetime AS (
        SELECT
            f.player_id,
            COALESCE(SUM(f.deposit_success_count), 0) AS deposit_count_lifetime,
            COALESCE(SUM(f.deposit_success_base), 0)  AS deposit_amount_lifetime
        FROM ps_bi.fct_player_activity_daily f
        WHERE f.player_id IN ({ids_str})
        GROUP BY f.player_id
    )
    SELECT
        m.player_id,
        m.ggr_30d,
        m.ngr_30d,
        m.deposit_count_30d,
        m.deposit_amount_30d,
        m.withdrawal_count_30d,
        m.withdrawal_amount_30d,
        CASE WHEN m.deposit_count_30d > 0
             THEN m.deposit_amount_30d * 1.0 / m.deposit_count_30d
             ELSE NULL END AS avg_deposit_ticket_30d,
        CASE WHEN l.deposit_count_lifetime > 0
             THEN l.deposit_amount_lifetime * 1.0 / l.deposit_count_lifetime
             ELSE NULL END AS avg_deposit_ticket_lifetime
    FROM metrics_30d m
    LEFT JOIN metrics_lifetime l ON m.player_id = l.player_id
    """
    print(f"[Athena] Rodando query para {len(player_ids)} players...")
    df = query_athena(sql, database="ps_bi")
    print(f"[Athena] Retornou {len(df)} linhas.")
    return df


def carregar_castrin_csv() -> pd.DataFrame:
    """Carrega CSV do Castrin com colunas relevantes."""
    print(f"[CSV] Carregando {CSV_CASTRIN}...")
    df = pd.read_csv(CSV_CASTRIN, sep=";", decimal=",", low_memory=False)
    cols = ["player_id", "GGR_30D", "NGR_30D",
            "DEPOSIT_AMOUNT_30D", "DEPOSIT_COUNT_30D",
            "WITHDRAWAL_AMOUNT_30D", "WITHDRAWAL_COUNT_30D",
            "AVG_DEPOSIT_TICKET_30D", "AVG_DEPOSIT_TICKET_LIFETIME"]
    df = df[cols].copy()
    df.columns = [c.lower() for c in df.columns]
    print(f"[CSV] {len(df)} linhas, {df['player_id'].nunique()} players unicos.")
    return df


def comparar(df_athena: pd.DataFrame, df_castrin: pd.DataFrame, tolerancia: float = 1.0):
    """Compara colunas equivalentes player-a-player."""
    df_athena["player_id"] = df_athena["player_id"].astype("int64")
    df_castrin["player_id"] = df_castrin["player_id"].astype("int64")

    merge = df_castrin.merge(df_athena, on="player_id", how="inner",
                              suffixes=("_castrin", "_nosso"))
    print(f"\n[COMPARE] {len(merge)} players matched")

    pares = [
        ("ggr_30d_castrin", "ggr_30d_nosso", "GGR_30D"),
        ("ngr_30d_castrin", "ngr_30d_nosso", "NGR_30D"),
        ("deposit_amount_30d_castrin", "deposit_amount_30d_nosso", "DEPOSIT_AMOUNT_30D"),
        ("deposit_count_30d_castrin", "deposit_count_30d_nosso", "DEPOSIT_COUNT_30D"),
        ("withdrawal_amount_30d_castrin", "withdrawal_amount_30d_nosso", "WITHDRAWAL_AMOUNT_30D"),
        ("withdrawal_count_30d_castrin", "withdrawal_count_30d_nosso", "WITHDRAWAL_COUNT_30D"),
        ("avg_deposit_ticket_30d_castrin", "avg_deposit_ticket_30d_nosso", "AVG_DEPOSIT_TICKET_30D"),
        ("avg_deposit_ticket_lifetime_castrin", "avg_deposit_ticket_lifetime_nosso", "AVG_DEPOSIT_TICKET_LIFETIME"),
    ]

    print(f"\n{'='*100}")
    print(f"{'COLUNA':<32} {'MATCH':<10} {'DIFF MEDIA':>12} {'DIFF MAX':>12} {'STATUS':<15}")
    print("-"*100)

    resultados = []
    for col_c, col_n, label in pares:
        if col_c not in merge.columns or col_n not in merge.columns:
            print(f"{label:<32} COLUNA AUSENTE")
            continue
        c = pd.to_numeric(merge[col_c], errors="coerce").fillna(0)
        n = pd.to_numeric(merge[col_n], errors="coerce").fillna(0)
        diff = (c - n).abs()
        match_pct = (diff <= tolerancia).mean() * 100
        diff_media = diff.mean()
        diff_max = diff.max()
        status = "OK" if match_pct >= 95 else ("REVISAR" if match_pct >= 80 else "DIVERGENTE")
        print(f"{label:<32} {match_pct:>6.1f}%   {diff_media:>12.2f} {diff_max:>12.2f} {status:<15}")
        resultados.append({"col": label, "match_pct": match_pct,
                          "diff_media": diff_media, "diff_max": diff_max, "status": status})
    print("="*100)

    print("\n[AMOSTRAS DIVERGENTES — top 5 maiores diferencas em GGR_30D]")
    if "ggr_30d_castrin" in merge.columns:
        merge["diff_ggr"] = (pd.to_numeric(merge["ggr_30d_castrin"], errors="coerce").fillna(0)
                            - pd.to_numeric(merge["ggr_30d_nosso"], errors="coerce").fillna(0)).abs()
        top_div = merge.nlargest(5, "diff_ggr")[["player_id", "ggr_30d_castrin", "ggr_30d_nosso", "diff_ggr"]]
        print(top_div.to_string(index=False))
    return resultados


def main():
    df_castrin = carregar_castrin_csv()

    # Amostragem aleatoria mas ESTAVEL (seed fixo) — players com atividade
    df_castrin_ativo = df_castrin[
        (df_castrin["deposit_count_30d"].fillna(0) > 0)
        | (df_castrin["ggr_30d"].fillna(0) != 0)
    ]
    print(f"[CSV] {len(df_castrin_ativo)} players com atividade 30d (filtro p/ amostra)")

    sample = df_castrin_ativo.sample(n=min(SAMPLE_SIZE, len(df_castrin_ativo)),
                                      random_state=42)
    sample_ids = sample["player_id"].astype("int64").tolist()
    print(f"[SAMPLE] {len(sample_ids)} players selecionados (seed=42)")

    df_athena = extrair_metricas_30d(sample_ids)

    if df_athena.empty:
        print("[ERRO] Athena retornou 0 linhas. Verificar SQL ou janela.")
        return

    comparar(df_athena, df_castrin)


if __name__ == "__main__":
    main()
