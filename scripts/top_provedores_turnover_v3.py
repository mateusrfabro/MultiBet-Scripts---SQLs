"""
Top 10 Provedores por Turnover + Top 5 Jogos por Provedor (CORRIGIDO v3)
=========================================================================
CORRECAO CRITICA: Descontar rollbacks (txn_type=72) do turnover e GGR.
  - Turnover Liquido = Bets (27) - Rollbacks (72)
  - GGR = Turnover Liquido - Wins (45)

Periodo: 01/01/2026 a 25/03/2026
Fonte: bireports_ec2 (dados completos, centavos /100)
Jogos: ps_bi.fct_casino_activity_daily + tbl_vendor_games_mapping_data
"""

import sys
import os
import io
import warnings
from datetime import datetime

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import pandas as pd

warnings.filterwarnings("ignore")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.athena import query_athena

DATA_INICIO = "2026-01-01"
DATA_FIM = "2026-03-24"
OUTPUT_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "reports",
    "top_provedores_turnover_20260101_20260324_FINAL.xlsx",
)


def query_top_provedores():
    """Top 10 provedores com rollbacks descontados."""
    sql = f"""
    SELECT
        s.c_vendor_id AS provedor,
        COUNT(DISTINCT s.c_ecr_id) AS jogadores_unicos,
        SUM(CASE WHEN s.c_txn_type = 27 THEN s.c_txn_count ELSE 0 END) AS total_apostas,
        -- Turnover bruto (bets tipo 27)
        SUM(CASE WHEN s.c_txn_type = 27 THEN s.c_txn_real_cash_amount_ecr_crncy ELSE 0 END) / 100.0 AS turnover_bruto_brl,
        -- Rollbacks (tipo 72)
        SUM(CASE WHEN s.c_txn_type = 72 THEN s.c_txn_real_cash_amount_ecr_crncy ELSE 0 END) / 100.0 AS rollbacks_brl,
        -- Turnover liquido = Bets - Rollbacks
        (SUM(CASE WHEN s.c_txn_type = 27 THEN s.c_txn_real_cash_amount_ecr_crncy ELSE 0 END)
       - SUM(CASE WHEN s.c_txn_type = 72 THEN s.c_txn_real_cash_amount_ecr_crncy ELSE 0 END)) / 100.0 AS turnover_liquido_brl,
        -- Wins (tipo 45)
        SUM(CASE WHEN s.c_txn_type = 45 THEN s.c_txn_real_cash_amount_ecr_crncy ELSE 0 END) / 100.0 AS wins_brl,
        -- GGR = Turnover Liquido - Wins
        (SUM(CASE WHEN s.c_txn_type = 27 THEN s.c_txn_real_cash_amount_ecr_crncy ELSE 0 END)
       - SUM(CASE WHEN s.c_txn_type = 72 THEN s.c_txn_real_cash_amount_ecr_crncy ELSE 0 END)
       - SUM(CASE WHEN s.c_txn_type = 45 THEN s.c_txn_real_cash_amount_ecr_crncy ELSE 0 END)) / 100.0 AS ggr_brl,
        -- Hold Rate = GGR / Turnover Liquido
        CASE
            WHEN (SUM(CASE WHEN s.c_txn_type = 27 THEN s.c_txn_real_cash_amount_ecr_crncy ELSE 0 END)
                - SUM(CASE WHEN s.c_txn_type = 72 THEN s.c_txn_real_cash_amount_ecr_crncy ELSE 0 END)) > 0
            THEN CAST(
                (SUM(CASE WHEN s.c_txn_type = 27 THEN s.c_txn_real_cash_amount_ecr_crncy ELSE 0 END)
               - SUM(CASE WHEN s.c_txn_type = 72 THEN s.c_txn_real_cash_amount_ecr_crncy ELSE 0 END)
               - SUM(CASE WHEN s.c_txn_type = 45 THEN s.c_txn_real_cash_amount_ecr_crncy ELSE 0 END))
               AS DOUBLE)
               / (SUM(CASE WHEN s.c_txn_type = 27 THEN s.c_txn_real_cash_amount_ecr_crncy ELSE 0 END)
                - SUM(CASE WHEN s.c_txn_type = 72 THEN s.c_txn_real_cash_amount_ecr_crncy ELSE 0 END)) * 100
            ELSE 0
        END AS hold_rate_pct
    FROM bireports_ec2.tbl_ecr_txn_type_wise_daily_game_play_summary s
    JOIN bireports_ec2.tbl_ecr e ON s.c_ecr_id = e.c_ecr_id
    WHERE s.c_created_date BETWEEN DATE '{DATA_INICIO}' AND DATE '{DATA_FIM}'
      AND e.c_test_user = false
      AND s.c_txn_type IN (27, 45, 72)
    GROUP BY s.c_vendor_id
    ORDER BY turnover_liquido_brl DESC
    LIMIT 10
    """
    print("[1/3] Top 10 provedores (bireports_ec2, COM rollbacks)...")
    df = query_athena(sql, database="bireports_ec2")
    print(f"  -> {len(df)} provedores")
    return df


def query_top_jogos(provedores: list):
    """Top 5 jogos por provedor via ps_bi + mapping."""
    provedor_list = ", ".join([f"'{p}'" for p in provedores])
    sql = f"""
    WITH game_data AS (
        SELECT
            m.c_vendor_id AS provedor,
            m.c_game_desc AS jogo,
            CAST(f.game_id AS VARCHAR) AS game_id,
            m.c_game_type_desc AS tipo_jogo,
            COUNT(DISTINCT f.player_id) AS jogadores_unicos,
            SUM(f.bet_count) AS total_apostas,
            SUM(f.bet_amount_local) AS turnover_brl,
            SUM(f.win_amount_local) AS wins_brl,
            SUM(f.ggr_local) AS ggr_brl,
            CASE WHEN SUM(f.real_bet_amount_local) > 0
                 THEN SUM(f.ggr_local) / SUM(f.real_bet_amount_local) * 100
                 ELSE 0 END AS hold_rate_pct
        FROM ps_bi.fct_casino_activity_daily f
        JOIN bireports_ec2.tbl_vendor_games_mapping_data m
            ON CAST(f.game_id AS VARCHAR) = m.c_game_id
        JOIN ps_bi.dim_user u ON f.player_id = u.ecr_id
        WHERE f.activity_date BETWEEN DATE '{DATA_INICIO}' AND DATE '{DATA_FIM}'
          AND u.is_test = false
          AND m.c_vendor_id IN ({provedor_list})
        GROUP BY m.c_vendor_id, m.c_game_desc, f.game_id, m.c_game_type_desc
    ),
    ranked AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY provedor ORDER BY turnover_brl DESC) AS rank_turnover
        FROM game_data
    )
    SELECT * FROM ranked WHERE rank_turnover <= 5
    ORDER BY provedor, rank_turnover
    """
    print("[2/3] Top 5 jogos por provedor (ps_bi + mapping)...")
    df = query_athena(sql, database="ps_bi")
    print(f"  -> {len(df)} linhas ({df['provedor'].nunique()} provedores)")
    return df


def query_validacao_daily_bi():
    """Validacao contra daily_bi_summary (referencia)."""
    sql = f"""
    SELECT
        SUM(s.c_casino_realcash_bet_amount) / 100.0 AS turnover_real,
        SUM(s.c_casino_realcash_win_amount) / 100.0 AS wins_real,
        (SUM(s.c_casino_realcash_bet_amount) - SUM(s.c_casino_realcash_win_amount)) / 100.0 AS ggr_real,
        COUNT(DISTINCT s.c_ecr_id) AS jogadores
    FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary s
    JOIN bireports_ec2.tbl_ecr e ON s.c_ecr_id = e.c_ecr_id
    WHERE s.c_created_date BETWEEN DATE '{DATA_INICIO}' AND DATE '{DATA_FIM}'
      AND e.c_test_user = false
    """
    print("[3/3] Validacao daily_bi_summary...")
    df = query_athena(sql, database="bireports_ec2")
    return df


def gerar_legenda():
    rows = [
        ("Top 10 Provedores", "provedor", "Nome do provedor/vendor", "texto"),
        ("Top 10 Provedores", "jogadores_unicos", "Jogadores distintos", "inteiro"),
        ("Top 10 Provedores", "total_apostas", "Numero de apostas (bets)", "inteiro"),
        ("Top 10 Provedores", "turnover_bruto_brl", "Bets brutas antes de rollbacks (R$)", "R$"),
        ("Top 10 Provedores", "rollbacks_brl", "Valor de bets estornadas/canceladas (R$)", "R$"),
        ("Top 10 Provedores", "turnover_liquido_brl", "Turnover efetivo = Bets - Rollbacks (R$)", "R$"),
        ("Top 10 Provedores", "wins_brl", "Total pago aos jogadores (R$)", "R$"),
        ("Top 10 Provedores", "ggr_brl", "GGR = Turnover Liquido - Wins (R$)", "R$"),
        ("Top 10 Provedores", "hold_rate_pct", "GGR / Turnover Liquido x 100 (%)", "%"),
        ("Top 5 Jogos", "jogo", "Nome do jogo", "texto"),
        ("Top 5 Jogos", "rank_turnover", "Posicao no ranking do provedor", "inteiro"),
        ("Top 5 Jogos", "turnover_brl", "Volume apostado (ps_bi, BRL)", "R$"),
        ("Top 5 Jogos", "ggr_brl", "GGR do jogo (ps_bi, BRL)", "R$"),
        ("Glossario", "Turnover Liquido", "Bets - Rollbacks. Volume efetivamente apostado", ""),
        ("Glossario", "GGR", "Gross Gaming Revenue = Turnover Liquido - Wins", ""),
        ("Glossario", "Hold Rate", "GGR / Turnover Liquido x 100", ""),
        ("Glossario", "Rollback", "Aposta cancelada/estornada (tipo 72). Dinheiro devolvido ao jogador", ""),
        ("Glossario", "Fonte", "bireports_ec2 (Athena), centavos /100, test users excluidos", ""),
    ]
    return pd.DataFrame(rows, columns=["Aba", "Campo", "Descricao", "Unidade"])


def main():
    # -- 1. Top 10 provedores --
    df_prov = query_top_provedores()

    print(f"\n{'='*100}")
    print(f"  TOP 10 PROVEDORES POR TURNOVER (COM ROLLBACKS DESCONTADOS)")
    print(f"  Periodo: {DATA_INICIO} a {DATA_FIM}")
    print(f"{'='*100}\n")

    for i, r in df_prov.iterrows():
        print(f"  #{i+1:2d}  {r['provedor']:<22s}  "
              f"Turnover Liq: R$ {r['turnover_liquido_brl']:>14,.2f}  "
              f"GGR: R$ {r['ggr_brl']:>12,.2f}  "
              f"Hold: {r['hold_rate_pct']:>5.1f}%  "
              f"Players: {r['jogadores_unicos']:>7,.0f}")

    total_t = df_prov["turnover_liquido_brl"].sum()
    total_g = df_prov["ggr_brl"].sum()
    print(f"\n  TOTAL  Turnover: R$ {total_t:>14,.2f}  GGR: R$ {total_g:>12,.2f}  Hold: {total_g/total_t*100:.1f}%")

    # -- 2. Top 5 jogos --
    df_jogos = query_top_jogos(df_prov["provedor"].tolist())

    print(f"\n{'='*100}")
    print(f"  TOP 5 JOGOS POR PROVEDOR")
    print(f"{'='*100}\n")

    for provedor in df_prov["provedor"].tolist():
        subset = df_jogos[df_jogos["provedor"] == provedor]
        if subset.empty:
            print(f"  > {provedor} (sem detalhamento)")
            continue
        print(f"  > {provedor}")
        for _, r in subset.iterrows():
            print(f"    {int(r['rank_turnover']):d}. {str(r['jogo'])[:42]:<44s}  "
                  f"Turnover: R$ {r['turnover_brl']:>12,.2f}  "
                  f"GGR: R$ {r['ggr_brl']:>10,.2f}  "
                  f"Players: {r['jogadores_unicos']:>6,.0f}")
        print()

    # -- 3. Validacao --
    df_val = query_validacao_daily_bi()
    val_ggr = float(df_val["ggr_real"].iloc[0])
    val_turnover = float(df_val["turnover_real"].iloc[0])

    print(f"{'='*100}")
    print(f"  VALIDACAO CRUZADA")
    print(f"{'='*100}")
    print(f"  game_play_summary (bets-rollbacks-wins):  GGR R$ {total_g:>14,.2f}  Turnover R$ {total_t:>14,.2f}")
    print(f"  daily_bi_summary (casino realcash):       GGR R$ {val_ggr:>14,.2f}  Turnover R$ {val_turnover:>14,.2f}")
    print(f"  Ref Mauro (matriz financeira):            GGR R$ {'15,244,894.06':>14s}")
    div = (total_g - val_ggr) / val_ggr * 100 if val_ggr else 0
    print(f"  Divergencia game_play vs daily_bi:        {div:>+.1f}%")
    print()

    # -- Export --
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    df_legenda = gerar_legenda()

    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
        df_prov.to_excel(writer, sheet_name="Top 10 Provedores", index=False)
        df_jogos.to_excel(writer, sheet_name="Top 5 Jogos por Provedor", index=False)
        df_legenda.to_excel(writer, sheet_name="Legenda", index=False)

    print(f"  Excel: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()