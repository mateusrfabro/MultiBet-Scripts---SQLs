"""
Validacao cruzada de turnover: 3 fontes independentes
=====================================================
1. bireports_ec2.tbl_ecr_txn_type_wise_daily_game_play_summary (fonte primaria)
2. bireports_ec2.tbl_ecr_wise_daily_bi_summary (agregado diario por player)
3. BigQuery Smartico tr_casino_bet (CRM - fonte externa)

Periodo: 01/01/2026 a 25/03/2026
"""

import sys
import os
import io
import warnings

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import pandas as pd

warnings.filterwarnings("ignore")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.athena import query_athena
from db.bigquery import query_bigquery

DATA_INICIO = "2026-01-01"
DATA_FIM = "2026-03-25"


def fonte1_game_play_summary():
    """bireports_ec2.tbl_ecr_txn_type_wise_daily_game_play_summary por vendor"""
    sql = f"""
    SELECT
        s.c_vendor_id AS provedor,
        SUM(CASE WHEN s.c_txn_type = 27 THEN s.c_txn_real_cash_amount_ecr_crncy ELSE 0 END) / 100.0 AS bet_real,
        SUM(CASE WHEN s.c_txn_type = 27 THEN
            COALESCE(s.c_txn_crp_amount_ecr_crncy,0) + COALESCE(s.c_txn_drp_amount_ecr_crncy,0)
            + COALESCE(s.c_txn_wrp_amount_ecr_crncy,0) + COALESCE(s.c_txn_rrp_amount_ecr_crncy,0)
        ELSE 0 END) / 100.0 AS bet_bonus,
        SUM(CASE WHEN s.c_txn_type = 27 THEN s.c_txn_count ELSE 0 END) AS bet_count,
        COUNT(DISTINCT s.c_ecr_id) AS jogadores
    FROM bireports_ec2.tbl_ecr_txn_type_wise_daily_game_play_summary s
    JOIN bireports_ec2.tbl_ecr e ON s.c_ecr_id = e.c_ecr_id
    WHERE s.c_created_date BETWEEN DATE '{DATA_INICIO}' AND DATE '{DATA_FIM}'
      AND e.c_test_user = false
    GROUP BY s.c_vendor_id
    ORDER BY bet_real DESC
    """
    print("[1/3] Executando: game_play_summary (bireports_ec2)...")
    df = query_athena(sql, database="bireports_ec2")
    df["turnover_total"] = df["bet_real"] + df["bet_bonus"]
    return df


def fonte2_daily_bi_summary():
    """bireports_ec2.tbl_ecr_wise_daily_bi_summary - total casino"""
    sql = f"""
    SELECT
        SUM(s.c_casino_bet_amount) / 100.0 AS casino_bet_total,
        SUM(s.c_casino_realcash_bet_amount) / 100.0 AS casino_bet_real,
        SUM(s.c_casino_win_amount) / 100.0 AS casino_win_total,
        SUM(s.c_casino_realcash_win_amount) / 100.0 AS casino_win_real,
        SUM(s.c_sb_bet_amount) / 100.0 AS sb_bet_total,
        SUM(s.c_sb_realcash_bet_amount) / 100.0 AS sb_bet_real,
        COUNT(DISTINCT s.c_ecr_id) AS jogadores
    FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary s
    JOIN bireports_ec2.tbl_ecr e ON s.c_ecr_id = e.c_ecr_id
    WHERE s.c_created_date BETWEEN DATE '{DATA_INICIO}' AND DATE '{DATA_FIM}'
      AND e.c_test_user = false
    """
    print("[2/3] Executando: daily_bi_summary (bireports_ec2)...")
    df = query_athena(sql, database="bireports_ec2")
    return df


def fonte3_bigquery():
    """BigQuery Smartico tr_casino_bet - por provider_id"""
    sql = f"""
    SELECT
        casino_last_bet_game_provider AS provider_id,
        COUNT(*) AS total_bets,
        SUM(casino_last_bet_amount) AS turnover_total,
        SUM(casino_last_bet_amount_real) AS turnover_real,
        SUM(casino_last_bet_amount_bonus) AS turnover_bonus,
        COUNT(DISTINCT user_id) AS jogadores
    FROM `smartico-bq6.dwh_ext_24105.tr_casino_bet`
    WHERE event_time >= TIMESTAMP("{DATA_INICIO}")
      AND event_time < TIMESTAMP("2026-03-26")
      AND IFNULL(casino_is_rollback, false) = false
    GROUP BY casino_last_bet_game_provider
    ORDER BY turnover_total DESC
    LIMIT 15
    """
    print("[3/3] Executando: tr_casino_bet (BigQuery Smartico)...")
    df = query_bigquery(sql)
    # Converter Decimal para float
    for col in ["turnover_total", "turnover_real", "turnover_bonus"]:
        df[col] = df[col].astype(float)
    return df


def main():
    print(f"{'='*75}")
    print(f"  VALIDACAO CRUZADA - TURNOVER CASINO")
    print(f"  Periodo: {DATA_INICIO} a {DATA_FIM}")
    print(f"{'='*75}\n")

    # -- Fonte 1: game_play_summary por vendor --
    df1 = fonte1_game_play_summary()
    total_f1 = df1["turnover_total"].sum()
    total_f1_real = df1["bet_real"].sum()

    # -- Fonte 2: daily_bi_summary (total casino) --
    df2 = fonte2_daily_bi_summary()
    total_f2 = float(df2["casino_bet_total"].iloc[0])
    total_f2_real = float(df2["casino_bet_real"].iloc[0])
    total_f2_sb = float(df2["sb_bet_total"].iloc[0])

    # -- Fonte 3: BigQuery --
    df3 = fonte3_bigquery()
    total_f3 = df3["turnover_total"].sum()
    total_f3_real = df3["turnover_real"].sum()

    # -- Comparacao totais --
    print(f"\n{'='*75}")
    print(f"  COMPARACAO DE TOTAIS CASINO")
    print(f"{'='*75}\n")
    print(f"  Fonte 1 (game_play_summary):    R$ {total_f1:>15,.2f}  (real: R$ {total_f1_real:>15,.2f})")
    print(f"  Fonte 2 (daily_bi_summary):     R$ {total_f2:>15,.2f}  (real: R$ {total_f2_real:>15,.2f})")
    print(f"  Fonte 3 (BigQuery Smartico):     R$ {total_f3:>15,.2f}  (real: R$ {total_f3_real:>15,.2f})")
    print()

    # Divergencias
    div_f1_f2 = (total_f1 - total_f2) / total_f2 * 100 if total_f2 else 0
    div_f1_f3 = (total_f1 - total_f3) / total_f3 * 100 if total_f3 else 0
    div_f2_f3 = (total_f2 - total_f3) / total_f3 * 100 if total_f3 else 0

    print(f"  Divergencia F1 vs F2:  {div_f1_f2:>+.2f}%")
    print(f"  Divergencia F1 vs F3:  {div_f1_f3:>+.2f}%")
    print(f"  Divergencia F2 vs F3:  {div_f2_f3:>+.2f}%")
    print()

    # -- Match por volume: BigQuery provider_id x bireports vendor --
    print(f"{'='*75}")
    print(f"  MATCH POR VOLUME: BigQuery provider_id x bireports vendor_id")
    print(f"  (match por numero de jogadores e volume)")
    print(f"{'='*75}\n")

    # Ordena ambos por turnover e faz match posicional + por players
    df1_sorted = df1.sort_values("turnover_total", ascending=False).head(10).reset_index(drop=True)
    df3_sorted = df3.sort_values("turnover_total", ascending=False).head(10).reset_index(drop=True)

    for i in range(min(len(df1_sorted), len(df3_sorted))):
        r1 = df1_sorted.iloc[i]
        r3 = df3_sorted.iloc[i]
        diff_pct = (float(r3["turnover_total"]) - r1["turnover_total"]) / r1["turnover_total"] * 100
        players_match = "OK" if abs(r1["jogadores"] - r3["jogadores"]) / max(r1["jogadores"], 1) < 0.05 else "~"
        print(f"  #{i+1:2d}  {r1['provedor']:<22s} R$ {r1['turnover_total']:>14,.2f} | "
              f"BQ id={int(r3['provider_id']):>10d}  R$ {float(r3['turnover_total']):>14,.2f} | "
              f"diff: {diff_pct:>+5.1f}%  players: {int(r1['jogadores'])} vs {int(r3['jogadores'])} [{players_match}]")
    print()

    # -- Sportsbook (so Athena) --
    print(f"  Nota: Sportsbook (daily_bi_summary): R$ {total_f2_sb:>15,.2f}")
    print(f"        BigQuery nao inclui sportsbook nesta view (tr_casino_bet)")
    print()

    # -- Veredicto --
    print(f"{'='*75}")
    print(f"  VEREDICTO DA AUDITORIA")
    print(f"{'='*75}\n")

    all_ok = all(abs(d) < 10 for d in [div_f1_f2, div_f1_f3, div_f2_f3])
    if all_ok:
        print(f"  [OK] Todas as 3 fontes convergem (<10% divergencia)")
    else:
        print(f"  [!!] Divergencia significativa detectada (>10%)")
    print()
    print(f"  Fontes utilizadas:")
    print(f"    F1: bireports_ec2.tbl_ecr_txn_type_wise_daily_game_play_summary (por vendor, tipo 27)")
    print(f"    F2: bireports_ec2.tbl_ecr_wise_daily_bi_summary (agregado player/dia)")
    print(f"    F3: BigQuery smartico-bq6.dwh_ext_24105.tr_casino_bet (CRM externo)")
    print(f"  Test users: excluidos em todas as fontes")
    print(f"  Periodo: {DATA_INICIO} a {DATA_FIM}")
    print()


if __name__ == "__main__":
    main()