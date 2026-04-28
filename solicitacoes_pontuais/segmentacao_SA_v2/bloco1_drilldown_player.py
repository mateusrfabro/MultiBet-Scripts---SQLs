"""
Drill-down: pega 1 player do CSV do Castrin e quebra atividade dele dia-a-dia
para entender qual janela ele usou no GGR_30D, DEPOSIT_AMOUNT_30D, etc.

Estrategia:
- Pega o primeiro player do CSV (com atividade) e os valores 30D dele
- Roda query Athena por dia (90 dias)
- Computa cumulativo por janela: 7d, 14d, 30d, abril-mes, 90d
- Mostra qual janela bate com os valores do Castrin
"""
import sys
from pathlib import Path
from datetime import date, timedelta
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from db.athena import query_athena

CSV = r"C:/Users/NITRO/Downloads/players_segmento_SA.csv"
SNAPSHOT = date(2026, 4, 27)


def main():
    df = pd.read_csv(CSV, sep=";", decimal=",", low_memory=False)
    # Pega 5 players com atividade significativa
    df_at = df[(df["DEPOSIT_COUNT_30D"].fillna(0) > 5) & (df["GGR_30D"].fillna(0) > 100)]
    sample = df_at.sample(5, random_state=42)
    print("[CASTRIN] Sample de 5 players com atividade:")
    print(sample[["player_id", "PCR_RATING", "RECENCY_DAYS", "GGR_30D", "NGR_30D",
                  "DEPOSIT_AMOUNT_30D", "DEPOSIT_COUNT_30D",
                  "WITHDRAWAL_AMOUNT_30D", "WITHDRAWAL_COUNT_30D"]].to_string(index=False))

    ids = sample["player_id"].astype("int64").tolist()
    ids_str = ", ".join(str(i) for i in ids)

    # Pega atividade diaria 90d
    sql = f"""
    SELECT
        f.player_id, f.activity_date,
        COALESCE(SUM(f.ggr_base), 0) AS ggr,
        COALESCE(SUM(f.ngr_base), 0) AS ngr,
        COALESCE(SUM(f.deposit_success_count), 0) AS dep_count,
        COALESCE(SUM(f.deposit_success_base), 0) AS dep_amount,
        COALESCE(SUM(f.cashout_success_count), 0) AS wd_count,
        COALESCE(SUM(f.cashout_success_base), 0) AS wd_amount
    FROM ps_bi.fct_player_activity_daily f
    WHERE f.activity_date >= DATE '{SNAPSHOT - timedelta(days=120)}'
      AND f.activity_date <= DATE '{SNAPSHOT}'
      AND f.player_id IN ({ids_str})
    GROUP BY f.player_id, f.activity_date
    ORDER BY f.player_id, f.activity_date
    """
    print("\n[Athena] Rodando query daily breakdown...")
    daily = query_athena(sql, database="ps_bi")
    print(f"[Athena] {len(daily)} linhas (player x dia)")

    # Para cada player, calcula varias janelas
    JANELAS = {
        "30d_excl_D":  (SNAPSHOT - timedelta(days=30), SNAPSHOT - timedelta(days=1)),
        "30d_incl_D":  (SNAPSHOT - timedelta(days=29), SNAPSHOT),
        "abril_full":  (date(2026, 4, 1),              date(2026, 4, 30)),
        "abril_27d":   (date(2026, 4, 1),              SNAPSHOT),
        "marco_full":  (date(2026, 3, 1),              date(2026, 3, 31)),
        "60d":         (SNAPSHOT - timedelta(days=60), SNAPSHOT - timedelta(days=1)),
        "90d":         (SNAPSHOT - timedelta(days=90), SNAPSHOT - timedelta(days=1)),
    }

    daily["activity_date"] = pd.to_datetime(daily["activity_date"]).dt.date

    print(f"\n{'='*120}")
    print(f"{'Player':<22} {'Janela':<14} {'GGR':>10} {'CASTRIN_GGR':>12} {'DEP_AMT':>10} {'CASTRIN_DEP':>12} {'DEP_CT':>7} {'CASTRIN_DC':>10}")
    print("-"*120)

    for _, row in sample.iterrows():
        pid = int(row["player_id"])
        ggr_c = row["GGR_30D"]
        dep_c = row["DEPOSIT_AMOUNT_30D"]
        depc_c = row["DEPOSIT_COUNT_30D"]
        wd_c = row["WITHDRAWAL_AMOUNT_30D"]

        sub = daily[daily["player_id"] == pid]
        if sub.empty:
            print(f"{pid:<22} (sem dados Athena)")
            continue

        for jname, (ini, fim) in JANELAS.items():
            mask = (sub["activity_date"] >= ini) & (sub["activity_date"] <= fim)
            ggr_n = sub.loc[mask, "ggr"].sum()
            dep_n = sub.loc[mask, "dep_amount"].sum()
            depc_n = sub.loc[mask, "dep_count"].sum()
            mark_g = "MATCH" if abs(ggr_n - ggr_c) < 1 else ""
            mark_d = "MATCH" if abs(dep_n - dep_c) < 1 else ""
            mark_c = "MATCH" if abs(depc_n - depc_c) < 1 else ""
            mark = " " + " ".join(m for m in [mark_g, mark_d, mark_c] if m)
            print(f"{pid:<22} {jname:<14} {ggr_n:>10.2f} {ggr_c:>12.2f} {dep_n:>10.2f} {dep_c:>12.2f} {depc_n:>7.0f} {depc_c:>10.0f}{mark}")
        print("-"*120)


if __name__ == "__main__":
    main()
