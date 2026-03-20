"""
Entregável Step 4 — Follow-up do Jonathan
==========================================
1) Análise de account_category na base PGS
2) Double check PGS x TAP com números claros

Usa os outputs já gerados (pgs_full.csv e tap_unified_union.csv).
"""

from pathlib import Path
import pandas as pd


def read_csv_flexible(path: str, usecols=None) -> pd.DataFrame:
    seps_to_try = [",", ";", "\t", "|"]
    encs_to_try = ["utf-8-sig", "utf-8", "latin1"]
    last_err = None
    for enc in encs_to_try:
        for sep in seps_to_try:
            try:
                df = pd.read_csv(path, sep=sep, dtype=str, encoding=enc, low_memory=False, usecols=usecols)
                if df.shape[1] == 1 and sep != "|":
                    continue
                return df
            except Exception as e:
                last_err = e
    raise RuntimeError(f"Falha lendo {path}: {last_err}")


def norm_str_series(s: pd.Series) -> pd.Series:
    s = s.astype(str).str.strip()
    s = s.replace({"nan": None, "None": None, "": None})
    s = s.str.replace(r"\.0$", "", regex=True)
    return s


def main():
    out_dir = Path("out")
    out_dir.mkdir(parents=True, exist_ok=True)

    # --- Carregar PGS e TAP ---
    print("Carregando pgs_full.csv...")
    pgs = read_csv_flexible(str(out_dir / "pgs_full.csv"))

    print("Carregando tap_unified_union.csv (somente user_ext_id)...")
    tap_full = read_csv_flexible(str(out_dir / "tap_unified_union.csv"))
    tap = tap_full[["user_ext_id"]].copy()
    del tap_full

    # Normalizar chaves
    pgs["ext_id"] = norm_str_series(pgs["ext_id"])
    tap["user_ext_id"] = norm_str_series(tap["user_ext_id"])

    # =========================================================
    # 1) ANÁLISE DE ACCOUNT_CATEGORY (PGS)
    # =========================================================
    print("\n--- Análise account_category ---")

    if "account_category" not in pgs.columns:
        print("ERRO: coluna account_category não encontrada na base PGS!")
        return

    pgs["account_category"] = norm_str_series(pgs["account_category"])

    # Contagem simples por valor
    cat_counts = (
        pgs["account_category"]
        .fillna("(vazio/null)")
        .value_counts()
        .reset_index()
    )
    cat_counts.columns = ["account_category", "total_jogadores"]
    cat_counts = cat_counts.sort_values("total_jogadores", ascending=False)
    cat_counts.to_csv(out_dir / "pgs_account_category_counts.csv", index=False)

    print("\nDistribuição de account_category:")
    for _, row in cat_counts.iterrows():
        pct = row["total_jogadores"] / len(pgs) * 100
        print(f"  {row['account_category']:20s}  {row['total_jogadores']:>10,}  ({pct:.2f}%)")

    # Tabela cruzada: account_category x (está na TAP ou não)
    tap_ext_set = set(tap["user_ext_id"].dropna().unique())
    pgs["na_tap"] = pgs["ext_id"].isin(tap_ext_set).map({True: "Sim", False: "Não"})

    cross = (
        pgs.groupby(["account_category", "na_tap"], dropna=False)
        .size()
        .reset_index(name="total_jogadores")
    )
    cross["account_category"] = cross["account_category"].fillna("(vazio/null)")
    cross.to_csv(out_dir / "pgs_account_category_vs_tap.csv", index=False)

    print("\nCruzamento account_category x presença na TAP:")
    pivot = cross.pivot_table(
        index="account_category", columns="na_tap",
        values="total_jogadores", fill_value=0, aggfunc="sum"
    )
    if "Sim" not in pivot.columns:
        pivot["Sim"] = 0
    if "Não" not in pivot.columns:
        pivot["Não"] = 0
    pivot["Total"] = pivot["Sim"] + pivot["Não"]
    pivot = pivot.sort_values("Total", ascending=False)
    print(pivot.to_string())
    pivot.to_csv(out_dir / "pgs_account_category_vs_tap_pivot.csv")

    # =========================================================
    # 2) DOUBLE CHECK PGS x TAP
    # =========================================================
    total_pgs = len(pgs)
    total_pgs_distinct = pgs["ext_id"].dropna().nunique()
    total_tap = len(tap)
    total_tap_distinct = tap["user_ext_id"].dropna().nunique()

    pgs_in_tap = pgs["ext_id"].isin(tap_ext_set).sum()
    pgs_not_in_tap = total_pgs - pgs_in_tap

    print("\n--- Double Check PGS x TAP ---")
    print(f"  Total de jogadores PGS (linhas):       {total_pgs:>12,}")
    print(f"  Total de jogadores PGS (ext_id únicos): {total_pgs_distinct:>11,}")
    print(f"  Total de jogadores TAP (linhas):       {total_tap:>12,}")
    print(f"  Total de jogadores TAP (user_ext_id):  {total_tap_distinct:>12,}")
    print(f"  PGS encontrados na TAP:                {pgs_in_tap:>12,}")
    print(f"  PGS NÃO encontrados na TAP:            {pgs_not_in_tap:>12,}")

    # =========================================================
    # 3) RESUMO CONSOLIDADO
    # =========================================================
    summary_lines = [
        "=" * 60,
        "FOLLOW-UP — Análises adicionais solicitadas por Jonathan",
        "=" * 60,
        "",
        "1) DISTRIBUIÇÃO DE ACCOUNT_CATEGORY (PGS)",
        "-" * 40,
    ]
    for _, row in cat_counts.iterrows():
        pct = row["total_jogadores"] / len(pgs) * 100
        summary_lines.append(
            f"  {row['account_category']:20s}  {row['total_jogadores']:>10,}  ({pct:.2f}%)"
        )

    summary_lines += [
        "",
        "2) DOUBLE CHECK — PGS x TAP",
        "-" * 40,
        f"  Total de jogadores PGS:                {total_pgs:>12,}",
        f"  Total de jogadores PGS (ext_id únicos): {total_pgs_distinct:>11,}",
        f"  Total de jogadores TAP:                {total_tap:>12,}",
        f"  Total de jogadores TAP (únicos):       {total_tap_distinct:>12,}",
        f"  PGS encontrados na TAP:                {pgs_in_tap:>12,}",
        f"  PGS NÃO encontrados na TAP:            {pgs_not_in_tap:>12,}",
        "",
        "NOTA: A TAP possui mais registros que a PGS porque inclui",
        "jogadores que existem na Smartico mas não na base PGS.",
        "Os 2.225 que não estão na TAP são contas PGS sem registro",
        "correspondente na Smartico TAP.",
        "",
        "3) CRUZAMENTO: ACCOUNT_CATEGORY x PRESENÇA NA TAP",
        "-" * 40,
        "  (ver pgs_account_category_vs_tap_pivot.csv para detalhes)",
        "",
        pivot.to_string(),
        "",
        "=" * 60,
        "Arquivos gerados:",
        "  - pgs_account_category_counts.csv",
        "  - pgs_account_category_vs_tap.csv",
        "  - pgs_account_category_vs_tap_pivot.csv",
        "  - followup_summary.txt (este arquivo)",
        "=" * 60,
    ]

    summary_text = "\n".join(summary_lines)
    (out_dir / "followup_summary.txt").write_text(summary_text, encoding="utf-8")
    print(f"\nResumo salvo em: {(out_dir / 'followup_summary.txt').resolve()}")
    print("Concluído!")


if __name__ == "__main__":
    main()