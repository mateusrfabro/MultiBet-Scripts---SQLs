import argparse
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


def norm_email_series(s: pd.Series) -> pd.Series:
    s = norm_str_series(s)
    return s.str.lower()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pgs", required=True)
    ap.add_argument("--tap", required=True)
    ap.add_argument("--out_dir", required=True)
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    pgs_wanted = [
        "ext_id", "ecr_id", "signup_date", "email",
        "rg_cool_off_status", "is_rg_closed",
        "c_registration_status", "c_ecr_status", "account_category",
    ]
    pgs = read_csv_flexible(args.pgs, usecols=lambda c: c in set(pgs_wanted))

    tap_wanted = ["user_ext_id", "e_mail"]
    tap = read_csv_flexible(args.tap, usecols=lambda c: c in set(tap_wanted))

    if "ext_id" not in pgs.columns:
        raise ValueError("PGS não tem coluna ext_id (esperado). Confira out/pgs_full.csv.")
    if "user_ext_id" not in tap.columns:
        raise ValueError("TAP unificada não tem coluna user_ext_id (esperado). Confira out/tap_unified_union.csv.")

    pgs["ext_id"] = norm_str_series(pgs["ext_id"])
    tap["user_ext_id"] = norm_str_series(tap["user_ext_id"])

    pgs_ext_distinct = pgs["ext_id"].dropna().nunique()
    tap_ext_distinct = tap["user_ext_id"].dropna().nunique()

    tap_ext_set = set(tap["user_ext_id"].dropna().unique())
    pgs_not_in_tap = pgs[~pgs["ext_id"].isin(tap_ext_set)].copy()

    out_csv = out_dir / "pgs_not_in_tap_by_ext_id.csv"
    pgs_not_in_tap.to_csv(out_csv, index=False)

    inter_count = pgs["ext_id"].isin(tap_ext_set).sum()

    pgs_dup_rows = pgs[pgs["ext_id"].duplicated(keep=False) & pgs["ext_id"].notna()]
    if len(pgs_dup_rows) > 0:
        pgs_dup_rows.to_csv(out_dir / "pgs_duplicates_by_ext_id.csv", index=False)

    summary = []
    summary.append(f"PGS rows (loaded cols): {len(pgs)}")
    summary.append(f"PGS distinct ext_id: {pgs_ext_distinct}")
    summary.append(f"TAP distinct user_ext_id: {tap_ext_distinct}")
    summary.append(f"Intersection (PGS ext_id in TAP): {inter_count}")
    summary.append(f"PGS not in TAP (by ext_id): {len(pgs_not_in_tap)}")
    summary.append(f"Output: {out_csv.name}")
    if len(pgs_dup_rows) > 0:
        summary.append(f"PGS duplicate rows by ext_id: {len(pgs_dup_rows)} (see pgs_duplicates_by_ext_id.csv)")

    (out_dir / "pgs_vs_tap_summary.txt").write_text("\n".join(summary), encoding="utf-8")
    print(f"OK. Outputs em: {out_dir.resolve()}")


if __name__ == "__main__":
    main()