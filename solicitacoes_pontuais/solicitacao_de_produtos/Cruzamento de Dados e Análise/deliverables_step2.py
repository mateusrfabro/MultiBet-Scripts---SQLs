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
    ap.add_argument("--tap_a", required=True)
    ap.add_argument("--tap_b", required=True)
    ap.add_argument("--out_dir", required=True)
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    pgs_cols = ["ext_id", "email", "rg_cool_off_status", "is_rg_closed"]
    pgs = read_csv_flexible(args.pgs, usecols=lambda c: c in set(pgs_cols))

    if "ext_id" in pgs.columns:
        pgs["ext_id"] = norm_str_series(pgs["ext_id"])
    if "email" in pgs.columns:
        pgs["email"] = norm_email_series(pgs["email"])

    tap_key_cols = ["user_ext_id", "e_mail"]
    tap_a = read_csv_flexible(args.tap_a, usecols=lambda c: c in set(tap_key_cols))
    tap_b = read_csv_flexible(args.tap_b, usecols=lambda c: c in set(tap_key_cols))

    if "user_ext_id" in tap_a.columns:
        tap_a["user_ext_id"] = norm_str_series(tap_a["user_ext_id"])
    if "user_ext_id" in tap_b.columns:
        tap_b["user_ext_id"] = norm_str_series(tap_b["user_ext_id"])

    if "e_mail" in tap_a.columns:
        tap_a["e_mail"] = norm_email_series(tap_a["e_mail"])
    if "e_mail" in tap_b.columns:
        tap_b["e_mail"] = norm_email_series(tap_b["e_mail"])

    tap_ext = pd.concat(
        [
            tap_a["user_ext_id"] if "user_ext_id" in tap_a.columns else pd.Series([], dtype=str),
            tap_b["user_ext_id"] if "user_ext_id" in tap_b.columns else pd.Series([], dtype=str),
        ],
        ignore_index=True,
    ).dropna()
    tap_ext = tap_ext.drop_duplicates()

    tap_email = pd.concat(
        [
            tap_a["e_mail"] if "e_mail" in tap_a.columns else pd.Series([], dtype=str),
            tap_b["e_mail"] if "e_mail" in tap_b.columns else pd.Series([], dtype=str),
        ],
        ignore_index=True,
    ).dropna()
    tap_email = tap_email.drop_duplicates()

    summary_lines = []
    summary_lines.append(f"PGS rows (loaded cols only): {len(pgs)}")
    summary_lines.append(f"TAP distinct user_ext_id: {len(tap_ext)}")
    summary_lines.append(f"TAP distinct e_mail: {len(tap_email)}")

    if "ext_id" in pgs.columns and len(tap_ext) > 0:
        pgs_not_in_tap_ext = pgs[~pgs["ext_id"].isin(set(tap_ext))].copy()
        pgs_not_in_tap_ext.to_csv(out_dir / "pgs_not_in_tap_by_ext_id.csv", index=False)
        summary_lines.append(f"PGS not in TAP by ext_id: {len(pgs_not_in_tap_ext)}")

    if "email" in pgs.columns and len(tap_email) > 0:
        pgs_not_in_tap_email = pgs[~pgs["email"].isin(set(tap_email))].copy()
        pgs_not_in_tap_email.to_csv(out_dir / "pgs_not_in_tap_by_email.csv", index=False)
        summary_lines.append(f"PGS not in TAP by email: {len(pgs_not_in_tap_email)}")

    status_cols_found = [c for c in ["rg_cool_off_status", "is_rg_closed"] if c in pgs.columns]
    if status_cols_found:
        st = pgs[status_cols_found].copy()
        for c in status_cols_found:
            st[c] = norm_str_series(st[c]).str.lower()

        combo = st.fillna("null").value_counts().reset_index(name="players")
        combo.to_csv(out_dir / "pgs_status_counts_by_combination.csv", index=False)

        rows = []
        for c in status_cols_found:
            vc = st[c].fillna("null").value_counts(dropna=False)
            for k, v in vc.items():
                rows.append({"status_col": c, "value": k, "players": int(v)})
        pd.DataFrame(rows).to_csv(out_dir / "pgs_status_counts_by_flag.csv", index=False)

        summary_lines.append(f"Status cols used: {', '.join(status_cols_found)}")
    else:
        summary_lines.append("No status columns found in PGS for rg_cooloff/rg_closed counting.")

    (out_dir / "deliverables_step2_summary.txt").write_text("\n".join(summary_lines), encoding="utf-8")
    print(f"OK. Step2 outputs em: {out_dir.resolve()}")


if __name__ == "__main__":
    main()