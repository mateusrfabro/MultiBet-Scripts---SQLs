import argparse
import glob
from pathlib import Path
import pandas as pd


PREFERRED_KEYS = [
    "ext_id", "external_id", "c_external_id", "user_external_id", "userid", "user_id", "player_id",
    "ecr_id", "c_ecr_id",
    "email", "e-mail", "mail",
]

STATUS_COL_CANDIDATES = ["rg_closed", "rg_cooloff", "rg_coolf_off"]


def read_csv_flexible(path: str) -> pd.DataFrame:
    seps_to_try = [",", ";", "\t", "|"]
    encs_to_try = ["utf-8-sig", "utf-8", "latin1"]
    last_err = None

    for enc in encs_to_try:
        for sep in seps_to_try:
            try:
                df = pd.read_csv(path, sep=sep, dtype=str, encoding=enc, low_memory=False)
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


def pick_key(df_a: pd.DataFrame, df_b: pd.DataFrame, forced_key: str | None) -> str:
    cols_a = {c.lower(): c for c in df_a.columns}
    cols_b = {c.lower(): c for c in df_b.columns}

    if forced_key:
        lk = forced_key.lower()
        if lk in cols_a and lk in cols_b:
            return cols_a[lk]
        raise ValueError(
            f"Key '{forced_key}' não existe em ambos os TAP.\n"
            f"Colunas TAP A (amostra): {list(df_a.columns)[:60]}\n"
            f"Colunas TAP B (amostra): {list(df_b.columns)[:60]}"
        )

    for k in PREFERRED_KEYS:
        lk = k.lower()
        if lk in cols_a and lk in cols_b:
            return cols_a[lk]

    common = list(set(cols_a.keys()) & set(cols_b.keys()))
    if not common:
        raise ValueError(
            "Não há nenhuma coluna com o mesmo nome nos 2 arquivos TAP.\n"
            "Informe --key (ex: ext_id, user_id, email)."
        )

    best = None
    best_score = -1.0
    for lk in common:
        ca = cols_a[lk]
        cb = cols_b[lk]
        sa = norm_str_series(df_a[ca])
        sb = norm_str_series(df_b[cb])

        ua = sa.nunique(dropna=True) / max(len(sa), 1)
        ub = sb.nunique(dropna=True) / max(len(sb), 1)
        na = 1 - sa.isna().mean()
        nb = 1 - sb.isna().mean()

        score = ((ua + ub) / 2) * ((na + nb) / 2)
        if score > best_score:
            best_score = score
            best = ca

    return best


def ensure_key_normalized(df: pd.DataFrame, key: str) -> pd.DataFrame:
    df = df.copy()
    if key.lower() in ["email", "e-mail", "mail"]:
        df[key] = norm_email_series(df[key])
    else:
        df[key] = norm_str_series(df[key])
    return df


def jaccard_cols(df_a: pd.DataFrame, df_b: pd.DataFrame) -> float:
    a = set([c.lower() for c in df_a.columns])
    b = set([c.lower() for c in df_b.columns])
    if not a and not b:
        return 1.0
    return len(a & b) / len(a | b)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pgs_glob", required=True)
    ap.add_argument("--tap_a", required=True)
    ap.add_argument("--tap_b", required=True)
    ap.add_argument("--out_dir", required=True)
    ap.add_argument("--key", required=False)
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    pgs_files = sorted(glob.glob(args.pgs_glob))
    if not pgs_files:
        raise ValueError(f"Nenhum arquivo PGS encontrado com glob: {args.pgs_glob}")

    pgs_parts = [read_csv_flexible(f) for f in pgs_files]
    pgs = pd.concat(pgs_parts, ignore_index=True)
    pgs.to_csv(out_dir / "pgs_full.csv", index=False)

    tap_a = read_csv_flexible(args.tap_a)
    tap_b = read_csv_flexible(args.tap_b)

    key = pick_key(tap_a, tap_b, args.key)
    tap_a = ensure_key_normalized(tap_a, key)
    tap_b = ensure_key_normalized(tap_b, key)

    set_a = set(tap_a[key].dropna().unique())
    set_b = set(tap_b[key].dropna().unique())
    inter = set_a & set_b

    dup_a = tap_a[tap_a[key].duplicated(keep=False) & tap_a[key].notna()].copy()
    dup_b = tap_b[tap_b[key].duplicated(keep=False) & tap_b[key].notna()].copy()
    if len(dup_a) > 0:
        dup_a.to_csv(out_dir / "tap_a_duplicates_by_key.csv", index=False)
    if len(dup_b) > 0:
        dup_b.to_csv(out_dir / "tap_b_duplicates_by_key.csv", index=False)

    if len(inter) > 0:
        tap_a[tap_a[key].isin(inter)].to_csv(out_dir / "tap_overlap_in_a.csv", index=False)
        tap_b[tap_b[key].isin(inter)].to_csv(out_dir / "tap_overlap_in_b.csv", index=False)

    col_sim = jaccard_cols(tap_a, tap_b)
    inter_ratio = len(inter) / max(min(len(set_a), len(set_b)), 1)

    tap_union = pd.concat([tap_a, tap_b], ignore_index=True)
    tap_union.to_csv(out_dir / "tap_unified_union.csv", index=False)

    tap_join = tap_a.merge(tap_b, on=key, how="outer", suffixes=("_a", "_b"), indicator=True)
    tap_join.to_csv(out_dir / "tap_unified_join_full_outer.csv", index=False)

    if len(inter) > 0:
        tap_join_left = tap_a.merge(tap_b, on=key, how="left", suffixes=("", "_b"))
        tap_join_left.to_csv(out_dir / "tap_unified_join_left.csv", index=False)

    pgs_cols_lower = {c.lower(): c for c in pgs.columns}
    tap_cols_lower = {c.lower(): c for c in tap_union.columns}

    compare_key = None
    tap_compare_key = None
    for k in ["ext_id", "external_id", "c_external_id", "ecr_id", "c_ecr_id", "email"]:
        lk = k.lower()
        if lk in pgs_cols_lower and lk in tap_cols_lower:
            compare_key = pgs_cols_lower[lk]
            tap_compare_key = tap_cols_lower[lk]
            break

    if compare_key is not None and tap_compare_key is not None:
        pgs_cmp = pgs.copy()
        tap_cmp = tap_union.copy()

        if compare_key.lower() == "email":
            pgs_cmp[compare_key] = norm_email_series(pgs_cmp[compare_key])
            tap_cmp[tap_compare_key] = norm_email_series(tap_cmp[tap_compare_key])
        else:
            pgs_cmp[compare_key] = norm_str_series(pgs_cmp[compare_key])
            tap_cmp[tap_compare_key] = norm_str_series(tap_cmp[tap_compare_key])

        tap_keys = set(tap_cmp[tap_compare_key].dropna().unique())
        pgs_not_in_tap = pgs_cmp[~pgs_cmp[compare_key].isin(tap_keys)].copy()
        pgs_not_in_tap.to_csv(out_dir / f"pgs_not_in_tap_by_{compare_key}.csv", index=False)
    else:
        (out_dir / "WARNING_no_common_key_found.txt").write_text(
            "Não encontrei chave comum entre PGS e TAP automaticamente.\n"
            "Abra pgs_full.csv e tap_unified_union.csv e escolha uma coluna comum (ext_id/ecr_id/email).\n",
            encoding="utf-8",
        )

    pgs_cols_l = {c.lower(): c for c in pgs.columns}
    status_cols = [pgs_cols_l[c.lower()] for c in STATUS_COL_CANDIDATES if c.lower() in pgs_cols_l]

    if status_cols:
        st = pgs[status_cols].copy()
        for c in status_cols:
            st[c] = norm_str_series(st[c]).str.lower()

        combo = st.fillna("null").value_counts().reset_index(name="players")
        combo.to_csv(out_dir / "pgs_status_counts_by_combination.csv", index=False)

        rows = []
        for c in status_cols:
            vc = st[c].fillna("null").value_counts(dropna=False)
            for k, v in vc.items():
                rows.append({"status_col": c, "value": k, "players": int(v)})
        pd.DataFrame(rows).to_csv(out_dir / "pgs_status_counts_by_flag.csv", index=False)

    report_lines = [
        f"PGS files: {len(pgs_files)} | PGS rows: {len(pgs)}",
        f"TAP A rows: {len(tap_a)} | TAP B rows: {len(tap_b)}",
        f"TAP key used (A/B): {key}",
        f"TAP key overlap (A∩B): {len(inter)}",
        f"TAP column similarity (Jaccard): {col_sim:.3f}",
        f"TAP overlap ratio vs min(distinct keys): {inter_ratio:.3f}",
    ]
    if len(dup_a) > 0:
        report_lines.append(f"TAP A duplicates by key rows: {len(dup_a)}")
    if len(dup_b) > 0:
        report_lines.append(f"TAP B duplicates by key rows: {len(dup_b)}")

    (out_dir / "report.txt").write_text("\n".join(report_lines), encoding="utf-8")
    print(f"OK. Outputs em: {out_dir.resolve()}")


if __name__ == "__main__":
    main()
