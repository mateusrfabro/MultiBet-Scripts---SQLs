import argparse
import csv
import glob
import os
import shutil
from datetime import date
from typing import Dict, List, Optional

import pandas as pd
from google.cloud import bigquery


HEAVY_TAGS = {
    "BEHAV_RISK_PLAYER",
    "PLAYER_REENGAGED",
    "SLEEPER_LOW_PLAYER",
    "VIP_WHALE_PLAYER",
    "WINBACK_HI_VAL_PLAYER",
}

TAG_ORDER = [
    "PLAYER_NOT_VALID",
    "POTENCIAL_ABUSER",
    "NON_PROMO_PLAYER",
    "ENGAGED_AND_RG",
    "PLAYER_REENGAGED",
    "SLEEPER_LOW_PLAYER",
    "BEHAV_RISK_PLAYER",
    "BEHAV_SLOTGAMER",
    "VIP_WHALE_PLAYER",
    "WINBACK_HI_VAL_PLAYER",
]

MIN_PARTIAL = -35
MAX_PARTIAL = 116


def read_sql(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read().strip()


def render(sql_template: str, project: str, dataset: str, window_days: Optional[int] = None) -> str:
    sql = sql_template.replace("{{PROJECT}}", project).replace("{{DATASET}}", dataset)
    if window_days is not None:
        sql = sql.replace("{{WINDOW_DAYS}}", str(window_days))
    return sql


def run_query_to_csv(client: bigquery.Client, sql: str, out_csv: str) -> int:
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)

    job_config = bigquery.QueryJobConfig(use_query_cache=True)
    job = client.query(sql, job_config=job_config)

    rows = job.result(page_size=10000)
    headers = [field.name for field in rows.schema]

    count = 0
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(headers)
        for r in rows:
            w.writerow([r.get(h) for h in headers])
            count += 1

    return count


def progressive_windows(tag: str) -> Optional[List[int]]:
    if tag in HEAVY_TAGS:
        return [30, 60, 90]
    return None


def classify(score_norm: float) -> str:
    if score_norm > 85:
        return "Muito Bom"
    if score_norm >= 70:
        return "Bom"
    if score_norm >= 50:
        return "Mediano"
    if score_norm >= 30:
        return "Ruim"
    return "Muito Ruim"


def build_partial_output(out_dir: str, run_date: str) -> str:
    dim_path = os.path.join(out_dir, f"dim_users_base_{run_date}.csv")
    # read dimension table with explicit dtypes for the join keys
    dim = pd.read_csv(dim_path, dtype={"label_id": str, "user_id": str})

    tag_files = sorted(
        glob.glob(os.path.join(out_dir, f"risk_abusers_*_{run_date}.csv"))
    )
    tag_files = [
        fp
        for fp in tag_files
        if "mateus_partial" not in fp and "dim_users_base" not in fp
    ]

    parts: List[pd.DataFrame] = []
    for fp in tag_files:
        df = pd.read_csv(fp, dtype={"label_id": str, "user_id": str, "score": float})
        if df.empty:
            continue

        required = ["label_id", "user_id", "tag", "score", "snapshot_date"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(f"Arquivo {fp} sem colunas {missing}")

        # make sure score column is numeric
        df["score"] = pd.to_numeric(df["score"], errors="coerce").fillna(0)
        parts.append(df[required])

    out_path = os.path.join(out_dir, f"risk_abusers_mateus_partial_{run_date}.csv")

    if not parts:
        base = dim[["label_id", "user_id"]].copy()
        base["score_bruto_parcial"] = 0
        base["score_norm_parcial"] = 0.0
        base["nivel_parcial"] = "Muito Ruim"
        base["tags_acionadas"] = ""
        base.to_csv(out_path, index=False)
        return out_path

    long = pd.concat(parts, ignore_index=True)

    agg = (
        long.groupby(["label_id", "user_id", "snapshot_date"], dropna=False)
        .agg(
            score_bruto_parcial=("score", "sum"),
            tags_acionadas=("tag", lambda x: "|".join(sorted(set(x)))),
        )
        .reset_index()
    )

    agg["score_norm_parcial"] = (
        (agg["score_bruto_parcial"] - MIN_PARTIAL) / (MAX_PARTIAL - MIN_PARTIAL)
    ) * 100
    agg["score_norm_parcial"] = agg["score_norm_parcial"].clip(lower=0, upper=100)
    agg["nivel_parcial"] = agg["score_norm_parcial"].apply(classify)

    # merge only the columns from dim that aren\'t computed above to avoid duplicates
    dim_cols = [
        c
        for c in dim.columns
        if c not in {
            "snapshot_date",
            "score_bruto_parcial",
            "score_norm_parcial",
            "nivel_parcial",
            "tags_acionadas",
        }
    ]
    final = agg.merge(dim[dim_cols], on=["label_id", "user_id"], how="left")
    final.to_csv(out_path, index=False)

    return out_path


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", default="smartico-bq6")
    ap.add_argument("--dataset", default="dwh_ext_24105")
    ap.add_argument("--sql_dir", default="sql")
    ap.add_argument("--out_dir", default="outputs")
    ap.add_argument("--date", default=date.today().isoformat())
    ap.add_argument("--only", nargs="*", help="Opcional: rode só algumas tags")
    args = ap.parse_args()

    run_date = args.date
    out_dir = os.path.join(args.out_dir, run_date)
    os.makedirs(out_dir, exist_ok=True)

    client = bigquery.Client(project=args.project)

    dim_sql_path = os.path.join(args.sql_dir, "dim_users_base.sql")
    dim_sql_tpl = read_sql(dim_sql_path)
    if not dim_sql_tpl:
        raise SystemExit(f"Arquivo vazio: {dim_sql_path}")

    dim_sql = render(dim_sql_tpl, args.project, args.dataset)
    dim_out = os.path.join(out_dir, f"dim_users_base_{run_date}.csv")
    n = run_query_to_csv(client, dim_sql, dim_out)
    print(f"[OK] DIM_USERS_BASE -> {dim_out} ({n} linhas)")

    tag_files: Dict[str, str] = {
        "PLAYER_NOT_VALID": "tags/PLAYER_NOT_VALID.sql",
        "POTENCIAL_ABUSER": "tags/POTENCIAL_ABUSER.sql",
        "NON_PROMO_PLAYER": "tags/NON_PROMO_PLAYER.sql",
        "ENGAGED_AND_RG": "tags/ENGAGED_AND_RG.sql",
        "PLAYER_REENGAGED": "tags/PLAYER_REENGAGED.sql",
        "SLEEPER_LOW_PLAYER": "tags/SLEEPER_LOW_PLAYER.sql",
        "BEHAV_RISK_PLAYER": "tags/BEHAV_RISK_PLAYER.sql",
        "BEHAV_SLOTGAMER": "tags/BEHAV_SLOTGAMER.sql",
        "VIP_WHALE_PLAYER": "tags/VIP_WHALE_PLAYER.sql",
        "WINBACK_HI_VAL_PLAYER": "tags/WINBACK_HI_VAL_PLAYER.sql",
    }

    selected = set(args.only) if args.only else set(tag_files.keys())

    for tag in TAG_ORDER:
        if tag not in selected:
            continue

        sql_path = os.path.join(args.sql_dir, tag_files[tag])
        sql_tpl = read_sql(sql_path)
        if not sql_tpl:
            raise SystemExit(f"Arquivo vazio: {sql_path}")

        canonical = os.path.join(out_dir, f"risk_abusers_{tag}_{run_date}.csv")
        windows = progressive_windows(tag)

        if windows:
            if "{{WINDOW_DAYS}}" not in sql_tpl:
                raise SystemExit(f"Tag {tag} é pesada, mas o SQL não tem {{WINDOW_DAYS}}: {sql_path}")

            last_ok = None
            last_w = None
            for w in windows:
                try:
                    sql = render(sql_tpl, args.project, args.dataset, window_days=w)
                    out = os.path.join(out_dir, f"risk_abusers_{tag}_{run_date}_{w}d.csv")
                    n = run_query_to_csv(client, sql, out)
                    print(f"[OK] {tag} {w}d -> {out} ({n} linhas)")
                    last_ok = out
                    last_w = w
                except Exception as e:
                    print(f"[FAIL] {tag} {w}d -> {e}")
                    break

            if last_ok is None:
                raise SystemExit(f"Tag {tag} falhou em todas as janelas.")

            shutil.copyfile(last_ok, canonical)
            print(f"[DONE] {tag}: melhor janela = {last_w}d -> {canonical}")

        else:
            sql = render(sql_tpl, args.project, args.dataset)
            n = run_query_to_csv(client, sql, canonical)
            print(f"[OK] {tag} -> {canonical} ({n} linhas)")

    out_path = build_partial_output(out_dir, run_date)
    print(f"[OK] Parcial Mateus -> {out_path}")


if __name__ == "__main__":
    main()