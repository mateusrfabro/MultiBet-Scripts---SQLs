"""
export_smartico_risk.py
────────────────────────────────────────────────────────────────────
Fluxo:
    1. Consulta BigQuery: dim_users_base + todas as tags de risco
    2. Consulta Athena: users_status → filtra jogadores inativos
    3. Pivota resultados → 1 linha por jogador, 1 coluna por tag (score)
    4. Abre SSH tunnel → PostgreSQL
    5. Cria tabela risk_tags (se não existir)
    6. Trunca e insere dados

Uso:
    python export_smartico_risk.py
    python export_smartico_risk.py --window_days 60 --only FAST_CASHOUT PROMO_ONLY

Dependências:
    pip install google-cloud-bigquery pandas psycopg2-binary python-dotenv sshtunnel pyathena
────────────────────────────────────────────────────────────────────
"""

import argparse
import glob
import os
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from google.cloud import bigquery
from pyathena import connect as athena_connect
from sshtunnel import SSHTunnelForwarder

# ─────────────────────────────────────────────
# 1. Variáveis de ambiente
# ─────────────────────────────────────────────
_SCRIPT_DIR = Path(__file__).resolve().parent
load_dotenv(dotenv_path=_SCRIPT_DIR.parent.parent.parent / ".env")

# BigQuery — credenciais via service account JSON
BQ_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if BQ_CREDENTIALS:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = BQ_CREDENTIALS

# Postgres
PG_HOST     = os.getenv("PG_HOST")
PG_PORT     = int(os.getenv("PG_PORT", 5432))
PG_DBNAME   = os.getenv("PG_DBNAME")
PG_USER     = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_SCHEMA   = os.getenv("PG_SCHEMA", "multibet")

# SSH / Bastion
SSH_HOST     = os.getenv("SSH_HOST")
SSH_PORT     = int(os.getenv("SSH_PORT", 22))
SSH_USER     = os.getenv("SSH_USER")
SSH_KEY_PATH = os.getenv("SSH_KEY_PATH")

# Athena
ATHENA_S3_OUTPUT  = os.getenv("ATHENA_S3_OUTPUT")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY    = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION        = os.getenv("AWS_REGION", "sa-east-1")

# ─────────────────────────────────────────────
# 2. Constantes de tags e scoring
# ─────────────────────────────────────────────
HEAVY_TAGS = {
    "BEHAV_RISK_PLAYER",
    "PLAYER_REENGAGED",
    "SLEEPER_LOW_PLAYER",
    "VIP_WHALE_PLAYER",
    "WINBACK_HI_VAL_PLAYER",
}

# -----------------------------------------------------------------------
# Edite esta lista para controlar a ORDEM de execução das tags.
# Tags encontradas no disco mas não listadas aqui rodam ao final,
# em ordem alfabética.
# -----------------------------------------------------------------------
TAG_ORDER = [
    'REGULAR_DEPOSITOR',
    'PROMO_ONLY',
    'ZERO_RISK_PLAYER',
    'FAST_CASHOUT',
    'SUSTAINED_PLAYER',
    'NON_BONUS_DEPOSITOR',
    'PROMO_CHAINER',
    'CASHOUT_AND_RUN',
    'REINVEST_PLAYER',
    'NON_PROMO_PLAYER',
    'ENGAGED_AND_RG',
    'BEHAV_RISK_PLAYER',
    'POTENCIAL_ABUSER',
    'PLAYER_NOT_VALID',
    'PLAYER_REENGAGED',
    'SLEEPER_LOW_PLAYER',
    'VIP_WHALE_PLAYER',
    'WINBACK_HI_VAL_PLAYER',
    'BEHAV_SLOTGAMER'
]

# Todas as colunas de tag na tabela PostgreSQL (lowercase)
ALL_TAG_COLUMNS = [
    'regular_depositor',
    'promo_only',
    'zero_risk_player',
    'fast_cashout',
    'sustained_player',
    'non_bonus_depositor',
    'promo_chainer',
    'cashout_and_run',
    'reinvest_player',
    'non_promo_player',
    'engaged_player',
    'rg_alert_player',
    'behav_risk_player',
    'potencial_abuser',
    'player_not_valid',
    'player_reengaged',
    'sleeper_low_player',
    'vip_whale_player',
    'winback_hi_val_player',
    'behav_slotgamer',
]

PG_TABLE = "risk_tags"

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {{schema}}.{PG_TABLE} (
    label_id              VARCHAR(50),
    user_id               VARCHAR(50),
    user_ext_id           VARCHAR(100),
    snapshot_date         DATE,
    regular_depositor     INTEGER DEFAULT 0,
    promo_only            INTEGER DEFAULT 0,
    zero_risk_player      INTEGER DEFAULT 0,
    fast_cashout          INTEGER DEFAULT 0,
    sustained_player      INTEGER DEFAULT 0,
    non_bonus_depositor   INTEGER DEFAULT 0,
    promo_chainer         INTEGER DEFAULT 0,
    cashout_and_run       INTEGER DEFAULT 0,
    reinvest_player       INTEGER DEFAULT 0,
    non_promo_player      INTEGER DEFAULT 0,
    engaged_player        INTEGER DEFAULT 0,
    rg_alert_player       INTEGER DEFAULT 0,
    behav_risk_player     INTEGER DEFAULT 0,
    potencial_abuser      INTEGER DEFAULT 0,
    player_not_valid      INTEGER DEFAULT 0,
    player_reengaged      INTEGER DEFAULT 0,
    sleeper_low_player    INTEGER DEFAULT 0,
    vip_whale_player      INTEGER DEFAULT 0,
    winback_hi_val_player INTEGER DEFAULT 0,
    behav_slotgamer       INTEGER DEFAULT 0,
    computed_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (label_id, user_id, snapshot_date)
);
"""


# ─────────────────────────────────────────────
# 3. BigQuery helpers
# ─────────────────────────────────────────────
def read_sql(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read().strip()


def render(sql_template: str, project: str, dataset: str, window_days: int = 90) -> str:
    return (
        sql_template
        .replace("{{PROJECT}}", project)
        .replace("{{DATASET}}", dataset)
        .replace("{{WINDOW_DAYS}}", str(window_days))
    )


def run_query_to_df(client: bigquery.Client, sql: str) -> pd.DataFrame:
    job_config = bigquery.QueryJobConfig(use_query_cache=True)
    job = client.query(sql, job_config=job_config)
    rows = job.result(page_size=10000)
    headers = [field.name for field in rows.schema]
    data = [[r.get(h) for h in headers] for r in rows]
    return pd.DataFrame(data, columns=headers)


def discover_tags(sql_dir: str) -> Dict[str, str]:
    """Auto-descobre todos os SQLs de tags em sql/tags/*.sql."""
    pattern = os.path.join(sql_dir, "tags", "*.sql")
    return {
        os.path.splitext(os.path.basename(fp))[0]: fp
        for fp in glob.glob(pattern)
    }


def ordered_tags(all_tags: Dict[str, str]) -> List[str]:
    """Retorna tags na ordem de TAG_ORDER; as demais vêm no final em ordem alfabética."""
    ordered = [t for t in TAG_ORDER if t in all_tags]
    remaining = sorted(t for t in all_tags if t not in set(TAG_ORDER))
    return ordered + remaining


def progressive_windows(tag: str) -> Optional[List[int]]:
    if tag in HEAVY_TAGS:
        return [30, 60, 90]
    return None


def run_athena_query(sql: str) -> pd.DataFrame:
    """Executa query no Athena e retorna DataFrame."""
    conn = athena_connect(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_KEY,
        s3_staging_dir=ATHENA_S3_OUTPUT,
        region_name=AWS_REGION,
    )
    return pd.read_sql(sql, conn)


# ─────────────────────────────────────────────
# 4. PostgreSQL via SSH Tunnel
# ─────────────────────────────────────────────
def save_to_postgres(df: pd.DataFrame) -> None:
    if df.empty:
        print("[Postgres] DataFrame vazio, nada a inserir.")
        return

    all_cols = list(df.columns)
    cols_escaped = ", ".join([f'"{c}"' for c in all_cols])
    placeholders = ", ".join(["%s"] * len(all_cols))

    insert_sql = f"""
        INSERT INTO {PG_SCHEMA}.{PG_TABLE} ({cols_escaped})
        VALUES ({placeholders})
    """

    rows = [tuple(row) for row in df.itertuples(index=False, name=None)]

    print(f"[SSH] Abrindo tunel para {SSH_HOST}:{SSH_PORT} -> {PG_HOST}:{PG_PORT}...")
    with SSHTunnelForwarder(
        (SSH_HOST, SSH_PORT),
        ssh_username=SSH_USER,
        ssh_pkey=SSH_KEY_PATH,
        remote_bind_address=(PG_HOST, PG_PORT),
    ) as tunnel:
        local_port = tunnel.local_bind_port
        print(f"[SSH] Tunel ativo na porta local {local_port}.")

        print("[Postgres] Conectando via tunel SSH...")
        conn = psycopg2.connect(
            host="127.0.0.1",
            port=local_port,
            dbname=PG_DBNAME,
            user=PG_USER,
            password=PG_PASSWORD,
        )
        try:
            with conn.cursor() as cur:
                # cria tabela se não existir
                cur.execute(CREATE_TABLE_SQL.format(schema=PG_SCHEMA))

                # trunca dados existentes
                print(f"[Postgres] Truncando {PG_SCHEMA}.{PG_TABLE}...")
                cur.execute(f"TRUNCATE TABLE {PG_SCHEMA}.{PG_TABLE};")

                # insere
                print(f"[Postgres] Inserindo {len(rows)} linhas em {PG_SCHEMA}.{PG_TABLE}...")
                psycopg2.extras.execute_batch(cur, insert_sql, rows, page_size=500)

            conn.commit()
            print(f"[Postgres] {len(rows)} linhas processadas com sucesso.")
        except Exception as e:
            conn.rollback()
            print(f"[ERRO] Falha no insert: {e}")
            raise
        finally:
            conn.close()


# ─────────────────────────────────────────────
# 6. Main
# ─────────────────────────────────────────────
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", default="smartico-bq6")
    ap.add_argument("--dataset", default="dwh_ext_24105")
    ap.add_argument("--sql_dir", default=str(_SCRIPT_DIR.parent / "sql"))
    ap.add_argument("--date", default=date.today().isoformat())
    ap.add_argument(
        "--window_days",
        type=int,
        default=90,
        help="Janela padrão em dias passada a todos os SQLs (padrão: 90)",
    )
    ap.add_argument("--only", nargs="*", help="Opcional: rode só algumas tags")
    args = ap.parse_args()

    run_date = args.date
    client = bigquery.Client(project=args.project)

    # ── ETAPA 1: dim_users_base do BigQuery ──────────────────────
    print("[BQ] Carregando dim_users_base...")
    dim_sql_tpl = read_sql(os.path.join(args.sql_dir, "dim_users_base.sql"))
    if not dim_sql_tpl:
        raise SystemExit("Arquivo vazio: dim_users_base.sql")

    dim_sql = render(dim_sql_tpl, args.project, args.dataset, window_days=args.window_days)
    dim = run_query_to_df(client, dim_sql)
    dim["label_id"] = dim["label_id"].astype(str)
    dim["user_id"] = dim["user_id"].astype(str)
    dim["user_ext_id"] = dim["user_ext_id"].astype(str)
    print(f"[BQ] dim_users_base -> {len(dim)} linhas")

    # ── ETAPA 1.5: filtra jogadores inativos via Athena ──────────
    print("[Athena] Carregando users_status (jogadores ativos)...")
    status_sql = read_sql(os.path.join(args.sql_dir, "users_status.sql"))
    active_users = run_athena_query(status_sql)
    active_users["user_ext_id"] = active_users["user_ext_id"].astype(str)
    active_ext_ids = set(active_users["user_ext_id"])

    antes = len(dim)
    dim = dim[dim["user_ext_id"].isin(active_ext_ids)]
    print(f"[Athena] {antes} -> {len(dim)} jogadores (removidos {antes - len(dim)} inativos)")

    # ── ETAPA 2: executa todas as tags no BigQuery ───────────────
    all_tags = discover_tags(args.sql_dir)
    selected = set(args.only) if args.only else set(all_tags.keys())
    tags_to_run = [t for t in ordered_tags(all_tags) if t in selected]

    scoring_parts: List[pd.DataFrame] = []

    for tag in tags_to_run:
        sql_path = all_tags[tag]
        sql_tpl = read_sql(sql_path)
        if not sql_tpl:
            raise SystemExit(f"Arquivo vazio: {sql_path}")

        windows = progressive_windows(tag)

        if windows:
            if "{{WINDOW_DAYS}}" not in sql_tpl:
                raise SystemExit(
                    f"Tag {tag} é pesada, mas o SQL não tem {{WINDOW_DAYS}}: {sql_path}"
                )
            last_df = None
            last_w = None
            for w in windows:
                try:
                    sql = render(sql_tpl, args.project, args.dataset, window_days=w)
                    df = run_query_to_df(client, sql)
                    print(f"[BQ] {tag} {w}d -> {len(df)} linhas")
                    last_df = df
                    last_w = w
                except Exception as e:
                    print(f"[FAIL] {tag} {w}d -> {e}")
                    break

            if last_df is None:
                raise SystemExit(f"Tag {tag} falhou em todas as janelas.")
            if not last_df.empty:
                scoring_parts.append(last_df)
            print(f"[DONE] {tag}: melhor janela = {last_w}d")

        else:
            sql = render(sql_tpl, args.project, args.dataset, window_days=args.window_days)
            df = run_query_to_df(client, sql)
            print(f"[BQ] {tag} -> {len(df)} linhas")
            if not df.empty:
                scoring_parts.append(df)

    # ── ETAPA 3: pivota tags → 1 linha por jogador ──────────────
    print("[PIVOT] Montando tabela pivotada...")

    if scoring_parts:
        long = pd.concat(scoring_parts, ignore_index=True)

        # filtra apenas linhas com formato de scoring
        required = ["label_id", "user_id", "tag", "score"]
        has_all = all(c in long.columns for c in required)
        if has_all:
            long = long[required].dropna(subset=["tag", "score"])
            long["label_id"] = long["label_id"].astype(str)
            long["user_id"] = long["user_id"].astype(str)
            long["tag"] = long["tag"].str.lower()
            long["score"] = pd.to_numeric(long["score"], errors="coerce").fillna(0).astype(int)

            pivoted = long.pivot_table(
                index=["label_id", "user_id"],
                columns="tag",
                values="score",
                aggfunc="first",
            ).reset_index()
            pivoted.columns.name = None
        else:
            pivoted = pd.DataFrame(columns=["label_id", "user_id"])
    else:
        pivoted = pd.DataFrame(columns=["label_id", "user_id"])

    # merge dim (todos os jogadores) com tags pivotadas
    final = dim[["label_id", "user_id", "user_ext_id"]].merge(
        pivoted, on=["label_id", "user_id"], how="left"
    )

    # garante que todas as colunas de tag existem e estão com 0 onde não acionadas
    for col in ALL_TAG_COLUMNS:
        if col not in final.columns:
            final[col] = 0
        else:
            final[col] = final[col].fillna(0).astype(int)

    final["snapshot_date"] = run_date

    # ordena colunas: label_id, user_id, snapshot_date, tags...
    final = final[["label_id", "user_id", "user_ext_id", "snapshot_date"] + ALL_TAG_COLUMNS]

    print(f"[PIVOT] {len(final)} jogadores, {len(ALL_TAG_COLUMNS)} tags")

    # ── ETAPA 4: SSH Tunnel → PostgreSQL ─────────────────────────
    save_to_postgres(final)

    print("\nConcluido!")


if __name__ == "__main__":
    main()
