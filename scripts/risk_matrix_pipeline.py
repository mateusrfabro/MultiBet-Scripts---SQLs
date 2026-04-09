"""
risk_matrix_pipeline.py
================================================================
Pipeline da Matriz de Risco MultiBet — 100% Athena

Fluxo:
    1. Le todos os SQLs de tags em scripts/sql/risk_matrix/
    2. Executa cada tag no Athena (Trino/Presto)
    3. Pivota resultados: 1 linha por jogador, 1 coluna por tag
    4. Calcula score_bruto (soma), score_norm (0-100), tier
    5. Busca user_ext_id no ps_bi.dim_user
    6. Persiste no Super Nova DB (multibet.risk_tags)
    7. Exporta CSV + legenda

Uso:
    python scripts/risk_matrix_pipeline.py
    python scripts/risk_matrix_pipeline.py --window_days 60
    python scripts/risk_matrix_pipeline.py --only FAST_CASHOUT PROMO_ONLY
    python scripts/risk_matrix_pipeline.py --dry-run  # apenas CSV, sem gravar no DB

Dependencias:
    pip install pandas psycopg2-binary python-dotenv pyathena sshtunnel
================================================================
"""

import argparse
import logging
import os
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

# ── Setup paths ──────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
SQL_DIR = SCRIPT_DIR / "sql" / "risk_matrix"

sys.path.insert(0, str(PROJECT_DIR))

from db.athena import query_athena
from db.supernova import get_supernova_connection

# ── Logging ──────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("risk_matrix")

# ── Constantes ───────────────────────────────────────────────────

# Ordem de execucao das tags (alinhada com a Matriz de Risco Notion)
TAG_ORDER = [
    "REGULAR_DEPOSITOR",
    "PROMO_ONLY",
    "ZERO_RISK_PLAYER",
    "FAST_CASHOUT",
    "SUSTAINED_PLAYER",
    "NON_BONUS_DEPOSITOR",
    "PROMO_CHAINER",
    "CASHOUT_AND_RUN",
    "REINVEST_PLAYER",
    "NON_PROMO_PLAYER",
    "ENGAGED_PLAYER",
    "RG_ALERT_PLAYER",
    "BEHAV_RISK_PLAYER",
    "POTENCIAL_ABUSER",
    "PLAYER_REENGAGED",
    "SLEEPER_LOW_PLAYER",
    "VIP_WHALE_PLAYER",
    "WINBACK_HI_VAL_PLAYER",
    "BEHAV_SLOTGAMER",
    "MULTI_GAME_PLAYER",
    "ROLLBACK_PLAYER",
]

# Mapeamento tag -> coluna no PostgreSQL (lowercase, sem espacos)
TAG_TO_COLUMN = {
    "REGULAR_DEPOSITOR": "regular_depositor",
    "PROMO_ONLY": "promo_only",
    "ZERO_RISK_PLAYER": "zero_risk_player",
    "FAST_CASHOUT": "fast_cashout",
    "SUSTAINED_PLAYER": "sustained_player",
    "NON_BONUS_DEPOSITOR": "non_bonus_depositor",
    "PROMO_CHAINER": "promo_chainer",
    "CASHOUT_AND_RUN": "cashout_and_run",
    "REINVEST_PLAYER": "reinvest_player",
    "NON_PROMO_PLAYER": "non_promo_player",
    "ENGAGED_PLAYER": "engaged_player",
    "RG_ALERT_PLAYER": "rg_alert_player",
    "BEHAV_RISK_PLAYER": "behav_risk_player",
    "POTENCIAL_ABUSER": "potencial_abuser",
    "PLAYER_REENGAGED": "player_reengaged",
    "SLEEPER_LOW_PLAYER": "sleeper_low_player",
    "VIP_WHALE_PLAYER": "vip_whale_player",
    "WINBACK_HI_VAL_PLAYER": "winback_hi_val_player",
    "BEHAV_SLOTGAMER": "behav_slotgamer",
    "MULTI_GAME_PLAYER": "multi_game_player",
    "ROLLBACK_PLAYER": "rollback_player",
}

ALL_TAG_COLUMNS = list(TAG_TO_COLUMN.values())

# Scores da Matriz (usados na normalizacao)
TAG_SCORES = {
    "REGULAR_DEPOSITOR": 10,
    "PROMO_ONLY": -15,
    "ZERO_RISK_PLAYER": 0,
    "FAST_CASHOUT": -25,
    "SUSTAINED_PLAYER": 15,
    "NON_BONUS_DEPOSITOR": 10,
    "PROMO_CHAINER": -10,
    "CASHOUT_AND_RUN": -25,
    "REINVEST_PLAYER": 15,
    "NON_PROMO_PLAYER": 10,
    "ENGAGED_PLAYER": 10,
    "RG_ALERT_PLAYER": 1,
    "BEHAV_RISK_PLAYER": -10,
    "POTENCIAL_ABUSER": -5,
    "PLAYER_REENGAGED": 30,
    "SLEEPER_LOW_PLAYER": 5,
    "VIP_WHALE_PLAYER": 30,
    "WINBACK_HI_VAL_PLAYER": 25,
    "BEHAV_SLOTGAMER": 5,
    "MULTI_GAME_PLAYER": -10,
    "ROLLBACK_PLAYER": -15,
}

# Normalizacao: recalibrada com percentis reais da base (09/04/2026)
# score_norm = (score_bruto - P05) / RANGE * 100, limitado entre 0 e 100
# P05=-35 (arredondado), P95=+50 (arredondado), range=85
# Calibracao empirica: mediana bruto=6 -> norm=48 (Mediano), bruto=10 -> norm=53 (Bom)
NORM_P05 = -35
NORM_P95 = 50
NORM_RANGE = NORM_P95 - NORM_P05  # 85

# Tiers
TIER_MAP = [
    (75, "Muito Bom"),
    (50, "Bom"),
    (25, "Mediano"),
    (10, "Ruim"),
    (0, "Muito Ruim"),
]

PG_SCHEMA = "multibet"
PG_TABLE = "risk_tags"


# ── DDL PostgreSQL ───────────────────────────────────────────────

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.{PG_TABLE} (
    label_id              VARCHAR(50),
    user_id               VARCHAR(50),
    user_ext_id           VARCHAR(100),
    snapshot_date         DATE,
    {chr(10).join(f'    {col:30s} INTEGER DEFAULT 0,' for col in ALL_TAG_COLUMNS)}
    score_bruto           INTEGER DEFAULT 0,
    score_norm            NUMERIC(5,1) DEFAULT 0,
    tier                  VARCHAR(20) DEFAULT 'Mediano',
    computed_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (label_id, user_id, snapshot_date)
);
"""


# ── Helpers ──────────────────────────────────────────────────────

def read_sql(path: Path) -> str:
    """Le arquivo SQL e retorna conteudo."""
    return path.read_text(encoding="utf-8").strip()


def discover_tags() -> Dict[str, Path]:
    """Auto-descobre SQLs em scripts/sql/risk_matrix/."""
    tags = {}
    for fp in SQL_DIR.glob("*.sql"):
        tag_name = fp.stem  # nome do arquivo sem extensao
        tags[tag_name] = fp
    return tags


def ordered_tags(available: Dict[str, Path], only: Optional[List[str]] = None) -> List[str]:
    """Retorna tags na ordem de TAG_ORDER, filtrando por --only se fornecido."""
    selected = set(only) if only else set(available.keys())
    ordered = [t for t in TAG_ORDER if t in available and t in selected]
    remaining = sorted(t for t in available if t not in set(TAG_ORDER) and t in selected)
    return ordered + remaining


def classify_tier(score_norm: float) -> str:
    """Classifica tier baseado no score normalizado."""
    for threshold, label in TIER_MAP:
        if score_norm > threshold:
            return label
    return "Muito Ruim"


def normalize_score(score_bruto: int) -> float:
    """Normaliza score bruto para 0-100 usando formula da Matriz."""
    raw = (score_bruto - NORM_P05) / NORM_RANGE * 100
    return max(0.0, min(100.0, round(raw, 1)))


# ── Etapa 1: Base de usuarios via Athena ─────────────────────────

def load_user_base() -> pd.DataFrame:
    """Carrega base de usuarios com atividade financeira (deposito ou aposta)
    nos ultimos 90 dias. Exclui test users e quem nunca movimentou dinheiro."""
    sql = """
    SELECT DISTINCT
        CAST(u.c_partner_id AS VARCHAR) AS label_id,
        CAST(u.c_ecr_id AS VARCHAR)     AS user_id
    FROM ecr_ec2.tbl_ecr u
    JOIN ecr_ec2.tbl_ecr_flags f ON u.c_ecr_id = f.c_ecr_id
    WHERE f.c_test_user = false
      AND u.c_partner_id IS NOT NULL
      AND (
        EXISTS (
          SELECT 1 FROM cashier_ec2.tbl_cashier_deposit d
          WHERE d.c_ecr_id = u.c_ecr_id
            AND d.c_created_time >= CURRENT_TIMESTAMP - INTERVAL '90' DAY
            AND d.c_txn_status = 'txn_confirmed_success'
        )
        OR EXISTS (
          SELECT 1 FROM fund_ec2.tbl_real_fund_txn t
          WHERE t.c_ecr_id = u.c_ecr_id
            AND t.c_start_time >= CURRENT_TIMESTAMP - INTERVAL '90' DAY
            AND t.c_txn_type IN (27, 28, 41, 43, 59, 127)
            AND t.c_txn_status = 'SUCCESS'
        )
      )
    """
    log.info("Carregando base de usuarios com atividade financeira (90d)...")
    df = query_athena(sql, database="ecr_ec2")
    df["label_id"] = df["label_id"].astype(str)
    df["user_id"] = df["user_id"].astype(str)
    log.info(f"Base de usuarios (com atividade): {len(df)} jogadores")
    return df


def load_user_ext_ids() -> pd.DataFrame:
    """Busca user_ext_id no ps_bi.dim_user para cross-referencia."""
    sql = """
    SELECT
        CAST(ecr_id AS VARCHAR)      AS user_id,
        CAST(external_id AS VARCHAR) AS user_ext_id
    FROM ps_bi.dim_user
    WHERE ecr_id IS NOT NULL
      AND external_id IS NOT NULL
    """
    try:
        log.info("Buscando user_ext_id (ps_bi.dim_user)...")
        df = query_athena(sql, database="ps_bi")
        df["user_id"] = df["user_id"].astype(str)
        df["user_ext_id"] = df["user_ext_id"].astype(str)
        log.info(f"user_ext_id mapeados: {len(df)}")
        return df
    except Exception as e:
        log.warning(f"Nao foi possivel carregar user_ext_id: {e}")
        return pd.DataFrame(columns=["user_id", "user_ext_id"])


# ── Etapa 2: Executar tags ───────────────────────────────────────

def run_tag(tag: str, sql_path: Path) -> pd.DataFrame:
    """Executa SQL de uma tag no Athena e retorna DataFrame."""
    sql = read_sql(sql_path)
    if not sql:
        log.error(f"SQL vazio: {sql_path}")
        return pd.DataFrame()

    try:
        # Identifica o database principal da query
        # Maioria usa ecr_ec2 como base, mas executa cross-db
        df = query_athena(sql, database="ecr_ec2")
        log.info(f"  {tag}: {len(df)} jogadores flagados")
        return df
    except Exception as e:
        log.error(f"  {tag}: FALHOU — {e}")
        return pd.DataFrame()


# ── Etapa 3: Pivot e scoring ─────────────────────────────────────

def pivot_results(scoring_parts: List[pd.DataFrame]) -> pd.DataFrame:
    """Pivota resultados de todas as tags em 1 linha por jogador."""
    if not scoring_parts:
        return pd.DataFrame(columns=["label_id", "user_id"])

    long = pd.concat(scoring_parts, ignore_index=True)

    required = ["label_id", "user_id", "tag", "score"]
    if not all(c in long.columns for c in required):
        log.error(f"Colunas faltando no resultado. Encontradas: {list(long.columns)}")
        return pd.DataFrame(columns=["label_id", "user_id"])

    long = long[required].dropna(subset=["tag", "score"])
    long["label_id"] = long["label_id"].astype(str)
    long["user_id"] = long["user_id"].astype(str)
    long["score"] = pd.to_numeric(long["score"], errors="coerce").fillna(0).astype(int)

    # Mapeia tag name -> column name para pivot
    long["col_name"] = long["tag"].map(TAG_TO_COLUMN)
    long = long.dropna(subset=["col_name"])

    pivoted = long.pivot_table(
        index=["label_id", "user_id"],
        columns="col_name",
        values="score",
        aggfunc="first",
    ).reset_index()
    pivoted.columns.name = None

    return pivoted


def compute_scores(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula score_bruto, score_norm e tier.
    Quem nao tem nenhuma tag ativa = 'SEM SCORE'."""
    # Garante que todas as colunas de tag existem
    for col in ALL_TAG_COLUMNS:
        if col not in df.columns:
            df[col] = 0
        else:
            df[col] = df[col].fillna(0).astype(int)

    # Conta quantas tags o jogador tem (qualquer valor != 0)
    df["tags_ativas"] = (df[ALL_TAG_COLUMNS] != 0).sum(axis=1)

    # Score bruto = soma de todos os scores das tags ativas
    df["score_bruto"] = df[ALL_TAG_COLUMNS].sum(axis=1).astype(int)

    # Score normalizado (0-100) — so para quem tem tags
    df["score_norm"] = df["score_bruto"].apply(normalize_score)

    # Tier — SEM SCORE para quem nao tem nenhuma tag
    df["tier"] = df.apply(
        lambda r: "SEM SCORE" if r["tags_ativas"] == 0 else classify_tier(r["score_norm"]),
        axis=1,
    )

    # SEM SCORE: score_norm e score_bruto = NULL (evita que CRM interprete 41.2 como Mediano)
    # Usa pd.Int64Dtype para manter integer nullable (sem converter para float)
    df["score_bruto"] = df["score_bruto"].astype("Int64")
    df.loc[df["tier"] == "SEM SCORE", "score_norm"] = None
    df.loc[df["tier"] == "SEM SCORE", "score_bruto"] = pd.NA

    # Remove coluna auxiliar
    df = df.drop(columns=["tags_ativas"])

    return df


# ── Etapa 4: Persistencia PostgreSQL ─────────────────────────────

def save_to_postgres(df: pd.DataFrame, snapshot_date: str) -> None:
    """Salva DataFrame no Super Nova DB via COPY.
    HISTORICO: deleta apenas o snapshot_date atual, preserva snapshots anteriores."""
    if df.empty:
        log.warning("DataFrame vazio, nada a inserir no PostgreSQL.")
        return

    import io

    tunnel, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            # Adiciona colunas faltantes (se tabela ja existe)
            cur.execute(
                f"SELECT column_name FROM information_schema.columns "
                f"WHERE table_schema = '{PG_SCHEMA}' AND table_name = '{PG_TABLE}'"
            )
            existing = set(r[0] for r in cur.fetchall())

            if not existing:
                # Tabela nao existe — cria
                log.info("Criando tabela...")
                cur.execute(CREATE_TABLE_SQL)
                existing = set(df.columns)
            else:
                # Adiciona colunas novas que faltam
                type_map = {
                    "score_bruto": "INTEGER DEFAULT 0",
                    "score_norm": "NUMERIC(5,1) DEFAULT 0",
                    "tier": "VARCHAR(20)",
                }
                for col in df.columns:
                    if col not in existing:
                        col_type = type_map.get(col, "INTEGER DEFAULT 0")
                        cur.execute(
                            f"ALTER TABLE {PG_SCHEMA}.{PG_TABLE} "
                            f"ADD COLUMN IF NOT EXISTS {col} {col_type};"
                        )
                        log.info(f"  ADD COLUMN {col}")

            # HISTORICO: deleta apenas o snapshot atual (preserva anteriores)
            log.info(f"Deletando snapshot {snapshot_date} (preservando historico)...")
            cur.execute(
                f"DELETE FROM {PG_SCHEMA}.{PG_TABLE} WHERE snapshot_date = %s",
                (snapshot_date,),
            )

            # Filtra colunas do DF que existem na tabela
            cur.execute(
                f"SELECT column_name FROM information_schema.columns "
                f"WHERE table_schema = '{PG_SCHEMA}' AND table_name = '{PG_TABLE}'"
            )
            table_cols = set(r[0] for r in cur.fetchall())
            insert_cols = [c for c in df.columns if c in table_cols]
            df_insert = df[insert_cols].drop_duplicates(
                subset=["label_id", "user_id", "snapshot_date"], keep="first"
            )

            # COPY FROM STDIN (rapido para 1M+ linhas)
            buffer = io.StringIO()
            df_insert.to_csv(buffer, index=False, header=False, sep="\t", na_rep="\\N")
            buffer.seek(0)

            cols_str = ", ".join([f'"{c}"' for c in insert_cols])
            copy_sql = (
                f"COPY {PG_SCHEMA}.{PG_TABLE} ({cols_str}) "
                f"FROM STDIN WITH DELIMITER '\t' NULL '\\N'"
            )
            log.info(f"COPY {len(df_insert)} linhas...")
            cur.copy_expert(copy_sql, buffer)

        conn.commit()

        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {PG_SCHEMA}.{PG_TABLE}")
            total = cur.fetchone()[0]
            cur.execute(
                f"SELECT COUNT(DISTINCT snapshot_date) FROM {PG_SCHEMA}.{PG_TABLE}"
            )
            snapshots = cur.fetchone()[0]
        log.info(f"PostgreSQL: {len(df_insert)} linhas inseridas. "
                 f"Total na tabela: {total} ({snapshots} snapshots)")

    except Exception as e:
        conn.rollback()
        log.error(f"ERRO no PostgreSQL: {e}")
        raise
    finally:
        conn.close()
        tunnel.stop()


# ── Etapa 5: Export CSV + Legenda ─────────────────────────────────

def export_csv(df: pd.DataFrame, run_date: str) -> Path:
    """Exporta CSV final para validacao."""
    output_dir = PROJECT_DIR / "reports"
    output_dir.mkdir(exist_ok=True)

    csv_path = output_dir / f"risk_matrix_{run_date}_FINAL.csv"
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    log.info(f"CSV exportado: {csv_path}")
    return csv_path


def export_legenda(run_date: str) -> Path:
    """Gera arquivo de legenda/dicionario para acompanhar o CSV."""
    output_dir = PROJECT_DIR / "reports"
    output_dir.mkdir(exist_ok=True)

    legenda_path = output_dir / f"risk_matrix_{run_date}_legenda.txt"

    lines = [
        "=" * 70,
        "LEGENDA — Matriz de Risco MultiBet",
        f"Data: {run_date}",
        f"Gerado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "=" * 70,
        "",
        "COLUNAS DE IDENTIFICACAO",
        "-" * 40,
        "label_id        : ID do label/marca (c_partner_id do ECR)",
        "user_id          : ID interno do jogador (c_ecr_id, 18 digitos)",
        "user_ext_id      : ID externo Smartico (para cross-referencia CRM)",
        "snapshot_date    : Data do snapshot",
        "",
        "COLUNAS DE TAGS (0 = nao aplicavel, valor = score da tag)",
        "-" * 40,
    ]

    tag_descriptions = {
        "regular_depositor": "+10 | Depositos regulares >=3/mes",
        "promo_only": "-15 | So deposita durante promocoes (>=80%)",
        "zero_risk_player": "  0 | Avg saque ≈ avg deposito (neutro)",
        "fast_cashout": "-25 | Deposito-saque em < 1h (abuso)",
        "sustained_player": "+15 | Saca e continua jogando",
        "non_bonus_depositor": "+10 | Deposita sem usar bonus",
        "promo_chainer": "-10 | 3+ promos consecutivas sem jogo organico",
        "cashout_and_run": "-25 | Bonus > saque > inativo 48h",
        "reinvest_player": "+15 | Saca e reinveste (deposita de novo)",
        "non_promo_player": "+10 | Ativo ha 7d sem usar promo",
        "engaged_player": "+10 | 3-10 sessoes/dia (engajado)",
        "rg_alert_player": " +1 | 10+ sessoes/dia (alerta jogo responsavel)",
        "behav_risk_player": "-10 | Saques em horarios extremos + valores anomalos",
        "potencial_abuser": " -5 | Conta com < 2 dias (muito nova)",
        "player_reengaged": "+30 | Reativado apos 30d inativo, engajado",
        "sleeper_low_player": " +5 | So ativo em eventos sazonais",
        "vip_whale_player": "+30 | GGR > R$15k + alta frequencia",
        "winback_hi_val_player": "+25 | Reativado, R$8k < GGR < R$15k",
        "behav_slotgamer": " +5 | Focado em slots + tem deposito",
        "multi_game_player": "-10 | Sessoes simultaneas (possivel bot)",
        "rollback_player": "-15 | Taxa rollback > 10% (exploit tecnico)",
    }

    for col, desc in tag_descriptions.items():
        lines.append(f"  {col:30s}: {desc}")

    lines.extend([
        "",
        "COLUNAS DE SCORE",
        "-" * 40,
        "score_bruto     : Soma de todos os scores das tags ativas",
        "score_norm      : Score normalizado 0-100 (formula: (bruto+35)/85*100)",
        "tier            : Classificacao baseada no score_norm",
        "",
        "TIERS",
        "-" * 40,
        "  Score > 75  -> Muito Bom  (jogador legitimo, alto engajamento)",
        "  51 - 75     -> Bom        (jogador ativo, deposita sem bonus)",
        "  26 - 50     -> Mediano    (mistura de comportamentos)",
        "  11 - 25     -> Ruim       (dependente de promos, risco medio)",
        "  <= 10       -> Muito Ruim (multiplas flags criticas, investigar)",
        "",
        "FORMULA DE NORMALIZACAO",
        "-" * 40,
        "  score_norm = (score_bruto + 35) / 85 * 100, limitado [0, 100]",
        "  Onde: -35 = P05 calibrado, +50 = P95 calibrado, range = 85",
        "  Pior caso teorico: -115 | Melhor caso: +166",
        "",
        "FONTE DE DADOS",
        "-" * 40,
        "  Banco: AWS Athena (Iceberg Data Lake, sa-east-1)",
        "  Databases: ecr_ec2, fund_ec2, cashier_ec2, bonus_ec2, ps_bi",
        "  Janela: ultimos 90 dias",
        "  Filtros: test_user = false, c_partner_id IS NOT NULL",
        "  Timestamps: UTC (converter para BRT em dashboards)",
        "",
        "ACAO SUGERIDA POR TIER",
        "-" * 40,
        "  Muito Bom  : Monitoramento, considerar pipeline VIP",
        "  Bom        : Manter elegibilidade a promocoes",
        "  Mediano    : Monitoramento passivo, reengajamento opcional",
        "  Ruim       : Restringir bonus, monitorar atividade",
        "  Muito Ruim : Bloqueio temporario, investigacao completa",
        "",
        "=" * 70,
    ])

    legenda_path.write_text("\n".join(lines), encoding="utf-8")
    log.info(f"Legenda exportada: {legenda_path}")
    return legenda_path


# ── Main ─────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description="Pipeline Matriz de Risco MultiBet")
    ap.add_argument("--date", default=date.today().isoformat(),
                    help="Data do snapshot (default: hoje)")
    ap.add_argument("--window_days", type=int, default=90,
                    help="Janela em dias (default: 90)")
    ap.add_argument("--only", nargs="*",
                    help="Executar apenas tags especificas")
    ap.add_argument("--dry-run", action="store_true",
                    help="Apenas exporta CSV, sem gravar no PostgreSQL")
    args = ap.parse_args()

    run_date = args.date
    log.info(f"=== Matriz de Risco MultiBet — {run_date} ===")
    log.info(f"Janela: {args.window_days} dias | Dry-run: {args.dry_run}")

    # ── 1. Descobre e ordena tags ────────────────────────────────
    available = discover_tags()
    tags_to_run = ordered_tags(available, args.only)
    log.info(f"Tags a executar ({len(tags_to_run)}): {', '.join(tags_to_run)}")

    # ── 2. Executa cada tag ──────────────────────────────────────
    scoring_parts: List[pd.DataFrame] = []

    for i, tag in enumerate(tags_to_run, 1):
        log.info(f"[{i}/{len(tags_to_run)}] Executando {tag}...")
        df = run_tag(tag, available[tag])
        if not df.empty:
            scoring_parts.append(df)

    log.info(f"Tags executadas: {len(scoring_parts)} com resultados")

    # ── 3. Base de usuarios ──────────────────────────────────────
    user_base = load_user_base()
    ext_ids = load_user_ext_ids()

    # ── 4. Pivot + merge ─────────────────────────────────────────
    log.info("Pivotando resultados...")
    pivoted = pivot_results(scoring_parts)

    # Merge com base de usuarios (LEFT JOIN: todos os users, mesmo sem tag)
    final = user_base.merge(pivoted, on=["label_id", "user_id"], how="left")

    # Merge user_ext_id
    if not ext_ids.empty:
        final = final.merge(ext_ids, on="user_id", how="left")
    else:
        final["user_ext_id"] = None

    # ── 5. Calcula scores ────────────────────────────────────────
    final = compute_scores(final)
    final["snapshot_date"] = run_date
    final["computed_at"] = datetime.now().isoformat()

    # Ordena colunas
    col_order = (
        ["label_id", "user_id", "user_ext_id", "snapshot_date"]
        + ALL_TAG_COLUMNS
        + ["score_bruto", "score_norm", "tier", "computed_at"]
    )
    final = final[[c for c in col_order if c in final.columns]]

    log.info(f"Total: {len(final)} jogadores, {len(ALL_TAG_COLUMNS)} tags")

    # Stats
    if not final.empty:
        tier_counts = final["tier"].value_counts()
        log.info("Distribuicao por tier:")
        for tier, count in tier_counts.items():
            pct = count / len(final) * 100
            log.info(f"  {tier:15s}: {count:>6d} ({pct:.1f}%)")

    # ── 6. Export ────────────────────────────────────────────────
    csv_path = export_csv(final, run_date)
    legenda_path = export_legenda(run_date)

    # ── 7. Persiste no PostgreSQL ────────────────────────────────
    if not args.dry_run:
        save_to_postgres(final, run_date)
    else:
        log.info("Dry-run: pulando gravacao no PostgreSQL")

    log.info("")
    log.info("=" * 50)
    log.info("CONCLUIDO!")
    log.info(f"  CSV:     {csv_path}")
    log.info(f"  Legenda: {legenda_path}")
    if not args.dry_run:
        log.info(f"  DB:      {PG_SCHEMA}.{PG_TABLE}")
    log.info("=" * 50)


if __name__ == "__main__":
    main()
