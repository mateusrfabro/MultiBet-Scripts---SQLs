"""
Batch de players para teste de IA de gamificacao.

Gera CSV com 1.000 (ou 10.000) players aleatorios que apostaram nos ultimos 15 dias,
cruzando 3 fontes:
  - Redshift (Pragmatic): cadastro, financeiro, casino/sportsbook
  - BigQuery (Smartico): sessions (logins) ultimos 7 dias
  - Super Nova DB (PostgreSQL): vipTier e playerScore (matriz de risco)

Uso:
    python pipelines/base_teste_ia_gamificacao.py                # 1.000 players
    python pipelines/base_teste_ia_gamificacao.py --limit 10000  # 10.000 players
"""

import argparse
import logging
import os
import sys

import pandas as pd

sys.path.insert(0, ".")
from db.redshift import query_redshift
from db.bigquery import query_bigquery
from db.supernova import execute_supernova

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 1) REDSHIFT — base de players + cadastro + financeiro + verticals
#    Fonte: segment schema (Pragmatic Solutions)
# ---------------------------------------------------------------------------
def fetch_redshift_base(limit: int) -> pd.DataFrame:
    sql = f"""
    WITH apostadores AS (
        -- Players distintos que apostaram nos ultimos 30 dias
        SELECT DISTINCT c_ecr_id
        FROM fund.tbl_real_fund_txn
        WHERE c_txn_type = 27              -- Aposta
          AND c_txn_status = 'SUCCESS'
          AND c_start_time >= DATEADD(day, -30, GETDATE())
    ),
    base AS (
        -- Amostra aleatoria de N players
        SELECT c_ecr_id
        FROM apostadores
        ORDER BY RANDOM()
        LIMIT {limit}
    )
    SELECT
        -- playerId (external_id = user_ext_id do Smartico / Super Nova)
        e.c_external_id                         AS player_id,

        -- geo: country_code + state da tabela de perfil (fonte recomendada pela Pragmatic)
        CASE
            WHEN prof.c_state IS NOT NULL AND prof.c_state <> '' AND prof.c_state <> '-NA-'
                THEN COALESCE(prof.c_country_code, 'BR') || '-' || prof.c_state
            WHEN prof.c_country_code IS NOT NULL AND prof.c_country_code <> ''
                THEN prof.c_country_code
            ELSE 'UNKNOWN'
        END                                     AS geo,

        -- kycStatus (fonte transacional: ecr.tbl_ecr_kyc_level, 100% match, tempo real)
        CASE kyc.c_level
            WHEN 'KYC_0' THEN 'none'
            WHEN 'KYC_1' THEN 'pending'
            WHEN 'KYC_2' THEN 'approved'
            ELSE 'none'
        END                                     AS kyc_status,

        -- createdAt (data de cadastro — fonte: ecr.tbl_ecr, 100% match)
        e.c_signup_time                         AS created_at,

        -- hasDepositHistory
        CASE WHEN COALESCE(pay.c_lifetime_deposit_count, 0) > 0
             THEN TRUE ELSE FALSE
        END                                     AS has_deposit_history,

        -- lifetimeDeposit (centavos -> BRL)
        ROUND(COALESCE(pay.c_lifetime_deposit_amount, 0) / 100.0, 2)
                                                AS lifetime_deposit,

        -- ngrLifetime = casino revenue + sportsbook revenue (centavos -> BRL)
        ROUND((COALESCE(cas.c_total_casino_lifetime_revenue, 0)
             + COALESCE(sb.c_total_sb_lifetime_revenue, 0)) / 100.0, 2)
                                                AS ngr_lifetime,

        -- daysSinceLastLogin: baseado em LOGIN real (c_last_login_time), nao em aposta
        DATEDIFF(day, p.c_last_login_time, GETDATE())
                                                AS days_since_last_login,

        -- deposits7d (quantidade de depositos nos ultimos 7 dias)
        COALESCE(pay.c_last_7_days_deposit_count, 0)
                                                AS deposits_7d,

        -- preferredVertical (casino / sportsbook / mixed)
        CASE
            WHEN COALESCE(cas.c_casino_life_time_bet_count, 0) > 0
             AND COALESCE(sb.c_sb_life_time_bet_count, 0) > 0
                THEN 'mixed'
            WHEN COALESCE(sb.c_sb_life_time_bet_count, 0) > 0
                THEN 'sportsbook'
            ELSE 'casino'
        END                                     AS preferred_vertical

    FROM base b
    -- ecr.tbl_ecr: external_id + signup (100% match)
    INNER JOIN ecr.tbl_ecr e
        ON e.c_ecr_id = b.c_ecr_id
    -- ecr.tbl_ecr_profile: geo — state + country (100% match, fonte recomendada Pragmatic)
    LEFT JOIN ecr.tbl_ecr_profile prof
        ON prof.c_ecr_id = b.c_ecr_id
    -- ecr.tbl_ecr_kyc_level: KYC transacional (100% match, tempo real)
    LEFT JOIN ecr.tbl_ecr_kyc_level kyc
        ON kyc.c_ecr_id = b.c_ecr_id
    -- segment particular: apenas para c_last_login_time (~88% match, fallback via BigQuery)
    LEFT JOIN segment.tbl_segment_ecr_particular_details p
        ON p.c_ecr_id = b.c_ecr_id
    -- segment payment: depositos
    LEFT JOIN segment.tbl_segment_ecr_payment_details pay
        ON pay.c_ecr_id = b.c_ecr_id
    -- segment casino: revenue + bets casino
    LEFT JOIN segment.tbl_segment_ecr_casino_details cas
        ON cas.c_ecr_id = b.c_ecr_id
    -- segment sportsbook: revenue + bets sb
    LEFT JOIN segment.tbl_segment_ecr_sb_details sb
        ON sb.c_ecr_id = b.c_ecr_id
    """
    log.info(f"Redshift: buscando {limit} players aleatorios (ultimos 15d)...")
    df = query_redshift(sql)
    log.info(f"Redshift: {len(df)} linhas retornadas.")
    return df


# ---------------------------------------------------------------------------
# 2) BIGQUERY (Smartico) — sessions 7d + ultimo login (fallback para days_since_last_login)
# ---------------------------------------------------------------------------
def fetch_smartico_login_data(player_ids: list) -> pd.DataFrame:
    if not player_ids:
        return pd.DataFrame(columns=["player_id", "sessions_7d", "last_login_bq"])

    ids_str = ", ".join([f"'{pid}'" for pid in player_ids])

    sql = f"""
    WITH target_users AS (
        -- Mapeia external_id -> user_id interno do Smartico
        SELECT user_id, CAST(user_ext_id AS INT64) AS player_id
        FROM `smartico-bq6.dwh_ext_24105.j_user`
        WHERE user_ext_id IN ({ids_str})
    ),
    all_logins AS (
        SELECT tu.player_id, l.event_time
        FROM target_users tu
        LEFT JOIN `smartico-bq6.dwh_ext_24105.tr_login` l
            ON l.user_id = tu.user_id
    )
    SELECT
        player_id,
        -- sessions nos ultimos 7 dias
        COUNTIF(event_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)) AS sessions_7d,
        -- ultimo login (fallback para days_since_last_login quando Redshift nao tem)
        MAX(event_time) AS last_login_bq
    FROM all_logins
    GROUP BY player_id
    """
    log.info(f"BigQuery: buscando sessions 7d + ultimo login para {len(player_ids)} players...")
    df = query_bigquery(sql)
    log.info(f"BigQuery: {len(df)} linhas retornadas.")
    return df


# ---------------------------------------------------------------------------
# 3) SUPER NOVA DB — vipTier e playerScore (view multibet.matriz_risco)
# ---------------------------------------------------------------------------
def fetch_matriz_risco(player_ids: list) -> pd.DataFrame:
    if not player_ids:
        return pd.DataFrame(columns=["player_id", "vip_tier", "player_score"])

    ids_str = ", ".join([f"'{pid}'" for pid in player_ids])

    sql = f"""
    SELECT
        CAST(user_ext_id AS BIGINT) AS player_id,
        classificacao               AS vip_tier,
        score_norm                  AS player_score
    FROM multibet.matriz_risco
    WHERE user_ext_id IN ({ids_str})
    """
    log.info(f"Super Nova: buscando matriz de risco para {len(player_ids)} players...")
    rows = execute_supernova(sql, fetch=True)
    df = pd.DataFrame(rows, columns=["player_id", "vip_tier", "player_score"])
    log.info(f"Super Nova: {len(df)} linhas retornadas.")
    return df


# ---------------------------------------------------------------------------
# 4) MERGE — junta as 3 fontes e exporta CSV
# ---------------------------------------------------------------------------
def build_base(limit: int = 1000) -> pd.DataFrame:
    # Passo 1: Redshift (base principal)
    df = fetch_redshift_base(limit)
    player_ids = df["player_id"].dropna().astype(int).tolist()
    log.info(f"IDs unicos para cruzamento: {len(player_ids)}")

    # Passo 2: BigQuery — sessions 7d + ultimo login (fallback)
    df_login = fetch_smartico_login_data(player_ids)
    df_login["player_id"] = df_login["player_id"].astype(int)

    # Passo 3: Super Nova — vipTier + playerScore
    df_risco = fetch_matriz_risco(player_ids)
    df_risco["player_id"] = df_risco["player_id"].astype(int)

    # Merge tudo pelo player_id
    df["player_id"] = df["player_id"].astype(int)
    df = df.merge(df_login, on="player_id", how="left")
    df = df.merge(df_risco, on="player_id", how="left")

    # Fallback: quando Redshift nao tem days_since_last_login, usa BigQuery tr_login
    if "last_login_bq" in df.columns:
        mask = df["days_since_last_login"].isna() & df["last_login_bq"].notna()
        df.loc[mask, "days_since_last_login"] = (
            pd.Timestamp.now(tz="UTC") - pd.to_datetime(df.loc[mask, "last_login_bq"], utc=True)
        ).dt.days
        fallback_count = mask.sum()
        if fallback_count > 0:
            log.info(f"Fallback BigQuery para days_since_last_login: {fallback_count} players preenchidos")
        df.drop(columns=["last_login_bq"], inplace=True)

    # Preenche nulos com defaults
    df["sessions_7d"] = df["sessions_7d"].fillna(0).astype(int)
    df["days_since_last_login"] = df["days_since_last_login"].fillna(-1).astype(int)
    df["vip_tier"] = df["vip_tier"].fillna("Sem Score")
    df["player_score"] = df["player_score"].fillna(0)

    # Ordena colunas no padrao esperado
    df = df[[
        "player_id",
        "geo",
        "kyc_status",
        "vip_tier",
        "player_score",
        "created_at",
        "has_deposit_history",
        "lifetime_deposit",
        "ngr_lifetime",
        "days_since_last_login",
        "sessions_7d",
        "deposits_7d",
        "preferred_vertical",
    ]]

    return df


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Batch para teste IA gamificacao")
    parser.add_argument("--limit", type=int, default=1000,
                        help="Quantidade de players (default: 1000)")
    args = parser.parse_args()

    df = build_base(limit=args.limit)

    os.makedirs("output", exist_ok=True)
    output_path = f"output/base_teste_ia_gamificacao_{args.limit}.csv"
    df.to_csv(output_path, index=False)

    log.info(f"Base exportada: {output_path} ({len(df)} linhas)")
    print(f"\nBase gerada com {len(df)} players -> {output_path}")
    print(df.head(10).to_string())