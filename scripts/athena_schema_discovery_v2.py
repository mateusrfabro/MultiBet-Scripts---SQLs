"""
Athena Schema Discovery v2 — Usa information_schema para colunas+tipos
(DESCRIBE tem bug de parse no pandas/pyathena).

Saída: output/athena_schema_discovery_results.txt  (sobrescreve)
"""

import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.athena import query_athena

OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "athena_schema_discovery_results.txt")

results = []


def log(msg: str):
    print(msg)
    results.append(msg)


def run_query(description, sql, database="default"):
    log(f"\n{'='*90}")
    log(f"  {description}")
    log(f"  Database: {database}")
    log(f"{'='*90}")
    try:
        df = query_athena(sql, database=database, retries=1)
        log(f"  -> {len(df)} rows")
        if len(df) > 0:
            log(df.to_string(index=False, max_colwidth=70))
        else:
            log("  (empty)")
        return df
    except Exception as e:
        err = str(e).split("\n")[0][:400]
        log(f"  ERROR: {err}")
        return None


def get_columns(database, table):
    """Retorna colunas + tipos via information_schema."""
    sql = f"""
    SELECT column_name, data_type, ordinal_position
    FROM information_schema.columns
    WHERE table_schema = '{database}'
      AND table_name = '{table}'
    ORDER BY ordinal_position
    """
    return run_query(
        f"COLUMNS+TYPES — {database}.{table}",
        sql,
        database=database,
    )


def get_sample(database, table, limit=3):
    """Retorna sample de linhas."""
    return run_query(
        f"SAMPLE ({limit} rows) — {database}.{table}",
        f"SELECT * FROM {table} LIMIT {limit}",
        database=database,
    )


def main():
    start = datetime.now()
    log(f"Athena Schema Discovery v2 — {start.strftime('%Y-%m-%d %H:%M:%S')}")
    log(f"Objetivo: mapear tabelas de gaming/casino/sports/jackpot")

    # =========================================================================
    # 1. LISTAR TABELAS — ps_bi
    # =========================================================================
    run_query("1. ALL TABLES — ps_bi", "SHOW TABLES", database="ps_bi")

    # =========================================================================
    # 2. LISTAR TABELAS — bireports_ec2
    # =========================================================================
    run_query("2. ALL TABLES — bireports_ec2", "SHOW TABLES", database="bireports_ec2")

    # =========================================================================
    # 3. SCHEMA + SAMPLE para cada tabela-alvo
    # =========================================================================
    targets = [
        ("ps_bi", "fct_casino_activity_daily"),
        ("ps_bi", "fct_casino_activity_hourly"),
        ("ps_bi", "dim_game"),
        ("ps_bi", "dim_bonus"),
        ("ps_bi", "fct_player_activity_daily"),
        ("ps_bi", "fct_player_balance_daily"),
        ("ps_bi", "fct_player_count"),
        ("bireports_ec2", "tbl_ecr_txn_type_wise_daily_game_play_summary"),
        ("bireports_ec2", "tbl_vendor_games_mapping_data"),
        ("bireports_ec2", "tbl_ecr_wise_daily_bi_summary"),
        ("bireports_ec2", "tbl_ecr_gaming_sessions"),
        ("bireports_ec2", "tbl_ecr_hourly_game_play_statistics"),
        ("bireports_ec2", "tbl_ecr_daily_settled_game_play_summary"),
        ("vendor_ec2", "tbl_sports_book_bets_info"),
        ("vendor_ec2", "tbl_sports_book_bet_details"),
        ("vendor_ec2", "tbl_sports_book_info"),
        ("vendor_ec2", "tbl_free_rounds_bonus_mapper"),
        ("vendor_ec2", "tbl_vendor_games_mapping_mst"),
        ("casino_ec2", "tbl_casino_game_category_mst"),
        ("casino_ec2", "tbl_casino_game_type_mst"),
        ("casino_ec2", "tbl_casino_game_type_info"),
    ]

    for db, table in targets:
        get_columns(db, table)
        get_sample(db, table, limit=3)

    # =========================================================================
    # 4. JACKPOT SEARCH — todos os databases
    # =========================================================================
    log(f"\n{'#'*90}")
    log("  4. JACKPOT SEARCH — procurar 'jackpot' em nomes de tabelas de todos os databases")
    log(f"{'#'*90}")

    dbs_df = run_query("4a. ALL DATABASES", "SHOW DATABASES")
    if dbs_df is not None:
        all_dbs = dbs_df.iloc[:, 0].tolist()
        found = []
        for db_name in all_dbs:
            try:
                tdf = query_athena("SHOW TABLES", database=db_name, retries=1)
                if tdf is not None and len(tdf) > 0:
                    for t in tdf.iloc[:, 0].tolist():
                        if "jackpot" in t.lower():
                            found.append((db_name, t))
                            log(f"  FOUND: {db_name}.{t}")
            except Exception:
                pass

        if not found:
            log("  No tables with 'jackpot' in name found in any database.")
            # Tentar buscar colunas com 'jackpot' nas tabelas que já conhecemos
            log("  Checking for 'jackpot' COLUMNS in key tables instead...")
            jackpot_col_tables = [
                ("ps_bi", "fct_casino_activity_daily"),
                ("ps_bi", "fct_player_activity_daily"),
                ("bireports_ec2", "tbl_ecr_wise_daily_bi_summary"),
                ("bireports_ec2", "tbl_ecr_txn_type_wise_daily_game_play_summary"),
            ]
            for db, table in jackpot_col_tables:
                run_query(
                    f"JACKPOT COLUMNS in {db}.{table}",
                    f"""SELECT column_name, data_type
                        FROM information_schema.columns
                        WHERE table_schema = '{db}'
                          AND table_name = '{table}'
                          AND column_name LIKE '%jackpot%'
                        ORDER BY ordinal_position""",
                    database=db,
                )
        else:
            for db_name, tbl in found:
                get_columns(db_name, tbl)
                get_sample(db_name, tbl)

    # =========================================================================
    # 5. EXTRA — casino_ec2, vendor_ec2, bonus_ec2, fund_ec2 table lists
    # =========================================================================
    run_query("5a. ALL TABLES — casino_ec2", "SHOW TABLES", database="casino_ec2")
    run_query("5b. ALL TABLES — vendor_ec2", "SHOW TABLES", database="vendor_ec2")
    run_query("5c. ALL TABLES — bonus_ec2", "SHOW TABLES", database="bonus_ec2")
    run_query("5d. ALL TABLES — fund_ec2", "SHOW TABLES", database="fund_ec2")
    run_query("5e. ALL TABLES — silver", "SHOW TABLES", database="silver")

    # =========================================================================
    # 6. EXTRA — game_type distinct values (entender categorias de jogo)
    # =========================================================================
    run_query(
        "6a. DISTINCT game_type values in ps_bi.fct_casino_activity_daily",
        """SELECT DISTINCT game_type, sub_vendor_id, product_id, sub_product_id
           FROM fct_casino_activity_daily
           LIMIT 50""",
        database="ps_bi",
    )

    run_query(
        "6b. DISTINCT game_category in bireports_ec2.tbl_vendor_games_mapping_data",
        """SELECT DISTINCT c_game_category, c_product_id, COUNT(*) as cnt
           FROM tbl_vendor_games_mapping_data
           GROUP BY c_game_category, c_product_id
           ORDER BY cnt DESC""",
        database="bireports_ec2",
    )

    run_query(
        "6c. DISTINCT c_game_cat_desc from casino_ec2.tbl_casino_game_category_mst (active)",
        """SELECT c_game_cat_id, c_game_cat_desc, c_vendor_id, c_game_category, c_status
           FROM tbl_casino_game_category_mst
           WHERE c_status = 'active'
           ORDER BY c_vendor_id, c_game_cat_desc""",
        database="casino_ec2",
    )

    run_query(
        "6d. DISTINCT product_id in ps_bi.dim_game",
        """SELECT DISTINCT product_id, vendor_id, game_category, COUNT(*) as cnt
           FROM dim_game
           GROUP BY product_id, vendor_id, game_category
           ORDER BY cnt DESC
           LIMIT 30""",
        database="ps_bi",
    )

    # =========================================================================
    # 7. bireports_ec2 — gaming sessions sample (live indicator?)
    # =========================================================================
    run_query(
        "7. SAMPLE — bireports_ec2.tbl_ecr_gaming_sessions (round/session data?)",
        "SELECT * FROM tbl_ecr_gaming_sessions LIMIT 5",
        database="bireports_ec2",
    )

    run_query(
        "7b. SAMPLE — bireports_ec2.tbl_ecr_hourly_game_play_statistics",
        "SELECT * FROM tbl_ecr_hourly_game_play_statistics LIMIT 5",
        database="bireports_ec2",
    )

    # =========================================================================
    # FIM
    # =========================================================================
    end = datetime.now()
    log(f"\nDiscovery v2 done in {(end-start).total_seconds():.1f}s — {end.strftime('%H:%M:%S')}")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(results))
    log(f"Saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
