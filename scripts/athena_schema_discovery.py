"""
Athena Schema Discovery — Descobre schemas completos de tabelas de gaming, casino,
sports, live casino, jackpot no Data Lake.

Saída: output/athena_schema_discovery_results.txt

Usa DESCRIBE ao invés de SHOW COLUMNS (compatível com Athena/Iceberg).
"""

import sys
import os
import traceback
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.athena import query_athena

OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "athena_schema_discovery_results.txt")

results = []


def log(msg: str):
    """Imprime e armazena no buffer de resultados."""
    print(msg)
    results.append(msg)


def run_query(description: str, sql: str, database: str = "default", retries: int = 1):
    """Executa query, loga resultado ou erro, e retorna o DataFrame."""
    log(f"\n{'='*80}")
    log(f"  {description}")
    log(f"  SQL: {sql}")
    log(f"  Database: {database}")
    log(f"{'='*80}")
    try:
        df = query_athena(sql, database=database, retries=retries)
        log(f"  -> {len(df)} linhas retornadas")
        if len(df) > 0:
            # Para DataFrames muito largos, usar to_string com max_colwidth
            log(df.to_string(index=False, max_colwidth=60))
        else:
            log("  (vazio)")
        return df
    except Exception as e:
        # Extrair só a mensagem relevante, sem stacktrace gigante
        err_msg = str(e).split("\n")[0][:500]
        log(f"  ERRO: {err_msg}")
        return None


def main():
    start = datetime.now()
    log(f"Athena Schema Discovery — Início: {start.strftime('%Y-%m-%d %H:%M:%S')}")

    # -------------------------------------------------------------------------
    # 1. Listar TODAS as tabelas em ps_bi
    # -------------------------------------------------------------------------
    run_query(
        "1. TODAS as tabelas em ps_bi",
        "SHOW TABLES",
        database="ps_bi",
    )

    # -------------------------------------------------------------------------
    # 2. Listar TODAS as tabelas em bireports_ec2
    # -------------------------------------------------------------------------
    run_query(
        "2. TODAS as tabelas em bireports_ec2",
        "SHOW TABLES",
        database="bireports_ec2",
    )

    # -------------------------------------------------------------------------
    # 3. DESCRIBE + SAMPLE para tabelas específicas
    # -------------------------------------------------------------------------
    tables_to_inspect = [
        ("ps_bi", "fct_casino_activity_daily"),
        ("ps_bi", "dim_game"),
        ("ps_bi", "dim_bonus"),
        ("ps_bi", "fct_player_activity_daily"),
        ("bireports_ec2", "tbl_ecr_txn_type_wise_daily_game_play_summary"),
        ("bireports_ec2", "tbl_vendor_games_mapping_data"),
        ("bireports_ec2", "tbl_ecr_wise_daily_bi_summary"),
        ("bireports_ec2", "tbl_ecr_daily_settled_game_play_summary"),
        ("vendor_ec2", "tbl_sports_book_bets_info"),
        ("vendor_ec2", "tbl_sports_book_bet_details"),
        ("casino_ec2", "tbl_casino_game_category_mst"),
        ("casino_ec2", "tbl_casino_game_type_mst"),
    ]

    for db, table in tables_to_inspect:
        # DESCRIBE para pegar schema
        run_query(
            f"3a. DESCRIBE — {db}.{table}",
            f"DESCRIBE {table}",
            database=db,
        )

        # SAMPLE
        if table == "tbl_ecr_wise_daily_bi_summary":
            # Tabela muito larga — pegar nomes de colunas via DESCRIBE primeiro
            cols_df = None
            try:
                cols_df = query_athena(f"DESCRIBE {table}", database=db, retries=1)
            except Exception:
                pass

            if cols_df is not None and len(cols_df) > 0:
                # DESCRIBE retorna col names na primeira coluna
                col_names = cols_df.iloc[:, 0].tolist()
                # Filtrar linhas que são partições (começam com #)
                col_names = [c.strip() for c in col_names if not c.strip().startswith("#") and c.strip()]
                first_30 = col_names[:30]
                cols_str = ", ".join(first_30)
                run_query(
                    f"3b. SAMPLE (primeiras 30 cols) — {db}.{table}",
                    f"SELECT {cols_str} FROM {table} LIMIT 3",
                    database=db,
                )
            else:
                run_query(
                    f"3b. SAMPLE — {db}.{table}",
                    f"SELECT * FROM {table} LIMIT 3",
                    database=db,
                )
        else:
            run_query(
                f"3b. SAMPLE — {db}.{table}",
                f"SELECT * FROM {table} LIMIT 3",
                database=db,
            )

    # -------------------------------------------------------------------------
    # 4. Buscar tabelas com 'jackpot' em todos os databases
    # -------------------------------------------------------------------------
    log(f"\n{'='*80}")
    log("  4. Busca por tabelas com 'jackpot' em TODOS os databases")
    log(f"{'='*80}")

    dbs_df = run_query("4a. Listar databases", "SHOW DATABASES")

    if dbs_df is not None and len(dbs_df) > 0:
        all_dbs = dbs_df.iloc[:, 0].tolist()
        jackpot_found = []

        for db_name in all_dbs:
            try:
                tables_df = query_athena("SHOW TABLES", database=db_name, retries=1)
                if tables_df is not None and len(tables_df) > 0:
                    table_names = tables_df.iloc[:, 0].tolist()
                    matches = [t for t in table_names if "jackpot" in t.lower()]
                    if matches:
                        for m in matches:
                            jackpot_found.append((db_name, m))
                            log(f"  JACKPOT TABLE FOUND: {db_name}.{m}")
            except Exception as e:
                err_msg = str(e).split("\n")[0][:200]
                log(f"  Erro ao listar tabelas em {db_name}: {err_msg}")

        if not jackpot_found:
            log("  Nenhuma tabela com 'jackpot' encontrada em nenhum database.")
        else:
            log(f"\n  Total de tabelas jackpot: {len(jackpot_found)}")
            for db_name, tbl_name in jackpot_found:
                run_query(
                    f"4b. DESCRIBE — {db_name}.{tbl_name}",
                    f"DESCRIBE {tbl_name}",
                    database=db_name,
                )
                run_query(
                    f"4c. SAMPLE — {db_name}.{tbl_name}",
                    f"SELECT * FROM {tbl_name} LIMIT 3",
                    database=db_name,
                )

    # -------------------------------------------------------------------------
    # 5. BONUS: listar tabelas em casino_ec2, vendor_ec2, bonus_ec2
    # -------------------------------------------------------------------------
    run_query(
        "5a. TODAS as tabelas em casino_ec2",
        "SHOW TABLES",
        database="casino_ec2",
    )

    run_query(
        "5b. TODAS as tabelas em vendor_ec2",
        "SHOW TABLES",
        database="vendor_ec2",
    )

    run_query(
        "5c. TODAS as tabelas em bonus_ec2",
        "SHOW TABLES",
        database="bonus_ec2",
    )

    # -------------------------------------------------------------------------
    # 6. BONUS: Procurar tabelas com 'round', 'session', 'live' em casino_ec2
    # -------------------------------------------------------------------------
    log(f"\n{'='*80}")
    log("  6. Inspecionar tabelas relevantes em casino_ec2")
    log(f"{'='*80}")
    try:
        casino_tables = query_athena("SHOW TABLES", database="casino_ec2", retries=1)
        if casino_tables is not None and len(casino_tables) > 0:
            all_casino = casino_tables.iloc[:, 0].tolist()
            keywords = ["round", "session", "live", "game", "category", "type", "provider", "vendor"]
            for tbl in all_casino:
                if any(kw in tbl.lower() for kw in keywords):
                    run_query(
                        f"6. DESCRIBE — casino_ec2.{tbl}",
                        f"DESCRIBE {tbl}",
                        database="casino_ec2",
                    )
    except Exception as e:
        log(f"  Erro: {str(e)[:200]}")

    # -------------------------------------------------------------------------
    # Fim
    # -------------------------------------------------------------------------
    end = datetime.now()
    duration = (end - start).total_seconds()
    log(f"\nDiscovery finalizado em {duration:.1f}s — {end.strftime('%Y-%m-%d %H:%M:%S')}")

    # Salvar resultados
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(results))
    log(f"\nResultados salvos em: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
