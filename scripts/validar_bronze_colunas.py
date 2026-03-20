"""
Validação de colunas Bronze — Mauro FINAL
==========================================
Roda SHOW COLUMNS em cada tabela listada no bronze_selects_kpis_FINAL.pdf
e cruza com as colunas que o Mauro pediu em cada SELECT.

Saída: relatório console + CSV com status OK / NAO_EXISTE por coluna.
"""

import sys
import logging
import pandas as pd
from datetime import datetime

sys.path.insert(0, ".")
from db.athena import query_athena

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)

# ============================================================================
# Mapeamento: cada SELECT do Mauro → tabela fonte + colunas pedidas
# ============================================================================

BRONZE_SELECTS = {
    "1. fact_registrations (ecr_ec2.tbl_ecr)": {
        "database": "ecr_ec2",
        "table": "tbl_ecr",
        "columns": [
            "c_ecr_id", "c_external_id", "c_tracker_id", "c_affiliate_id",
            "c_country_code", "c_signup_time"
        ]
    },
    "1b. fact_registrations JOIN (ecr_ec2.tbl_ecr_flags)": {
        "database": "ecr_ec2",
        "table": "tbl_ecr_flags",
        "columns": ["c_ecr_id", "c_test_user"]
    },
    "2. fact_ftd_deposits (cashier_ec2.tbl_cashier_deposit)": {
        "database": "cashier_ec2",
        "table": "tbl_cashier_deposit",
        "columns": [
            "c_ecr_id", "c_txn_id", "c_initial_amount",
            "c_created_time", "c_txn_status"
        ]
    },
    "3. dim_marketing_mapping (ecr_ec2.tbl_ecr_banner)": {
        "database": "ecr_ec2",
        "table": "tbl_ecr_banner",
        "columns": [
            "c_tracker_id", "c_click_id", "c_utm_source",
            "c_utm_medium", "c_utm_campaign", "c_affiliate_id", "c_created_time"
        ]
    },
    "4. fact_gaming_activity (fund_ec2.tbl_real_fund_txn)": {
        "database": "fund_ec2",
        "table": "tbl_real_fund_txn",
        "columns": [
            "c_ecr_id", "c_tracker_id", "c_txn_id", "c_txn_type",
            "c_start_time",
            # Colunas que o Mauro NAO incluiu mas que sao criticas:
            "c_amount_in_ecr_ccy",                    # valor (centavos)
            "c_confirmed_amount_in_inhouse_ccy",      # valor alternativo (arquiteto)
            "c_game_id",                              # ID do jogo
            "c_vendor_id",                            # ID do provedor
            "c_product_id",                           # casino/sports
            "c_txn_status",                           # status da transacao
            "c_round_id",                             # ID da rodada
            "dt",                                     # particao (arquiteto diz que existe)
        ]
    },
    "5. Sub-Fund Real (fund_ec2.tbl_realcash_sub_fund_txn)": {
        "database": "fund_ec2",
        "table": "tbl_realcash_sub_fund_txn",
        "columns": [
            "c_fund_txn_id", "c_amount_in_house_ccy",
            # Extras pra validar:
            "c_ecr_id",  # existe?
        ]
    },
    "6. Sub-Fund Bonus (fund_ec2.tbl_bonus_sub_fund_txn)": {
        "database": "fund_ec2",
        "table": "tbl_bonus_sub_fund_txn",
        "columns": [
            "c_fund_txn_id",
            "c_drp_amount_in_house_ccy",  # DRP = Real
            "c_crp_amount_in_house_ccy",  # CRP = Bonus
            "c_wrp_amount_in_house_ccy",  # WRP = Bonus
            "c_rrp_amount_in_house_ccy",  # RRP = Bonus
            # Extras:
            "c_ecr_id",  # existe?
        ]
    },
    "7. Tipos Transacao (fund_ec2.tbl_real_fund_txn_type_mst)": {
        "database": "fund_ec2",
        "table": "tbl_real_fund_txn_type_mst",
        "columns": [
            "c_txn_type", "c_txn_type_name", "c_op_type",
            "c_is_gaming_txn", "c_is_cancel_txn", "c_txn_identifier_key"
        ]
    },
    "9. Catalogo Jogos (vendor_ec2.tbl_vendor_games_mapping_mst)": {
        "database": "vendor_ec2",
        "table": "tbl_vendor_games_mapping_mst",
        "columns": [
            "c_game_id", "c_game_name", "c_vendor_id", "c_sub_vendor_id",
            "c_product_id", "c_game_category", "c_game_type_desc",
            "c_status", "c_has_jackpot", "c_has_free_spins",
            # Extras que Mauro pediu:
            "c_game_type_id", "c_technology", "c_feature_trigger", "c_updated_dt"
        ]
    },
    "10. Sports Bets (vendor_ec2.tbl_sports_book_bets_info)": {
        "database": "vendor_ec2",
        "table": "tbl_sports_book_bets_info",
        "columns": [
            "c_bet_id", "c_customer_id", "c_sport_name",  # c_sport_name existe?
            "c_total_stake", "c_total_return", "c_bonus_amount",
            "c_is_free", "c_bet_type", "c_bet_state",
            "c_transaction_type", "c_bet_closure_time", "c_created_time",
            # Extras da nossa validacao:
            "c_sport_type_name",  # alternativa a c_sport_name?
            "c_bet_slip_id", "c_total_odds",
        ]
    },
    "10b. Sports Details (vendor_ec2.tbl_sports_book_bet_details)": {
        "database": "vendor_ec2",
        "table": "tbl_sports_book_bet_details",
        "columns": [
            "c_sport_type_name", "c_sport_id", "c_event_name",
            "c_market_name", "c_selection_name", "c_odds",
            "c_leg_status", "c_tournament_name",
            "c_bet_slip_id", "c_transaction_id",
            "c_customer_id", "c_is_live",
        ]
    },
    "11. Gaming Sessions (bireports_ec2.tbl_ecr_gaming_sessions)": {
        "database": "bireports_ec2",
        "table": "tbl_ecr_gaming_sessions",
        "columns": [
            "c_ecr_id", "c_game_id",
            "c_session_start_time", "c_session_end_time",
            "c_session_duration_sec",  # Mauro usou esse nome
            "c_session_length_in_sec",  # nome real validado antes?
            "c_round_count",
            "c_game_played_count",  # alternativa?
            "c_product_id",
        ]
    },
    "12. Cashout (cashier_ec2.tbl_cashier_cashout)": {
        "database": "cashier_ec2",
        "table": "tbl_cashier_cashout",
        "columns": [
            "c_ecr_id", "c_txn_id", "c_initial_amount",
            "c_created_time", "c_txn_status"
        ]
    },
    "13. Daily Payment Summary (cashier_ec2.tbl_cashier_ecr_daily_payment_summary)": {
        "database": "cashier_ec2",
        "table": "tbl_cashier_ecr_daily_payment_summary",
        "columns": [
            "c_ecr_id", "c_date",
            "c_deposit_amount_brl", "c_deposit_count",
            "c_withdrawal_amount_brl", "c_withdrawal_count",
            "c_net_deposit_brl", "c_avg_deposit_ticket"
        ]
    },
    "14. Instrumentos Pagamento (cashier_ec2.tbl_instrument)": {
        "database": "cashier_ec2",
        "table": "tbl_instrument",
        "columns": [
            "c_instrument_id", "c_instrument_name", "c_instrument_type",
            "c_processing_time_avg_minutes", "c_approval_rate_pct",
            "c_chargeback_rate_pct"
        ]
    },
    "15. Bonus Details (bonus_ec2.tbl_ecr_bonus_details)": {
        "database": "bonus_ec2",
        "table": "tbl_ecr_bonus_details",
        "columns": [
            "c_ecr_id", "c_bonus_id", "c_bonus_amount_brl",
            "c_bonus_type", "c_rollover_requirement", "c_rollover_completed",
            "c_bonus_status", "c_issued_date", "c_expiry_date"
        ]
    },
    "17. Fraud/Risk (risk_ec2.tbl_ecr_ccf_score)": {
        "database": "risk_ec2",
        "table": "tbl_ecr_ccf_score",
        "columns": [
            "c_ecr_id", "c_ccf_score", "c_risk_level",
            "c_calculated_date", "c_fraud_indicators_json", "c_aml_flags_json"
        ]
    },
    "18. KYC (ecr_ec2.tbl_ecr_kyc_level)": {
        "database": "ecr_ec2",
        "table": "tbl_ecr_kyc_level",
        "columns": [
            "c_ecr_id", "c_kyc_level", "c_verification_status",
            "c_updated_date", "c_documents_verified_count",
            "c_verification_time_hours"
        ]
    },
    "19. dim_games (ps_bi.dim_game)": {
        "database": "ps_bi",
        "table": "dim_game",
        "columns": [
            "game_id", "game_name", "vendor_id", "sub_vendor_id",
            "product_id", "game_category", "game_type_id",
            "game_type_desc", "status", "game_technology",
        ]
    },
    "20. Casino Activity (ps_bi.fct_casino_activity_daily)": {
        "database": "ps_bi",
        "table": "fct_casino_activity_daily",
        "columns": [
            "activity_date", "ecr_id", "game_id",
            "real_bet_amount_local", "real_win_amount_local",
            "bonus_bet_amount_local", "bonus_win_amount_local",
            "jackpot_win_amount_local", "jackpot_contribution_local",
            "free_spin_bet_amount_local", "free_spin_win_amount_local",
            "number_of_rounds",
        ]
    },
    "20b. Game Images (bireports_ec2.tbl_vendor_games_mapping_data)": {
        "database": "bireports_ec2",
        "table": "tbl_vendor_games_mapping_data",
        "columns": [
            "c_game_id", "c_game_name", "c_vendor_id"
        ]
    },
    "25. Flags (ecr_ec2.tbl_ecr_flags)": {
        "database": "ecr_ec2",
        "table": "tbl_ecr_flags",
        "columns": [
            "c_ecr_id", "c_test_user",
            # Mauro adicionou estas — existem?
            "c_flag_name", "c_flag_value"
        ]
    },
    # === TABELAS FALTANTES que sugeri adicionar ===
    "EXTRA: BI Summary (bireports_ec2.tbl_ecr_wise_daily_bi_summary)": {
        "database": "bireports_ec2",
        "table": "tbl_ecr_wise_daily_bi_summary",
        "columns": [
            "c_ecr_id", "c_date",
            "c_casino_realcash_bet", "c_casino_realcash_win",
            "c_sb_realcash_bet", "c_sb_realcash_win",
        ]
    },
    "EXTRA: BI ECR Master (bireports_ec2.tbl_ecr)": {
        "database": "bireports_ec2",
        "table": "tbl_ecr",
        "columns": [
            "c_ecr_id", "c_external_id", "c_test_user",
            "c_last_login_time", "c_country_code",
        ]
    },
    "EXTRA: dim_user (ps_bi.dim_user)": {
        "database": "ps_bi",
        "table": "dim_user",
        "columns": [
            "ecr_id", "external_id", "registration_date",
            "country_code", "status",
        ]
    },
    "EXTRA: Player Activity (ps_bi.fct_player_activity_daily)": {
        "database": "ps_bi",
        "table": "fct_player_activity_daily",
        "columns": [
            "activity_date", "ecr_id",
            "deposit_amount_local", "withdrawal_amount_local",
            "real_bet_amount_local", "real_win_amount_local",
        ]
    },
    "EXTRA: Bonus Activity (ps_bi.fct_bonus_activity_daily)": {
        "database": "ps_bi",
        "table": "fct_bonus_activity_daily",
        "columns": [
            "activity_date", "ecr_id", "bonus_id",
        ]
    },
}


def get_table_columns(database: str, table: str) -> set:
    """Retorna set de colunas reais da tabela no Athena."""
    try:
        sql = f"SHOW COLUMNS IN {database}.{table}"
        df = query_athena(sql, database=database)
        # SHOW COLUMNS retorna uma coluna com nome 'column' ou 'Column'
        col_name = df.columns[0]
        return set(df[col_name].str.strip().str.lower().tolist())
    except Exception as e:
        log.error(f"  ERRO ao acessar {database}.{table}: {e}")
        return set()


def main():
    results = []
    tables_ok = 0
    tables_fail = 0
    tables_not_found = 0

    print("=" * 80)
    print("VALIDACAO BRONZE — Colunas do Mauro vs Athena Real")
    print(f"Data: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 80)

    for select_name, config in BRONZE_SELECTS.items():
        db = config["database"]
        tbl = config["table"]
        expected_cols = config["columns"]

        print(f"\n{'-' * 70}")
        print(f"  {select_name}")
        print(f"  Tabela: {db}.{tbl}")
        print(f"{'-' * 70}")

        real_cols = get_table_columns(db, tbl)

        if not real_cols:
            print(f"  >>> TABELA NAO ENCONTRADA ou sem acesso <<<")
            tables_not_found += 1
            for col in expected_cols:
                results.append({
                    "select": select_name,
                    "database": db,
                    "table": tbl,
                    "column": col,
                    "status": "TABELA_NAO_ENCONTRADA",
                    "nota": ""
                })
            continue

        has_issue = False
        for col in expected_cols:
            col_lower = col.strip().lower()
            if col_lower in real_cols:
                status = "OK"
                nota = ""
            else:
                status = "NAO_EXISTE"
                nota = ""
                # Tentar sugerir alternativa
                for real_col in sorted(real_cols):
                    # Similaridade basica: mesmo prefixo ou contem parte do nome
                    col_parts = col_lower.replace("c_", "").split("_")
                    if len(col_parts) >= 2:
                        key = "_".join(col_parts[:2])
                        if key in real_col:
                            nota = f"similar: {real_col}"
                            break
                has_issue = True

            symbol = "OK" if status == "OK" else "XX"
            extra = f" -> {nota}" if nota else ""
            print(f"  [{symbol}] {col}{extra}")

            results.append({
                "select": select_name,
                "database": db,
                "table": tbl,
                "column": col,
                "status": status,
                "nota": nota
            })

        # Mostrar colunas extras disponiveis que podem ser uteis
        expected_set = {c.strip().lower() for c in expected_cols}
        extras = real_cols - expected_set
        if extras and len(extras) <= 30:
            print(f"\n  Colunas disponiveis nao mapeadas ({len(extras)}):")
            for ec in sorted(extras):
                print(f"    + {ec}")
        elif extras:
            print(f"\n  {len(extras)} colunas adicionais disponiveis (tabela grande)")

        if has_issue:
            tables_fail += 1
        else:
            tables_ok += 1

    # === RESUMO ===
    print("\n" + "=" * 80)
    print("RESUMO")
    print("=" * 80)
    total = tables_ok + tables_fail + tables_not_found
    print(f"  Tabelas validadas OK:      {tables_ok}/{total}")
    print(f"  Tabelas com problemas:     {tables_fail}/{total}")
    print(f"  Tabelas nao encontradas:   {tables_not_found}/{total}")

    df_results = pd.DataFrame(results)
    nao_existe = df_results[df_results["status"] == "NAO_EXISTE"]
    tabela_nf = df_results[df_results["status"] == "TABELA_NAO_ENCONTRADA"]

    if len(nao_existe) > 0:
        print(f"\n  Colunas NAO_EXISTE: {len(nao_existe)}")
        for _, row in nao_existe.iterrows():
            nota = f" ({row['nota']})" if row["nota"] else ""
            print(f"    - {row['database']}.{row['table']}.{row['column']}{nota}")

    if len(tabela_nf) > 0:
        print(f"\n  Tabelas NAO ENCONTRADAS:")
        seen = set()
        for _, row in tabela_nf.iterrows():
            key = f"{row['database']}.{row['table']}"
            if key not in seen:
                print(f"    - {key}")
                seen.add(key)

    # Salvar CSV
    csv_path = "output/validacao_bronze_colunas.csv"
    df_results.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"\n  Relatorio CSV: {csv_path}")
    print("=" * 80)


if __name__ == "__main__":
    main()
