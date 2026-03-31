"""
Diagnostico Bronze DDL — Verifica colunas atuais no Super Nova DB
================================================================
Compara as colunas existentes nas tabelas bronze_ do Super Nova DB
com as colunas esperadas do documento v2 (bronze_selects_kpis_CORRIGIDO_v2.md).

Saida: console + CSV com status OK / FALTANDO / EXTRA por tabela.

USO: python scripts/diagnostico_bronze_ddl.py
"""

import sys
import logging
from datetime import datetime

sys.path.insert(0, ".")
from db.supernova import execute_supernova

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)

# ===========================================================================
# Colunas ESPERADAS por tabela (documento v2 corrigido)
# ===========================================================================

V2_EXPECTED = {
    "bronze_ecr": [
        "c_ecr_id", "c_external_id", "c_tracker_id", "c_affiliate_id",
        "c_jurisdiction", "c_language", "c_ecr_status", "c_signup_time", "dt",
    ],
    "bronze_cashier_deposit": [
        "c_ecr_id", "c_txn_id", "c_initial_amount", "c_created_time",
        "c_txn_status", "dt",
    ],
    "bronze_cashier_cashout": [
        "c_ecr_id", "c_txn_id", "c_initial_amount", "c_created_time",
        "c_txn_status", "dt",
    ],
    "bronze_real_fund_txn": [
        "c_ecr_id", "c_txn_id", "c_txn_type", "c_txn_status",
        "c_amount_in_ecr_ccy", "c_op_type", "c_game_id", "c_sub_vendor_id",
        "c_product_id", "c_game_category", "c_start_time", "dt",
    ],
    "bronze_realcash_sub_fund": [
        "c_fund_txn_id", "c_ecr_id", "c_amount_in_house_ccy",
    ],
    "bronze_bonus_sub_fund": [
        "c_fund_txn_id", "c_ecr_id",
        "c_drp_amount_in_house_ccy", "c_crp_amount_in_house_ccy",
        "c_wrp_amount_in_house_ccy", "c_rrp_amount_in_house_ccy",
    ],
    "bronze_fund_txn_type_mst": [
        "c_txn_type", "c_internal_description", "c_op_type",
        "c_is_gaming_txn", "c_is_cancel_txn", "c_is_free_spin_txn",
        "c_is_refund_txn_type", "c_is_settlement_txn_type",
        "c_product_id", "c_txn_identifier_key",
    ],
    "bronze_ecr_flags": [
        "c_ecr_id", "c_test_user", "c_referral_ban", "c_withdrawl_allowed",
        "c_two_factor_auth_enabled", "c_hide_username_feed",
    ],
    "bronze_ecr_banner": [
        "c_ecr_id", "c_tracker_id", "c_affiliate_id", "c_affiliate_name",
        "c_banner_id", "c_reference_url", "c_custom1", "c_custom2",
        "c_custom3", "c_custom4", "c_created_time",
    ],
    "bronze_games_catalog": [
        "c_game_id", "c_game_desc", "c_vendor_id", "c_sub_vendor_id",
        "c_product_id", "c_game_category_desc", "c_game_type_id",
        "c_game_type_desc", "c_status", "c_has_jackpot", "c_free_spin_game",
        "c_feature_trigger_game", "c_game_technology", "c_updated_time",
    ],
    "bronze_sports_bets": [
        "c_bet_id", "c_bet_slip_id", "c_customer_id", "c_total_stake",
        "c_total_return", "c_total_odds", "c_bonus_amount", "c_is_free",
        "c_is_live", "c_bet_type", "c_bet_state", "c_bet_slip_state",
        "c_transaction_type", "c_transaction_id", "c_bet_closure_time",
        "c_created_time", "dt",
    ],
    "bronze_gaming_sessions": [
        "c_ecr_id", "c_game_id", "c_session_start_time", "c_session_end_time",
        "c_session_length_in_sec", "c_game_played_count", "c_product_id",
        "c_vendor_id", "c_game_category", "dt",
    ],
    "bronze_daily_payment_summary": [
        "c_ecr_id", "c_created_date", "c_deposit_amount",
        "c_deposit_amount_inhouse", "c_deposit_count",
        "c_success_cashout_amount", "c_success_cashout_amount_inhouse",
        "c_success_cashout_count", "c_cb_amount", "c_cb_count",
        "c_option", "c_provider",
    ],
    "bronze_instrument": [
        "c_ecr_id", "c_instrument", "c_first_part", "c_last_part",
        "c_status", "c_use_in_deposit", "c_use_in_cashout",
        "c_last_deposit_date", "c_deposit_success", "c_deposit_attempted",
        "c_payout_success", "c_payout_attempted", "c_chargeback",
    ],
    "bronze_bonus_details": [
        "c_ecr_id", "c_bonus_id", "c_ecr_bonus_id", "c_issue_type",
        "c_criteria_type", "c_bonus_status", "c_is_freebet",
        "c_drp_in_ecr_ccy", "c_crp_in_ecr_ccy", "c_wrp_in_ecr_ccy",
        "c_rrp_in_ecr_ccy", "c_wager_amount", "c_wager_amount_in_inhouse_ccy",
        "c_created_time", "c_bonus_expired_date", "c_claimed_date",
        "c_free_spin_used", "c_vendor_id",
    ],
    "bronze_ccf_score": [
        "c_ecr_id", "c_ccf_score", "c_bet_factor", "c_ccf_timestamp",
        "c_created_time", "c_updated_time",
    ],
    "bronze_kyc_level": [
        "c_ecr_id", "c_level", "c_desc", "c_grace_action_status",
        "c_kyc_limit_nearly_reached", "c_kyc_reminder_count", "c_updated_time",
    ],
    "bronze_games_mapping_data": [
        "c_game_id", "c_game_desc", "c_vendor_id",
        "c_game_category_desc", "c_product_id", "c_status",
    ],
}

# Colunas que NAO devem existir (nomes v1 errados)
V1_WRONG_COLUMNS = {
    "bronze_ecr": ["c_country_code"],
    "bronze_real_fund_txn": [
        "c_tracker_id", "c_vendor_id", "c_round_id",
        "c_confirmed_amount_in_inhouse_ccy",
    ],
    "bronze_daily_payment_summary": [
        "c_date", "c_deposit_amount_brl", "c_withdrawal_amount_brl",
        "c_withdrawal_count", "c_net_deposit_brl", "c_avg_deposit_ticket",
    ],
    "bronze_gaming_sessions": ["c_session_duration_sec", "c_round_count"],
    "bronze_sports_bets": ["c_sport_name"],
    "bronze_bonus_details": [
        "c_bonus_amount_brl", "c_bonus_type", "c_rollover_requirement",
        "c_rollover_completed", "c_issued_date", "c_expiry_date",
    ],
    "bronze_ccf_score": [
        "c_risk_level", "c_calculated_date",
        "c_fraud_indicators_json", "c_aml_flags_json",
    ],
    "bronze_kyc_level": [
        "c_kyc_level", "c_verification_status", "c_updated_date",
        "c_documents_verified_count", "c_verification_time_hours",
    ],
    "bronze_games_catalog": [
        "c_game_name", "c_game_category", "c_has_free_spins",
        "c_technology", "c_feature_trigger", "c_updated_dt",
    ],
    "bronze_fund_txn_type_mst": ["c_txn_type_name"],
    "bronze_games_mapping_data": ["c_game_name"],
}


def get_pg_columns(table_name: str) -> set:
    """Retorna set de colunas reais da tabela no Super Nova DB."""
    sql = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'multibet'
      AND table_name = %s
    ORDER BY ordinal_position
    """
    try:
        rows = execute_supernova(sql, params=(table_name,), fetch=True)
        return {r[0].lower() for r in rows}
    except Exception as e:
        log.error(f"  ERRO ao acessar {table_name}: {e}")
        return set()


def get_row_count(table_name: str) -> int:
    """Retorna contagem de linhas."""
    try:
        sql = f"SELECT COUNT(*) FROM multibet.{table_name}"
        rows = execute_supernova(sql, fetch=True)
        return rows[0][0] if rows else 0
    except Exception:
        return -1


def main():
    print("=" * 90)
    print("DIAGNOSTICO BRONZE DDL — Super Nova DB vs Documento v2")
    print(f"Data: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 90)

    results = []
    total_ok = 0
    total_missing = 0
    total_wrong = 0

    for table_name, expected_cols in V2_EXPECTED.items():
        pg_cols = get_pg_columns(table_name)
        row_count = get_row_count(table_name)

        print(f"\n{'─' * 70}")
        status_icon = "COM DADOS" if row_count > 0 else "VAZIA" if row_count == 0 else "ERRO"
        print(f"  {table_name} ({status_icon}, {row_count:,} linhas)")
        print(f"{'─' * 70}")

        if not pg_cols:
            print(f"  >>> TABELA NAO ENCONTRADA no banco <<<")
            for col in expected_cols:
                results.append((table_name, col, "TABELA_NAO_EXISTE", row_count))
            continue

        # Verificar colunas v2 esperadas
        missing = []
        ok = []
        for col in expected_cols:
            if col.lower() in pg_cols:
                ok.append(col)
                print(f"  [OK] {col}")
                total_ok += 1
            else:
                missing.append(col)
                print(f"  [XX] {col} — FALTANDO")
                total_missing += 1
            results.append((table_name, col, "OK" if col.lower() in pg_cols else "FALTANDO", row_count))

        # Verificar colunas v1 erradas que NAO devem existir
        wrong_cols = V1_WRONG_COLUMNS.get(table_name, [])
        for wcol in wrong_cols:
            if wcol.lower() in pg_cols:
                print(f"  [!!] {wcol} — COLUNA v1 ERRADA AINDA PRESENTE")
                total_wrong += 1
                results.append((table_name, wcol, "V1_ERRADA_PRESENTE", row_count))

        # Colunas extras no banco (nao pedidas pelo v2)
        expected_set = {c.lower() for c in expected_cols}
        extras = pg_cols - expected_set - {"loaded_at", "id", "refreshed_at", "batch_date"}
        if extras:
            print(f"\n  Colunas extras no PG (nao no v2): {len(extras)}")
            for ec in sorted(extras):
                print(f"    + {ec}")

    # === RESUMO ===
    print("\n" + "=" * 90)
    print("RESUMO")
    print("=" * 90)
    print(f"  Colunas v2 OK:           {total_ok}")
    print(f"  Colunas v2 FALTANDO:     {total_missing}")
    print(f"  Colunas v1 erradas:      {total_wrong}")
    print(f"  Total verificacoes:      {total_ok + total_missing}")

    if total_missing > 0:
        print(f"\n  COLUNAS FALTANTES (precisam ALTER ou DDL corrigida):")
        for r in results:
            if r[2] == "FALTANDO":
                print(f"    - {r[0]}.{r[1]}")

    if total_wrong > 0:
        print(f"\n  COLUNAS v1 ERRADAS (precisam DROP/rename):")
        for r in results:
            if r[2] == "V1_ERRADA_PRESENTE":
                print(f"    - {r[0]}.{r[1]}")

    # Verificar tabelas novas (nao existem)
    new_tables = ["bronze_sports_bet_details", "bronze_dim_game", "bronze_dim_user"]
    print(f"\n  TABELAS NOVAS (precisam CREATE):")
    for nt in new_tables:
        pg_cols = get_pg_columns(nt)
        if pg_cols:
            print(f"    - {nt}: JA EXISTE ({len(pg_cols)} colunas)")
        else:
            print(f"    - {nt}: NAO EXISTE (criar)")

    # Salvar CSV
    try:
        import pandas as pd
        df = pd.DataFrame(results, columns=["table", "column", "status", "row_count"])
        csv_path = "output/diagnostico_bronze_ddl.csv"
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        print(f"\n  CSV salvo: {csv_path}")
    except ImportError:
        print("\n  (pandas nao disponivel — CSV nao salvo)")

    print("=" * 90)


if __name__ == "__main__":
    main()
