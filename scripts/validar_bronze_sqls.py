"""
Validacao Bronze v2 — Roda cada SELECT corrigido no Athena com LIMIT 5.
Confirma que as queries funcionam de verdade.
Gera relatorio + changelog comparativo com o documento original do Mauro.
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
# Cada SELECT do documento CORRIGIDO v2 — com LIMIT 5
# ============================================================================

BRONZE_SQLS = {
    "1. fact_registrations (ecr_ec2.tbl_ecr)": {
        "database": "ecr_ec2",
        "sql": """
SELECT
    e.c_ecr_id, e.c_external_id, e.c_tracker_id, e.c_affiliate_id,
    e.c_jurisdiction, e.c_language, e.c_ecr_status, e.c_signup_time,
    CAST(e.c_signup_time AS DATE) AS dt
FROM ecr_ec2.tbl_ecr e
JOIN ecr_ec2.tbl_ecr_flags f ON e.c_ecr_id = f.c_ecr_id
WHERE f.c_test_user = false
LIMIT 5"""
    },
    "2. fact_ftd_deposits (cashier_ec2.tbl_cashier_deposit)": {
        "database": "cashier_ec2",
        "sql": """
SELECT
    d.c_ecr_id, d.c_txn_id, d.c_initial_amount, d.c_created_time,
    d.c_txn_status, CAST(d.c_created_time AS DATE) AS dt
FROM cashier_ec2.tbl_cashier_deposit d
JOIN ecr_ec2.tbl_ecr_flags f ON d.c_ecr_id = f.c_ecr_id
WHERE d.c_txn_status = 'txn_confirmed_success'
  AND f.c_test_user = false
LIMIT 5"""
    },
    "3. dim_marketing (ecr_ec2.tbl_ecr_banner)": {
        "database": "ecr_ec2",
        "sql": """
SELECT
    b.c_ecr_id, b.c_tracker_id, b.c_affiliate_id, b.c_affiliate_name,
    b.c_banner_id, b.c_reference_url, b.c_custom1, b.c_custom2,
    b.c_custom3, b.c_custom4, b.c_created_time
FROM ecr_ec2.tbl_ecr_banner b
WHERE b.c_tracker_id IS NOT NULL
LIMIT 5"""
    },
    "4. fact_gaming_activity (fund_ec2.tbl_real_fund_txn)": {
        "database": "fund_ec2",
        "sql": """
SELECT
    t.c_ecr_id, t.c_txn_id, t.c_txn_type, t.c_txn_status,
    t.c_amount_in_ecr_ccy, t.c_op_type, t.c_game_id, t.c_sub_vendor_id,
    t.c_product_id, t.c_game_category, t.c_start_time,
    CAST(t.c_start_time AS DATE) AS dt
FROM fund_ec2.tbl_real_fund_txn t
JOIN ecr_ec2.tbl_ecr_flags f ON t.c_ecr_id = f.c_ecr_id
WHERE f.c_test_user = false
LIMIT 5"""
    },
    "5. Sub-Fund Real (fund_ec2.tbl_realcash_sub_fund_txn)": {
        "database": "fund_ec2",
        "sql": """
SELECT s.c_fund_txn_id, s.c_ecr_id, s.c_amount_in_house_ccy
FROM fund_ec2.tbl_realcash_sub_fund_txn s
LIMIT 5"""
    },
    "6. Sub-Fund Bonus (fund_ec2.tbl_bonus_sub_fund_txn)": {
        "database": "fund_ec2",
        "sql": """
SELECT
    b.c_fund_txn_id, b.c_ecr_id,
    b.c_drp_amount_in_house_ccy, b.c_crp_amount_in_house_ccy,
    b.c_wrp_amount_in_house_ccy, b.c_rrp_amount_in_house_ccy
FROM fund_ec2.tbl_bonus_sub_fund_txn b
LIMIT 5"""
    },
    "7. Tipos Transacao (fund_ec2.tbl_real_fund_txn_type_mst)": {
        "database": "fund_ec2",
        "sql": """
SELECT
    m.c_txn_type, m.c_internal_description, m.c_op_type,
    m.c_is_gaming_txn, m.c_is_cancel_txn, m.c_is_free_spin_txn,
    m.c_is_refund_txn_type, m.c_is_settlement_txn_type,
    m.c_product_id, m.c_txn_identifier_key
FROM fund_ec2.tbl_real_fund_txn_type_mst m
LIMIT 5"""
    },
    "8. Casino fund_ec2 (tbl_real_fund_txn filtro casino)": {
        "database": "fund_ec2",
        "sql": """
SELECT
    t.c_ecr_id, t.c_txn_id, t.c_game_id, t.c_sub_vendor_id,
    t.c_game_category, t.c_amount_in_ecr_ccy, t.c_txn_type,
    t.c_txn_status, t.c_op_type, t.c_start_time,
    CAST(t.c_start_time AS DATE) AS dt
FROM fund_ec2.tbl_real_fund_txn t
JOIN fund_ec2.tbl_real_fund_txn_type_mst m ON t.c_txn_type = m.c_txn_type
JOIN ecr_ec2.tbl_ecr_flags f ON t.c_ecr_id = f.c_ecr_id
WHERE m.c_is_gaming_txn = 'Y'
  AND m.c_txn_identifier_key LIKE '%CASINO%'
  AND f.c_test_user = false
LIMIT 5"""
    },
    "9. Catalogo Jogos (vendor_ec2.tbl_vendor_games_mapping_mst)": {
        "database": "vendor_ec2",
        "sql": """
SELECT
    g.c_game_id, g.c_game_desc, g.c_vendor_id, g.c_sub_vendor_id,
    g.c_product_id, g.c_game_category_desc, g.c_game_type_id,
    g.c_game_type_desc, g.c_status, g.c_has_jackpot,
    g.c_free_spin_game, g.c_feature_trigger_game, g.c_game_technology,
    g.c_updated_time
FROM vendor_ec2.tbl_vendor_games_mapping_mst g
WHERE g.c_product_id = 'CASINO'
LIMIT 5"""
    },
    "10. Sports Bets (vendor_ec2.tbl_sports_book_bets_info)": {
        "database": "vendor_ec2",
        "sql": """
SELECT
    i.c_bet_id, i.c_bet_slip_id, i.c_customer_id, i.c_total_stake,
    i.c_total_return, i.c_total_odds, i.c_bonus_amount, i.c_is_free,
    i.c_is_live, i.c_bet_type, i.c_bet_state, i.c_bet_slip_state,
    i.c_transaction_type, i.c_transaction_id, i.c_bet_closure_time,
    i.c_created_time, CAST(i.c_bet_closure_time AS DATE) AS dt
FROM vendor_ec2.tbl_sports_book_bets_info i
LIMIT 5"""
    },
    "10b. Sports Details (vendor_ec2.tbl_sports_book_bet_details)": {
        "database": "vendor_ec2",
        "sql": """
SELECT
    d.c_customer_id, d.c_bet_slip_id, d.c_transaction_id, d.c_bet_id,
    d.c_sport_type_name, d.c_sport_id, d.c_event_name, d.c_market_name,
    d.c_selection_name, d.c_odds, d.c_leg_status, d.c_tournament_name,
    d.c_is_live, d.c_created_time, d.c_leg_settlement_date
FROM vendor_ec2.tbl_sports_book_bet_details d
LIMIT 5"""
    },
    "10c. Sports Txn (vendor_ec2.tbl_sports_book_info)": {
        "database": "vendor_ec2",
        "sql": """
SELECT
    s.c_customer_id, s.c_bet_slip_id, s.c_transaction_id, s.c_amount,
    s.c_operation_type, s.c_bet_slip_state, s.c_vendor_id, s.c_currency
FROM vendor_ec2.tbl_sports_book_info s
LIMIT 5"""
    },
    "11. Gaming Sessions (bireports_ec2.tbl_ecr_gaming_sessions)": {
        "database": "bireports_ec2",
        "sql": """
SELECT
    gs.c_ecr_id, gs.c_game_id, gs.c_session_start_time,
    gs.c_session_end_time, gs.c_session_length_in_sec,
    gs.c_game_played_count, gs.c_product_id, gs.c_vendor_id,
    gs.c_game_category, CAST(gs.c_session_start_time AS DATE) AS dt
FROM bireports_ec2.tbl_ecr_gaming_sessions gs
WHERE gs.c_product_id = 'CASINO'
LIMIT 5"""
    },
    "12. Cashout (cashier_ec2.tbl_cashier_cashout)": {
        "database": "cashier_ec2",
        "sql": """
SELECT
    c.c_ecr_id, c.c_txn_id, c.c_initial_amount, c.c_created_time,
    c.c_txn_status, CAST(c.c_created_time AS DATE) AS dt
FROM cashier_ec2.tbl_cashier_cashout c
JOIN ecr_ec2.tbl_ecr_flags f ON c.c_ecr_id = f.c_ecr_id
WHERE c.c_txn_status = 'co_success'
  AND f.c_test_user = false
LIMIT 5"""
    },
    "13. Daily Payment Summary (cashier_ec2)": {
        "database": "cashier_ec2",
        "sql": """
SELECT
    s.c_ecr_id, s.c_created_date, s.c_deposit_amount,
    s.c_deposit_amount_inhouse, s.c_deposit_count,
    s.c_success_cashout_amount, s.c_success_cashout_amount_inhouse,
    s.c_success_cashout_count, s.c_cb_amount, s.c_cb_count,
    s.c_option, s.c_provider
FROM cashier_ec2.tbl_cashier_ecr_daily_payment_summary s
LIMIT 5"""
    },
    "14. Instrumentos (cashier_ec2.tbl_instrument)": {
        "database": "cashier_ec2",
        "sql": """
SELECT
    ins.c_ecr_id, ins.c_instrument, ins.c_first_part, ins.c_last_part,
    ins.c_status, ins.c_use_in_deposit, ins.c_use_in_cashout,
    ins.c_last_deposit_date, ins.c_deposit_success, ins.c_deposit_attempted,
    ins.c_payout_success, ins.c_payout_attempted, ins.c_chargeback
FROM cashier_ec2.tbl_instrument ins
LIMIT 5"""
    },
    "15. Bonus Details (bonus_ec2.tbl_ecr_bonus_details)": {
        "database": "bonus_ec2",
        "sql": """
SELECT
    bd.c_ecr_id, bd.c_bonus_id, bd.c_ecr_bonus_id, bd.c_issue_type,
    bd.c_criteria_type, bd.c_bonus_status, bd.c_is_freebet,
    bd.c_drp_in_ecr_ccy, bd.c_crp_in_ecr_ccy, bd.c_wrp_in_ecr_ccy,
    bd.c_rrp_in_ecr_ccy, bd.c_wager_amount, bd.c_wager_amount_in_inhouse_ccy,
    bd.c_created_time, bd.c_bonus_expired_date, bd.c_claimed_date,
    bd.c_free_spin_used, bd.c_vendor_id
FROM bonus_ec2.tbl_ecr_bonus_details bd
LIMIT 5"""
    },
    "16. Bonus Activity (ps_bi.fct_bonus_activity_daily)": {
        "database": "ps_bi",
        "sql": """
SELECT
    ba.activity_date, ba.player_id, ba.bonus_id, ba.product_id,
    ba.label_id, ba.amount_issued_local, ba.amount_dropped_local,
    ba.amount_expired_local, ba.amount_offered_local
FROM ps_bi.fct_bonus_activity_daily ba
LIMIT 5"""
    },
    "17. Fraud/Risk (risk_ec2.tbl_ecr_ccf_score)": {
        "database": "risk_ec2",
        "sql": """
SELECT
    r.c_ecr_id, r.c_ccf_score, r.c_bet_factor, r.c_ccf_timestamp,
    r.c_created_time, r.c_updated_time
FROM risk_ec2.tbl_ecr_ccf_score r
LIMIT 5"""
    },
    "18. KYC (ecr_ec2.tbl_ecr_kyc_level)": {
        "database": "ecr_ec2",
        "sql": """
SELECT
    k.c_ecr_id, k.c_level, k.c_desc, k.c_grace_action_status,
    k.c_kyc_limit_nearly_reached, k.c_kyc_reminder_count, k.c_updated_time
FROM ecr_ec2.tbl_ecr_kyc_level k
LIMIT 5"""
    },
    "19. dim_game (ps_bi)": {
        "database": "ps_bi",
        "sql": """
SELECT
    dg.game_id, dg.game_desc, dg.vendor_id, dg.product_id,
    dg.game_category, dg.game_category_desc, dg.game_type_id,
    dg.game_type_desc, dg.status, dg.updated_time
FROM ps_bi.dim_game dg
LIMIT 5"""
    },
    "20. Game Images (bireports_ec2.tbl_vendor_games_mapping_data)": {
        "database": "bireports_ec2",
        "sql": """
SELECT
    gd.c_game_id, gd.c_game_desc, gd.c_vendor_id,
    gd.c_game_category_desc, gd.c_product_id, gd.c_status
FROM bireports_ec2.tbl_vendor_games_mapping_data gd
LIMIT 5"""
    },
    "25. Flags (ecr_ec2.tbl_ecr_flags)": {
        "database": "ecr_ec2",
        "sql": """
SELECT
    f.c_ecr_id, f.c_test_user, f.c_referral_ban, f.c_withdrawl_allowed,
    f.c_two_factor_auth_enabled, f.c_hide_username_feed
FROM ecr_ec2.tbl_ecr_flags f
LIMIT 5"""
    },
    "26. BI Summary (bireports_ec2.tbl_ecr_wise_daily_bi_summary)": {
        "database": "bireports_ec2",
        "sql": """
SELECT
    bs.c_ecr_id, bs.c_created_date,
    bs.c_casino_realcash_bet_amount, bs.c_casino_realcash_win_amount,
    bs.c_casino_bonus_bet_amount, bs.c_casino_bonus_win_amount,
    bs.c_sb_realcash_bet_amount, bs.c_sb_realcash_win_amount,
    bs.c_sb_bonus_bet_amount, bs.c_sb_bonus_win_amount,
    bs.c_deposit_amount, bs.c_withdrawal_amount, bs.c_login_count
FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary bs
LIMIT 5"""
    },
    "27. dim_user (ps_bi)": {
        "database": "ps_bi",
        "sql": """
SELECT
    du.ecr_id, du.external_id, du.registration_date,
    du.country_code, du.first_deposit_date, du.first_deposit_amount_local
FROM ps_bi.dim_user du
LIMIT 5"""
    },
    "28. BI ECR Master (bireports_ec2.tbl_ecr)": {
        "database": "bireports_ec2",
        "sql": """
SELECT
    be.c_ecr_id, be.c_external_id, be.c_test_user, be.c_last_login_time
FROM bireports_ec2.tbl_ecr be
LIMIT 5"""
    },
}


def main():
    results = []
    ok_count = 0
    fail_count = 0

    print("=" * 80)
    print("VALIDACAO BRONZE v2 — SQLs no Athena Real")
    print(f"Data: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"Total de queries: {len(BRONZE_SQLS)}")
    print("=" * 80)

    for name, config in BRONZE_SQLS.items():
        db = config["database"]
        sql = config["sql"].strip()
        print(f"\n--- {name} ---")

        try:
            df = query_athena(sql, database=db)
            rows = len(df)
            cols = list(df.columns)
            print(f"  [OK] {rows} linhas, {len(cols)} colunas: {cols}")
            results.append({
                "select": name,
                "status": "OK",
                "rows": rows,
                "columns": len(cols),
                "column_names": ", ".join(cols),
                "error": ""
            })
            ok_count += 1
        except Exception as e:
            err_msg = str(e)[:200]
            print(f"  [ERRO] {err_msg}")
            results.append({
                "select": name,
                "status": "ERRO",
                "rows": 0,
                "columns": 0,
                "column_names": "",
                "error": err_msg
            })
            fail_count += 1

    # Resumo
    print("\n" + "=" * 80)
    print("RESUMO")
    print("=" * 80)
    print(f"  OK:    {ok_count}/{len(BRONZE_SQLS)}")
    print(f"  ERRO:  {fail_count}/{len(BRONZE_SQLS)}")

    if fail_count > 0:
        print(f"\n  Queries com erro:")
        for r in results:
            if r["status"] == "ERRO":
                print(f"    - {r['select']}: {r['error']}")

    # Salvar CSV
    df_results = pd.DataFrame(results)
    csv_path = "output/validacao_bronze_sqls_v2.csv"
    df_results.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"\n  CSV: {csv_path}")
    print("=" * 80)


if __name__ == "__main__":
    main()
