WITH params AS (
  SELECT
    TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{WINDOW_DAYS}} DAY), DAY) AS start_ts,
    CURRENT_TIMESTAMP() AS end_ts
),
u AS (
  SELECT user_id, user_ext_id, crm_brand_id
  FROM `{{PROJECT}}.{{DATASET}}.j_user`
  WHERE COALESCE(core_is_test_account, FALSE) = FALSE
),
brand AS (
  SELECT label_id, crm_brand_id, crm_brand_name
  FROM `{{PROJECT}}.{{DATASET}}.dm_brand`
  WHERE COALESCE(is_deleted, FALSE) = FALSE
),
casino_bet AS (
  SELECT user_id, event_time AS bet_ts, bcat_last_bet_amount_base_cur AS bet_base_cur
  FROM `{{PROJECT}}.{{DATASET}}.tr_casino_bet`
  WHERE event_time >= (SELECT start_ts FROM params)
    AND event_time <  (SELECT end_ts FROM params)
    AND COALESCE(casino_is_rollback, FALSE) = FALSE
    AND COALESCE(core_is_exclude_from_report, FALSE) = FALSE
    AND COALESCE(casino_is_free_bet, FALSE) = FALSE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingest_time DESC) = 1
),
casino_win AS (
  SELECT user_id, event_time AS win_ts, bcat_last_win_amount_base_cur AS win_base_cur
  FROM `{{PROJECT}}.{{DATASET}}.tr_casino_win`
  WHERE event_time >= (SELECT start_ts FROM params)
    AND event_time <  (SELECT end_ts FROM params)
    AND COALESCE(casino_is_rollback, FALSE) = FALSE
    AND COALESCE(core_is_exclude_from_report, FALSE) = FALSE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingest_time DESC) = 1
),
sport AS (
  SELECT
    user_id,
    event_time AS sport_ts,
    sport_last_bet_amount_real * user_to_label_cur_rate AS sport_bet_base_cur,
    sport_last_bet_win_amount_real * user_to_label_cur_rate AS sport_win_base_cur
  FROM `{{PROJECT}}.{{DATASET}}.tr_sport_bet_settled`
  WHERE event_time >= (SELECT start_ts FROM params)
    AND event_time <  (SELECT end_ts FROM params)
    AND COALESCE(sport_is_rollback, FALSE) = FALSE
    AND COALESCE(core_is_exclude_from_report, FALSE) = FALSE
    AND COALESCE(winb_is_real_money_bet, TRUE) = TRUE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingest_time DESC) = 1
),
bet_activity AS (
  SELECT user_id, bet_ts FROM casino_bet
  UNION ALL
  SELECT user_id, sport_ts AS bet_ts FROM sport
),
ggr AS (
  SELECT
    u.user_id,
    COALESCE(cb.bet_sum, 0) + COALESCE(sb.bet_sum, 0)
    - (COALESCE(cw.win_sum, 0) + COALESCE(sw.win_sum, 0)) AS ggr_base_cur,
    COALESCE(bd.bet_days, 0) AS bet_days
  FROM u
  LEFT JOIN (SELECT user_id, SUM(bet_base_cur) AS bet_sum FROM casino_bet GROUP BY 1) cb USING (user_id)
  LEFT JOIN (SELECT user_id, SUM(win_base_cur) AS win_sum FROM casino_win GROUP BY 1) cw USING (user_id)
  LEFT JOIN (SELECT user_id, SUM(sport_bet_base_cur) AS bet_sum FROM sport GROUP BY 1) sb USING (user_id)
  LEFT JOIN (SELECT user_id, SUM(sport_win_base_cur) AS win_sum FROM sport GROUP BY 1) sw USING (user_id)
  LEFT JOIN (SELECT user_id, COUNT(DISTINCT DATE(bet_ts)) AS bet_days FROM bet_activity GROUP BY 1) bd USING (user_id)
)
SELECT
  br.label_id,
  u.user_id,
  u.user_ext_id,
  u.crm_brand_id,
  br.crm_brand_name,
  TRUE AS flag,
  30 AS score,
  'VIP_WHALE_PLAYER' AS tag,
  CURRENT_DATE() AS snapshot_date,
  CURRENT_TIMESTAMP() AS computed_at,
  g.ggr_base_cur AS ggr_base_cur,
  g.bet_days AS bet_days
FROM u
LEFT JOIN brand br USING (crm_brand_id)
JOIN ggr g USING (user_id)
WHERE g.ggr_base_cur >= 15000
  AND g.bet_days >= 20;