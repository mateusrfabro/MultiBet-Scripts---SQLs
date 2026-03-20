WITH u AS (
  SELECT
    user_id,
    user_ext_id,
    crm_brand_id,
    core_registration_date AS reg_ts
  FROM `{{PROJECT}}.{{DATASET}}.j_user`
  WHERE COALESCE(core_is_test_account, FALSE) = FALSE
    AND core_registration_date IS NOT NULL
    AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), core_registration_date, HOUR) < 48
),
brand AS (
  SELECT label_id, crm_brand_id, crm_brand_name
  FROM `{{PROJECT}}.{{DATASET}}.dm_brand`
  WHERE COALESCE(is_deleted, FALSE) = FALSE
),
casino_bet AS (
  SELECT
    user_id,
    event_time AS bet_ts,
    bcat_last_bet_amount_base_cur AS bet_base_cur
  FROM `{{PROJECT}}.{{DATASET}}.tr_casino_bet`
  WHERE event_time >= TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY), DAY)
    AND event_time <  CURRENT_TIMESTAMP()
    AND COALESCE(casino_is_rollback, FALSE) = FALSE
    AND COALESCE(core_is_exclude_from_report, FALSE) = FALSE
    AND COALESCE(casino_is_free_bet, FALSE) = FALSE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingest_time DESC) = 1
),
casino_win AS (
  SELECT
    user_id,
    event_time AS win_ts,
    bcat_last_win_amount_base_cur AS win_base_cur
  FROM `{{PROJECT}}.{{DATASET}}.tr_casino_win`
  WHERE event_time >= TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY), DAY)
    AND event_time <  CURRENT_TIMESTAMP()
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
  WHERE event_time >= TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY), DAY)
    AND event_time <  CURRENT_TIMESTAMP()
    AND COALESCE(sport_is_rollback, FALSE) = FALSE
    AND COALESCE(core_is_exclude_from_report, FALSE) = FALSE
    AND COALESCE(winb_is_real_money_bet, TRUE) = TRUE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingest_time DESC) = 1
),
ggr_48h AS (
  SELECT
    n.user_id,
    COALESCE(SUM(cb.bet_base_cur), 0) + COALESCE(SUM(s.sport_bet_base_cur), 0)
    - (COALESCE(SUM(cw.win_base_cur), 0) + COALESCE(SUM(s.sport_win_base_cur), 0)) AS ggr_48h_base_cur
  FROM u n
  LEFT JOIN casino_bet cb
    ON cb.user_id = n.user_id
   AND cb.bet_ts >= n.reg_ts
   AND cb.bet_ts <  TIMESTAMP_ADD(n.reg_ts, INTERVAL 48 HOUR)
  LEFT JOIN casino_win cw
    ON cw.user_id = n.user_id
   AND cw.win_ts >= n.reg_ts
   AND cw.win_ts <  TIMESTAMP_ADD(n.reg_ts, INTERVAL 48 HOUR)
  LEFT JOIN sport s
    ON s.user_id = n.user_id
   AND s.sport_ts >= n.reg_ts
   AND s.sport_ts <  TIMESTAMP_ADD(n.reg_ts, INTERVAL 48 HOUR)
  GROUP BY 1
)
SELECT
  br.label_id,
  u.user_id,
  u.user_ext_id,
  u.crm_brand_id,
  br.crm_brand_name,
  TRUE AS flag,
  -5 AS score,
  'POTENCIAL_ABUSER' AS tag,
  CURRENT_DATE() AS snapshot_date,
  CURRENT_TIMESTAMP() AS computed_at,
  g.ggr_48h_base_cur
FROM u
LEFT JOIN brand br USING (crm_brand_id)
JOIN ggr_48h g USING (user_id)
WHERE g.ggr_48h_base_cur < 0;