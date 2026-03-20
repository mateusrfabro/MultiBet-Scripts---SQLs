WITH params AS (
  SELECT
    TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY), DAY) AS start_7d_ts,
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
eng_7d AS (
  SELECT DISTINCT user_id
  FROM `{{PROJECT}}.{{DATASET}}.j_engagements`
  WHERE create_date >= (SELECT start_7d_ts FROM params)
    AND create_date <  (SELECT end_ts FROM params)
    AND COALESCE(from_control_group, FALSE) = FALSE
),
dep_7d AS (
  SELECT user_id, event_time AS dep_ts
  FROM `{{PROJECT}}.{{DATASET}}.tr_acc_deposit_approved`
  WHERE event_time >= (SELECT start_7d_ts FROM params)
    AND event_time <  (SELECT end_ts FROM params)
    AND COALESCE(acc_is_rollback, FALSE) = FALSE
    AND COALESCE(core_is_exclude_from_report, FALSE) = FALSE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingest_time DESC) = 1
),
casino_bet_7d AS (
  SELECT user_id, event_time AS bet_ts
  FROM `{{PROJECT}}.{{DATASET}}.tr_casino_bet`
  WHERE event_time >= (SELECT start_7d_ts FROM params)
    AND event_time <  (SELECT end_ts FROM params)
    AND COALESCE(casino_is_rollback, FALSE) = FALSE
    AND COALESCE(core_is_exclude_from_report, FALSE) = FALSE
    AND COALESCE(casino_is_free_bet, FALSE) = FALSE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingest_time DESC) = 1
),
sport_7d AS (
  SELECT user_id, event_time AS sport_ts
  FROM `{{PROJECT}}.{{DATASET}}.tr_sport_bet_settled`
  WHERE event_time >= (SELECT start_7d_ts FROM params)
    AND event_time <  (SELECT end_ts FROM params)
    AND COALESCE(sport_is_rollback, FALSE) = FALSE
    AND COALESCE(core_is_exclude_from_report, FALSE) = FALSE
    AND COALESCE(winb_is_real_money_bet, TRUE) = TRUE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingest_time DESC) = 1
),
login_7d AS (
  SELECT user_id, event_time AS login_ts
  FROM `{{PROJECT}}.{{DATASET}}.tr_login`
  WHERE event_time >= (SELECT start_7d_ts FROM params)
    AND event_time <  (SELECT end_ts FROM params)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingest_time DESC) = 1
),
activity_7d AS (
  SELECT user_id, login_ts AS act_ts FROM login_7d
  UNION ALL SELECT user_id, bet_ts   AS act_ts FROM casino_bet_7d
  UNION ALL SELECT user_id, sport_ts AS act_ts FROM sport_7d
  UNION ALL SELECT user_id, dep_ts   AS act_ts FROM dep_7d
),
active_7d AS (
  SELECT DISTINCT user_id
  FROM activity_7d
)
SELECT
  br.label_id,
  u.user_id,
  u.user_ext_id,
  u.crm_brand_id,
  br.crm_brand_name,
  TRUE AS flag,
  10 AS score,
  'NON_PROMO_PLAYER' AS tag,
  CURRENT_DATE() AS snapshot_date,
  CURRENT_TIMESTAMP() AS computed_at
FROM u
LEFT JOIN brand br USING (crm_brand_id)
JOIN active_7d a USING (user_id)
LEFT JOIN eng_7d e USING (user_id)
WHERE e.user_id IS NULL;