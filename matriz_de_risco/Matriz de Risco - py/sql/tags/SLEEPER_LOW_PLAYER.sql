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
ach_dedup AS (
  SELECT
    user_id,
    event_time,
    event_id,
    ingest_time
  FROM `{{PROJECT}}.{{DATASET}}.tr_ach_achievement_completed`
  WHERE event_time >= (SELECT start_ts FROM params)
    AND event_time <  (SELECT end_ts FROM params)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingest_time DESC) = 1
),
last_ach AS (
  SELECT user_id, MAX(event_time) AS last_ach_ts
  FROM ach_dedup
  GROUP BY 1
),
activity AS (
  SELECT user_id, event_time AS act_ts
  FROM `{{PROJECT}}.{{DATASET}}.tr_login`
  WHERE event_time >= (SELECT start_ts FROM params) AND event_time < (SELECT end_ts FROM params)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingest_time DESC) = 1
  UNION ALL
  SELECT user_id, event_time AS act_ts
  FROM `{{PROJECT}}.{{DATASET}}.tr_casino_bet`
  WHERE event_time >= (SELECT start_ts FROM params) AND event_time < (SELECT end_ts FROM params)
    AND COALESCE(casino_is_rollback, FALSE) = FALSE
    AND COALESCE(core_is_exclude_from_report, FALSE) = FALSE
    AND COALESCE(casino_is_free_bet, FALSE) = FALSE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingest_time DESC) = 1
  UNION ALL
  SELECT user_id, event_time AS act_ts
  FROM `{{PROJECT}}.{{DATASET}}.tr_sport_bet_settled`
  WHERE event_time >= (SELECT start_ts FROM params) AND event_time < (SELECT end_ts FROM params)
    AND COALESCE(sport_is_rollback, FALSE) = FALSE
    AND COALESCE(core_is_exclude_from_report, FALSE) = FALSE
    AND COALESCE(winb_is_real_money_bet, TRUE) = TRUE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingest_time DESC) = 1
  UNION ALL
  SELECT user_id, event_time AS act_ts
  FROM `{{PROJECT}}.{{DATASET}}.tr_acc_deposit_approved`
  WHERE event_time >= (SELECT start_ts FROM params) AND event_time < (SELECT end_ts FROM params)
    AND COALESCE(acc_is_rollback, FALSE) = FALSE
    AND COALESCE(core_is_exclude_from_report, FALSE) = FALSE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingest_time DESC) = 1
),
flag AS (
  SELECT
    l.user_id,
    l.last_ach_ts,
    (
      TIMESTAMP_DIFF((SELECT end_ts FROM params), l.last_ach_ts, DAY) >= 14
      AND NOT EXISTS (
        SELECT 1
        FROM activity a
        WHERE a.user_id = l.user_id
          AND a.act_ts > l.last_ach_ts
          AND a.act_ts < TIMESTAMP_ADD(l.last_ach_ts, INTERVAL 14 DAY)
      )
    ) AS is_sleeper
  FROM last_ach l
)
SELECT
  br.label_id,
  u.user_id,
  u.user_ext_id,
  u.crm_brand_id,
  br.crm_brand_name,
  TRUE AS flag,
  5 AS score,
  'SLEEPER_LOW_PLAYER' AS tag,
  CURRENT_DATE() AS snapshot_date,
  CURRENT_TIMESTAMP() AS computed_at,
  f.last_ach_ts
FROM u
LEFT JOIN brand br USING (crm_brand_id)
JOIN flag f USING (user_id)
WHERE f.is_sleeper = TRUE;