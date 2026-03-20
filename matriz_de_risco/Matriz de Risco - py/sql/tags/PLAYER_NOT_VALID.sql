WITH params AS (
  SELECT
    TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY), DAY) AS start_ts,
    CURRENT_TIMESTAMP() AS end_ts
),
active_90d AS (
  SELECT DISTINCT user_id
  FROM `{{PROJECT}}.{{DATASET}}.tr_login`
  WHERE event_time >= (SELECT start_ts FROM params)
    AND event_time <  (SELECT end_ts FROM params)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingest_time DESC) = 1

  UNION DISTINCT
  SELECT DISTINCT user_id
  FROM `{{PROJECT}}.{{DATASET}}.tr_acc_deposit_approved`
  WHERE event_time >= (SELECT start_ts FROM params)
    AND event_time <  (SELECT end_ts FROM params)
    AND COALESCE(acc_is_rollback, FALSE) = FALSE
    AND COALESCE(core_is_exclude_from_report, FALSE) = FALSE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingest_time DESC) = 1

  UNION DISTINCT
  SELECT DISTINCT user_id
  FROM `{{PROJECT}}.{{DATASET}}.tr_casino_bet`
  WHERE event_time >= (SELECT start_ts FROM params)
    AND event_time <  (SELECT end_ts FROM params)
    AND COALESCE(casino_is_rollback, FALSE) = FALSE
    AND COALESCE(core_is_exclude_from_report, FALSE) = FALSE
    AND COALESCE(casino_is_free_bet, FALSE) = FALSE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingest_time DESC) = 1

  UNION DISTINCT
  SELECT DISTINCT user_id
  FROM `{{PROJECT}}.{{DATASET}}.tr_sport_bet_settled`
  WHERE event_time >= (SELECT start_ts FROM params)
    AND event_time <  (SELECT end_ts FROM params)
    AND COALESCE(sport_is_rollback, FALSE) = FALSE
    AND COALESCE(core_is_exclude_from_report, FALSE) = FALSE
    AND COALESCE(winb_is_real_money_bet, TRUE) = TRUE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingest_time DESC) = 1
),
u AS (
  SELECT
    user_id,
    user_ext_id,
    crm_brand_id,
    COALESCE(core_email_confirmed, FALSE) AS core_email_confirmed,
    COALESCE(core_phone_confirmed, FALSE) AS core_phone_confirmed,
    UPPER(COALESCE(user_email_status, '')) AS user_email_status_norm
  FROM `{{PROJECT}}.{{DATASET}}.j_user`
  WHERE COALESCE(core_is_test_account, FALSE) = FALSE
),
brand AS (
  SELECT label_id, crm_brand_id, crm_brand_name
  FROM `{{PROJECT}}.{{DATASET}}.dm_brand`
  WHERE COALESCE(is_deleted, FALSE) = FALSE
)
SELECT
  br.label_id,
  u.user_id,
  u.user_ext_id,
  u.crm_brand_id,
  br.crm_brand_name,
  TRUE AS flag,
  -20 AS score,
  'PLAYER_NOT_VALID' AS tag,
  CURRENT_DATE() AS snapshot_date,
  CURRENT_TIMESTAMP() AS computed_at,
  u.core_email_confirmed,
  u.core_phone_confirmed,
  u.user_email_status_norm AS user_email_status
FROM u
JOIN active_90d a
  ON a.user_id = u.user_id
LEFT JOIN brand br
  ON br.crm_brand_id = u.crm_brand_id
WHERE
  u.core_email_confirmed = FALSE
  OR u.core_phone_confirmed = FALSE
  OR u.user_email_status_norm != 'VERIFIED';