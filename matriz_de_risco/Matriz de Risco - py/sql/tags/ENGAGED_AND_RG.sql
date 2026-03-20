WITH params AS (
  SELECT
    TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY), DAY) AS start_30d_ts,
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
login_raw AS (
  SELECT
    user_id,
    event_time,
    event_id
  FROM `{{PROJECT}}.{{DATASET}}.tr_login`
  WHERE event_time >= (SELECT start_30d_ts FROM params)
    AND event_time <  (SELECT end_ts FROM params)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingest_time DESC) = 1
),
sessions_day AS (
  SELECT
    user_id,
    DATE(event_time) AS dt,
    COUNT(*) AS sessions_cnt
  FROM login_raw
  GROUP BY 1,2
),
per_user AS (
  SELECT
    user_id,
    COUNT(*) AS active_days_30d,
    AVG(sessions_cnt) AS avg_sessions_per_active_day_30d,
    MAX(sessions_cnt) AS max_sessions_in_day_30d
  FROM sessions_day
  GROUP BY 1
)
SELECT
  br.label_id,
  u.user_id,
  u.user_ext_id,
  u.crm_brand_id,
  br.crm_brand_name,
  TRUE AS flag,
  10 AS score,
  'ENGAGED_PLAYER' AS tag,
  CURRENT_DATE() AS snapshot_date,
  CURRENT_TIMESTAMP() AS computed_at,
  p.active_days_30d,
  p.avg_sessions_per_active_day_30d,
  p.max_sessions_in_day_30d
FROM u
LEFT JOIN brand br USING (crm_brand_id)
JOIN per_user p USING (user_id)
WHERE p.active_days_30d >= 5
  AND p.avg_sessions_per_active_day_30d BETWEEN 3 AND 10

UNION ALL

SELECT
  br.label_id,
  u.user_id,
  u.user_ext_id,
  u.crm_brand_id,
  br.crm_brand_name,
  TRUE AS flag,
  1 AS score,
  'RG_ALERT_PLAYER' AS tag,
  CURRENT_DATE() AS snapshot_date,
  CURRENT_TIMESTAMP() AS computed_at,
  p.active_days_30d,
  p.avg_sessions_per_active_day_30d,
  p.max_sessions_in_day_30d
FROM u
LEFT JOIN brand br USING (crm_brand_id)
JOIN per_user p USING (user_id)
WHERE p.max_sessions_in_day_30d >= 10;