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
eng_latest AS (
  SELECT
    user_id,
    create_date AS eng_start_ts,
    COALESCE(expected_stop_date, create_date) AS eng_stop_ts
  FROM `{{PROJECT}}.{{DATASET}}.j_engagements`
  WHERE create_date >= (SELECT start_ts FROM params)
    AND create_date <  (SELECT end_ts FROM params)
    AND COALESCE(from_control_group, FALSE) = FALSE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY create_date DESC) = 1
),
login_dedup AS (
  SELECT
    user_id,
    event_time AS login_ts,
    event_id,
    ingest_time
  FROM `{{PROJECT}}.{{DATASET}}.tr_login`
  WHERE event_time >= TIMESTAMP_SUB((SELECT start_ts FROM params), INTERVAL 7 DAY)
    AND event_time <  (SELECT end_ts FROM params)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingest_time DESC) = 1
),
login_day AS (
  SELECT user_id, DATE(login_ts) AS dt, COUNT(*) AS sessions_cnt
  FROM login_dedup
  GROUP BY 1,2
),
calc AS (
  SELECT
    e.user_id,
    SUM(IF(ld.dt >= DATE(TIMESTAMP_SUB(e.eng_start_ts, INTERVAL 7 DAY)) AND ld.dt < DATE(e.eng_start_ts), ld.sessions_cnt, 0)) AS pre_sessions_7d,
    SUM(IF(ld.dt >= DATE(e.eng_start_ts) AND ld.dt <= DATE(e.eng_stop_ts), ld.sessions_cnt, 0)) AS during_sessions,
    SUM(IF(ld.dt > DATE(e.eng_stop_ts) AND ld.dt <= DATE(TIMESTAMP_ADD(e.eng_stop_ts, INTERVAL 7 DAY)), ld.sessions_cnt, 0)) AS post_sessions_7d
  FROM eng_latest e
  LEFT JOIN login_day ld
    ON ld.user_id = e.user_id
   AND ld.dt BETWEEN DATE(TIMESTAMP_SUB(e.eng_start_ts, INTERVAL 7 DAY)) AND DATE(TIMESTAMP_ADD(e.eng_stop_ts, INTERVAL 7 DAY))
  GROUP BY 1
)
SELECT
  br.label_id,
  u.user_id,
  u.user_ext_id,
  u.crm_brand_id,
  br.crm_brand_name,
  TRUE AS flag,
  30 AS score,
  'PLAYER_REENGAGED' AS tag,
  CURRENT_DATE() AS snapshot_date,
  CURRENT_TIMESTAMP() AS computed_at,
  c.pre_sessions_7d,
  c.during_sessions,
  c.post_sessions_7d
FROM u
LEFT JOIN brand br USING (crm_brand_id)
JOIN calc c USING (user_id)
WHERE c.pre_sessions_7d = 0
  AND c.during_sessions > 0
  AND c.post_sessions_7d > 0;