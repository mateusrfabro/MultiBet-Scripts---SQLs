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
  SELECT user_id, DATE(event_time) AS dt, bcat_last_bet_amount_base_cur AS bet_base_cur
  FROM `{{PROJECT}}.{{DATASET}}.tr_casino_bet`
  WHERE event_time >= (SELECT start_ts FROM params) AND event_time < (SELECT end_ts FROM params)
    AND COALESCE(casino_is_rollback, FALSE) = FALSE
    AND COALESCE(core_is_exclude_from_report, FALSE) = FALSE
    AND COALESCE(casino_is_free_bet, FALSE) = FALSE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingest_time DESC) = 1
),
casino_win AS (
  SELECT user_id, DATE(event_time) AS dt, bcat_last_win_amount_base_cur AS win_base_cur
  FROM `{{PROJECT}}.{{DATASET}}.tr_casino_win`
  WHERE event_time >= (SELECT start_ts FROM params) AND event_time < (SELECT end_ts FROM params)
    AND COALESCE(casino_is_rollback, FALSE) = FALSE
    AND COALESCE(core_is_exclude_from_report, FALSE) = FALSE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingest_time DESC) = 1
),
sport AS (
  SELECT
    user_id,
    DATE(event_time) AS dt,
    sport_last_bet_amount_real * user_to_label_cur_rate AS sport_bet_base_cur,
    sport_last_bet_win_amount_real * user_to_label_cur_rate AS sport_win_base_cur
  FROM `{{PROJECT}}.{{DATASET}}.tr_sport_bet_settled`
  WHERE event_time >= (SELECT start_ts FROM params) AND event_time < (SELECT end_ts FROM params)
    AND COALESCE(sport_is_rollback, FALSE) = FALSE
    AND COALESCE(core_is_exclude_from_report, FALSE) = FALSE
    AND COALESCE(winb_is_real_money_bet, TRUE) = TRUE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingest_time DESC) = 1
),
activity_days AS (
  SELECT DISTINCT user_id, dt FROM casino_bet
  UNION DISTINCT
  SELECT DISTINCT user_id, dt FROM sport
),
ordered AS (
  SELECT
    user_id,
    dt,
    LAG(dt) OVER (PARTITION BY user_id ORDER BY dt) AS prev_dt
  FROM activity_days
),
activation AS (
  SELECT
    user_id,
    dt AS activation_dt
  FROM ordered
  WHERE prev_dt IS NOT NULL
    AND DATE_DIFF(dt, prev_dt, DAY) >= 14
    AND dt >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY dt DESC) = 1
),
post_window AS (
  SELECT
    user_id,
    activation_dt,
    DATE_ADD(activation_dt, INTERVAL 13 DAY) AS end_dt
  FROM activation
),
casino_bet_day AS (
  SELECT user_id, dt, SUM(bet_base_cur) AS bet_base_cur
  FROM casino_bet
  GROUP BY 1,2
),
casino_win_day AS (
  SELECT user_id, dt, SUM(win_base_cur) AS win_base_cur
  FROM casino_win
  GROUP BY 1,2
),
sport_day AS (
  SELECT user_id, dt, SUM(sport_bet_base_cur) AS bet_base_cur, SUM(sport_win_base_cur) AS win_base_cur
  FROM sport
  GROUP BY 1,2
),
post AS (
  SELECT
    w.user_id,
    COALESCE(SUM(cb.bet_base_cur), 0) + COALESCE(SUM(sd.bet_base_cur), 0)
    - (COALESCE(SUM(cw.win_base_cur), 0) + COALESCE(SUM(sd.win_base_cur), 0)) AS ggr_post_base_cur,
    COUNT(DISTINCT a.dt) AS bet_days_post
  FROM post_window w
  LEFT JOIN casino_bet_day cb
    ON cb.user_id = w.user_id AND cb.dt BETWEEN w.activation_dt AND w.end_dt
  LEFT JOIN casino_win_day cw
    ON cw.user_id = w.user_id AND cw.dt BETWEEN w.activation_dt AND w.end_dt
  LEFT JOIN sport_day sd
    ON sd.user_id = w.user_id AND sd.dt BETWEEN w.activation_dt AND w.end_dt
  LEFT JOIN activity_days a
    ON a.user_id = w.user_id AND a.dt BETWEEN w.activation_dt AND w.end_dt
  GROUP BY 1
)
SELECT
  br.label_id,
  u.user_id,
  u.user_ext_id,
  u.crm_brand_id,
  br.crm_brand_name,
  TRUE AS flag,
  25 AS score,
  'WINBACK_HI_VAL_PLAYER' AS tag,
  CURRENT_DATE() AS snapshot_date,
  CURRENT_TIMESTAMP() AS computed_at,
  p.ggr_post_base_cur,
  p.bet_days_post
FROM u
LEFT JOIN brand br USING (crm_brand_id)
JOIN post p USING (user_id)
WHERE p.ggr_post_base_cur > 8000
  AND p.ggr_post_base_cur < 15000
  AND p.bet_days_post >= 5;