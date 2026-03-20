WITH params AS (
  SELECT
    TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{WINDOW_DAYS}} DAY), DAY) AS start_ts,
    CURRENT_TIMESTAMP() AS end_ts
),
users AS (
  SELECT
    user_id,
    crm_brand_id,
    core_wallet_currency,
    user_country
  FROM `{{PROJECT}}.{{DATASET}}.j_user`
  WHERE COALESCE(core_is_test_account, FALSE) = FALSE
),
brand AS (
  SELECT label_id, crm_brand_id, crm_brand_name
  FROM `{{PROJECT}}.{{DATASET}}.dm_brand`
  WHERE COALESCE(is_deleted, FALSE) = FALSE
),
wd_dedup AS (
  SELECT
    label_id,
    user_id,
    event_time AS wd_ts,
    COALESCE(
      acc_sc_last_withdrawal_amount,
      acc_last_withdrawal_amount * user_to_label_cur_rate
    ) AS wd_base_cur,
    event_id,
    ingest_time
  FROM `{{PROJECT}}.{{DATASET}}.tr_acc_withdrawal_approved`
  WHERE event_time >= (SELECT start_ts FROM params)
    AND event_time <  (SELECT end_ts FROM params)
    AND COALESCE(acc_is_rollback, FALSE) = FALSE
    AND COALESCE(core_is_exclude_from_report, FALSE) = FALSE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingest_time DESC) = 1
),
wd_enriched AS (
  SELECT
    w.label_id,
    w.user_id,
    w.wd_ts,
    w.wd_base_cur,
    u.crm_brand_id,
    u.core_wallet_currency,
    u.user_country,
    EXTRACT(HOUR FROM w.wd_ts) AS wd_hour
  FROM wd_dedup w
  JOIN users u USING (user_id)
),
user_stats AS (
  SELECT
    label_id,
    user_id,
    crm_brand_id,
    core_wallet_currency,
    user_country,
    COUNT(*) AS wd_cnt_user,
    APPROX_QUANTILES(wd_base_cur, 101)[OFFSET(50)] AS med_wd_base_cur,
    APPROX_QUANTILES(wd_hour, 101)[OFFSET(50)] AS med_wd_hour
  FROM wd_enriched
  GROUP BY 1,2,3,4,5
),
group_stats AS (
  SELECT
    label_id,
    crm_brand_id,
    core_wallet_currency,
    user_country,
    COUNT(*) AS wd_cnt_group,
    APPROX_QUANTILES(wd_base_cur, 101)[OFFSET(95)] AS p95_wd_base_cur_group,
    APPROX_QUANTILES(wd_hour, 101)[OFFSET(5)]  AS p05_hour_group,
    APPROX_QUANTILES(wd_hour, 101)[OFFSET(95)] AS p95_hour_group
  FROM wd_enriched
  GROUP BY 1,2,3,4
)
SELECT
  br.label_id,
  us.user_id,
  ju.user_ext_id,
  ju.crm_brand_id,
  br.crm_brand_name,
  TRUE AS flag,
  -10 AS score,
  'BEHAV_RISK_PLAYER' AS tag,
  CURRENT_DATE() AS snapshot_date,
  CURRENT_TIMESTAMP() AS computed_at,
  us.wd_cnt_user,
  gs.wd_cnt_group,
  us.med_wd_base_cur,
  us.med_wd_hour,
  gs.p95_wd_base_cur_group,
  gs.p05_hour_group,
  gs.p95_hour_group
FROM user_stats us
JOIN group_stats gs
  ON gs.label_id = us.label_id
 AND gs.crm_brand_id = us.crm_brand_id
 AND gs.core_wallet_currency = us.core_wallet_currency
 AND gs.user_country = us.user_country
JOIN `{{PROJECT}}.{{DATASET}}.j_user` ju
  ON ju.user_id = us.user_id
LEFT JOIN brand br
  ON br.crm_brand_id = ju.crm_brand_id
WHERE us.wd_cnt_user >= 3
  AND gs.wd_cnt_group >= 50
  AND us.med_wd_base_cur >= gs.p95_wd_base_cur_group
  AND (us.med_wd_hour <= gs.p05_hour_group OR us.med_wd_hour >= gs.p95_hour_group);