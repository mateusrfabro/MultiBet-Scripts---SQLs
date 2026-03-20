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
  SELECT user_id, user_ext_id, crm_brand_id
  FROM `{{PROJECT}}.{{DATASET}}.j_user`
  WHERE COALESCE(core_is_test_account, FALSE) = FALSE
),
brand AS (
  SELECT label_id, crm_brand_id, crm_brand_name
  FROM `{{PROJECT}}.{{DATASET}}.dm_brand`
  WHERE COALESCE(is_deleted, FALSE) = FALSE
),
prefs AS (
  SELECT
    p.user_id,
    p.label_id,
    gt.smr_game_type_id,
    gt.share
  FROM `{{PROJECT}}.{{DATASET}}.ml_player_preferences` p,
  UNNEST(p.favorite_casino_game_types) gt
),
slot_types AS (
  SELECT label_id, smr_game_type_id
  FROM `{{PROJECT}}.{{DATASET}}.dm_casino_game_type`
  WHERE LOWER(game_type) LIKE '%slot%'
),
slot_share AS (
  SELECT
    p.user_id,
    SUM(p.share) AS slot_share
  FROM prefs p
  JOIN slot_types s
    ON s.label_id = p.label_id
   AND s.smr_game_type_id = p.smr_game_type_id
  GROUP BY 1
)
SELECT
  br.label_id,
  u.user_id,
  u.user_ext_id,
  u.crm_brand_id,
  br.crm_brand_name,
  TRUE AS flag,
  5 AS score,
  'BEHAV_SLOTGAMER' AS tag,
  CURRENT_DATE() AS snapshot_date,
  CURRENT_TIMESTAMP() AS computed_at,
  COALESCE(s.slot_share, 0) AS slot_share
FROM u
JOIN active_90d a
  ON a.user_id = u.user_id
LEFT JOIN brand br
  ON br.crm_brand_id = u.crm_brand_id
JOIN slot_share s
  ON s.user_id = u.user_id
WHERE COALESCE(s.slot_share, 0) >= 0.6;