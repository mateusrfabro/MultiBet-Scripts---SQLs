WITH params AS (
  SELECT CURRENT_TIMESTAMP - INTERVAL '90' DAY AS start_ts, CURRENT_TIMESTAMP AS end_ts
),
users AS (
  SELECT u.c_ecr_id AS user_id, u.c_partner_id AS crm_brand_id
  FROM ecr_ec2.tbl_ecr u
  JOIN ecr_ec2.tbl_ecr_flags f ON u.c_ecr_id = f.c_ecr_id
  WHERE f.c_test_user = false
),
brand AS (
  SELECT c_partner_id AS crm_brand_id, c_partner_id AS label_id
  FROM ecr_ec2.tbl_ecr
  WHERE c_partner_id IS NOT NULL
  GROUP BY c_partner_id
),

user_activity AS (
  SELECT
    t.c_ecr_id AS user_id,
    COUNT(*) AS total_transactions
  FROM fund_ec2.tbl_real_fund_txn t
  WHERE t.c_start_time >= (SELECT start_ts FROM params)
    AND t.c_start_time < (SELECT end_ts FROM params)
  GROUP BY t.c_ecr_id
),
qualifying AS (
  SELECT user_id FROM user_activity WHERE total_transactions >= 10
)
SELECT
  CAST(br.label_id AS VARCHAR) AS label_id,
  CAST(q.user_id AS VARCHAR) AS user_id,
  'WINBACK_HI_VAL_PLAYER' AS tag,
  12 AS score,
  CURRENT_DATE AS snapshot_date,
  CURRENT_TIMESTAMP AS computed_at
FROM qualifying q
JOIN users u ON q.user_id = u.user_id
LEFT JOIN brand br ON u.crm_brand_id = br.crm_brand_id
WHERE br.label_id IS NOT NULL