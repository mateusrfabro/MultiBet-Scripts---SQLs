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

cashout_reinvest AS (
  SELECT DISTINCT
    co.c_ecr_id AS user_id
  FROM cashier_ec2.tbl_cashier_cashout co
  JOIN cashier_ec2.tbl_cashier_deposit d ON co.c_ecr_id = d.c_ecr_id
  WHERE co.c_created_time >= (SELECT start_ts FROM params)
    AND d.c_created_time BETWEEN co.c_created_time AND co.c_created_time + INTERVAL '1' DAY
    AND co.c_txn_status = 'co_success'
    AND d.c_txn_status = 'txn_confirmed_success'
),
qualifying AS (
  SELECT user_id FROM cashout_reinvest
)
SELECT
  CAST(br.label_id AS VARCHAR) AS label_id,
  CAST(q.user_id AS VARCHAR) AS user_id,
  'REINVEST_PLAYER' AS tag,
  6 AS score,
  CURRENT_DATE AS snapshot_date,
  CURRENT_TIMESTAMP AS computed_at
FROM qualifying q
JOIN users u ON q.user_id = u.user_id
LEFT JOIN brand br ON u.crm_brand_id = br.crm_brand_id
WHERE br.label_id IS NOT NULL