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

deposits AS (
  SELECT
    d.c_ecr_id AS user_id,
    d.c_created_time AS deposit_time,
    d.c_initial_amount AS deposit_amount
  FROM cashier_ec2.tbl_cashier_deposit d
  WHERE d.c_created_time >= (SELECT start_ts FROM params)
    AND d.c_created_time < (SELECT end_ts FROM params)
    AND d.c_txn_status = 'txn_confirmed_success'
    AND d.c_initial_amount > 0
),
cashouts AS (
  SELECT
    c.c_ecr_id AS user_id,
    c.c_created_time AS cashout_time,
    c.c_paid_amount AS cashout_amount
  FROM cashier_ec2.tbl_cashier_cashout c
  WHERE c.c_created_time >= (SELECT start_ts FROM params)
    AND c.c_created_time < (SELECT end_ts FROM params)
    AND c.c_txn_status = 'co_success'
    AND c.c_paid_amount > 0
),
qualifying AS (
  SELECT DISTINCT d.user_id
  FROM deposits d
  JOIN cashouts c ON d.user_id = c.user_id
  WHERE c.cashout_time BETWEEN d.deposit_time AND d.deposit_time + INTERVAL '2' HOUR
)
SELECT
  CAST(br.label_id AS VARCHAR) AS label_id,
  CAST(q.user_id AS VARCHAR) AS user_id,
  'FAST_CASHOUT' AS tag,
  -15 AS score,
  CURRENT_DATE AS snapshot_date,
  CURRENT_TIMESTAMP AS computed_at
FROM qualifying q
JOIN users u ON q.user_id = u.user_id
LEFT JOIN brand br ON u.crm_brand_id = br.crm_brand_id
WHERE br.label_id IS NOT NULL