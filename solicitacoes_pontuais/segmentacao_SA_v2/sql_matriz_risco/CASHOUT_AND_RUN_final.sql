WITH params AS (
  SELECT CURRENT_TIMESTAMP - INTERVAL '90' DAY AS start_ts, CURRENT_TIMESTAMP AS end_ts
),
users AS (
  SELECT u.c_ecr_id AS user_id, u.c_partner_id AS crm_brand_id
  FROM ecr_ec2.tbl_ecr u
  JOIN ecr_ec2.tbl_ecr_flags f ON u.c_ecr_id = f.c_ecr_id
  WHERE f.c_test_user = false
    AND u.c_partner_id IS NOT NULL
),
brand AS (
  SELECT c_partner_id AS crm_brand_id, c_partner_id AS label_id
  FROM ecr_ec2.tbl_ecr
  WHERE c_partner_id IS NOT NULL
  GROUP BY c_partner_id
),

bonus_usage AS (
  SELECT DISTINCT
    b.c_ecr_id AS user_id,
    DATE(b.c_created_time) AS bonus_date
  FROM bonus_ec2.tbl_bonus_pocket_txn b
  WHERE b.c_created_time >= (SELECT start_ts FROM params)
    AND b.c_created_time < (SELECT end_ts FROM params)
    AND b.c_bonus_txn_status IN ('SUCCESS')
),
cashouts AS (
  SELECT
    c.c_ecr_id AS user_id,
    DATE(c.c_created_time) AS cashout_date
  FROM cashier_ec2.tbl_cashier_cashout c
  WHERE c.c_created_time >= (SELECT start_ts FROM params)
    AND c.c_created_time < (SELECT end_ts FROM params)
    AND c.c_txn_status = 'co_success'
    AND c.c_paid_amount > 0
),
recent_activity AS (
  SELECT
    t.c_ecr_id AS user_id,
    MAX(DATE(t.c_start_time)) AS last_activity_date
  FROM fund_ec2.tbl_real_fund_txn t
  WHERE t.c_start_time >= (SELECT start_ts FROM params)
    AND t.c_start_time < (SELECT end_ts FROM params)
  GROUP BY t.c_ecr_id
),
qualifying AS (
  SELECT
    bu.user_id,
    bu.bonus_date,
    MIN(co.cashout_date) AS first_cashout_after_bonus,
    ra.last_activity_date
  FROM bonus_usage bu
  JOIN cashouts co ON bu.user_id = co.user_id
    AND co.cashout_date >= bu.bonus_date
  JOIN recent_activity ra ON bu.user_id = ra.user_id
  GROUP BY bu.user_id, bu.bonus_date, ra.last_activity_date
  HAVING MIN(co.cashout_date) <= bu.bonus_date + INTERVAL '1' DAY
     AND ra.last_activity_date <= MIN(co.cashout_date) + INTERVAL '2' DAY
)
SELECT
  CAST(br.label_id AS VARCHAR) AS label_id,
  CAST(q.user_id AS VARCHAR) AS user_id,
  'CASHOUT_AND_RUN' AS tag,
  -25 AS score,
  CURRENT_DATE AS snapshot_date,
  CURRENT_TIMESTAMP AS computed_at
FROM qualifying q
JOIN users u ON q.user_id = u.user_id
LEFT JOIN brand br ON u.crm_brand_id = br.crm_brand_id
WHERE br.label_id IS NOT NULL