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
monthly_deposits AS (
    SELECT
        c.c_ecr_id AS user_id,
        DATE_TRUNC('month', DATE(c.c_created_time)) AS mes_ano,
        COUNT(c.c_txn_id) AS qtd_dep
    FROM cashier_ec2.tbl_cashier_deposit c
    WHERE c.c_created_time >= (SELECT start_ts FROM params)
      AND c.c_created_time < (SELECT end_ts FROM params)
      AND c.c_txn_status = 'txn_confirmed_success'
      AND c.c_initial_amount > 0
    GROUP BY c.c_ecr_id, DATE_TRUNC('month', DATE(c.c_created_time))
),
qualifying AS (
    SELECT user_id
    FROM monthly_deposits
    GROUP BY user_id
    HAVING AVG(qtd_dep) >= 3
)
SELECT
    CAST(br.label_id AS VARCHAR) AS label_id,
    CAST(u.user_id AS VARCHAR) AS user_id,
    'REGULAR_DEPOSITOR' AS tag,
    10 AS score,
    CURRENT_DATE AS snapshot_date,
    CURRENT_TIMESTAMP AS computed_at
FROM qualifying q
JOIN users u ON q.user_id = u.user_id
LEFT JOIN brand br ON u.crm_brand_id = br.crm_brand_id
WHERE br.label_id IS NOT NULL