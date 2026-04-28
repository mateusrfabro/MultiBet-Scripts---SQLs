WITH params AS (
  SELECT 
    CURRENT_TIMESTAMP - INTERVAL '90' DAY AS start_ts, 
    CURRENT_TIMESTAMP AS end_ts
),

users AS (
  SELECT 
    u.c_ecr_id AS user_id, 
    u.c_partner_id AS crm_brand_id
  FROM ecr_ec2.tbl_ecr u
  JOIN ecr_ec2.tbl_ecr_flags f 
    ON u.c_ecr_id = f.c_ecr_id
  WHERE f.c_test_user = false
),

brand AS (
  SELECT 
    c_partner_id AS crm_brand_id, 
    c_partner_id AS label_id
  FROM ecr_ec2.tbl_ecr
  WHERE c_partner_id IS NOT NULL
  GROUP BY c_partner_id
),

promotion_dates AS (
  SELECT DISTINCT 
    b.c_ecr_id AS user_id, 
    DATE(b.c_created_time) AS promo_date
  FROM bonus_ec2.tbl_bonus_pocket_txn b
  WHERE b.c_created_time >= (SELECT start_ts FROM params)
    AND b.c_created_time < (SELECT end_ts FROM params)
    AND b.c_bonus_txn_status = 'SUCCESS'
),

deposits AS (
  SELECT 
    c.c_ecr_id AS user_id, 
    DATE(c.c_created_time) AS deposit_date
  FROM cashier_ec2.tbl_cashier_deposit c
  WHERE c.c_created_time >= (SELECT start_ts FROM params)
    AND c.c_created_time < (SELECT end_ts FROM params)
    AND c.c_txn_status = 'txn_confirmed_success'
    AND c.c_initial_amount > 0
),
invalid_users AS (
  SELECT DISTINCT d.user_id
  FROM deposits d
  LEFT JOIN promotion_dates p
    ON d.user_id = p.user_id
   AND d.deposit_date = p.promo_date
  WHERE p.user_id IS NULL  -- depósito fora do dia de promo
),

qualifying AS (
  SELECT DISTINCT p.user_id
  FROM promotion_dates p
  LEFT JOIN invalid_users i
    ON p.user_id = i.user_id
  WHERE i.user_id IS NULL  -- mantém só quem NÃO tem depósito inválido
)

SELECT
  CAST(br.label_id AS VARCHAR) AS label_id,
  CAST(q.user_id AS VARCHAR) AS user_id,
  'PROMO_ONLY' AS tag,
  -20 AS score,
  CURRENT_DATE AS snapshot_date,
  CURRENT_TIMESTAMP AS computed_at
FROM qualifying q
JOIN users u 
  ON q.user_id = u.user_id
LEFT JOIN brand br 
  ON u.crm_brand_id = br.crm_brand_id