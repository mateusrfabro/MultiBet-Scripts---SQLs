-- ================================================================
-- Tag: ROLLBACK_PLAYER
-- Matriz de Risco MultiBet — Athena/Trino
-- Score: -15 | Categoria: Transacional | Tipo: Negativo
-- Descricao: Jogador com taxa alta de rollbacks vs apostas normais
--            Pode indicar exploits tecnicos ou abuso
-- ================================================================
WITH params AS (
  SELECT
    CURRENT_TIMESTAMP - INTERVAL '90' DAY AS start_ts,
    CURRENT_TIMESTAMP AS end_ts
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

-- Rollbacks por jogador
rollback_transactions AS (
  SELECT
    t.c_ecr_id AS user_id,
    COUNT(*) AS total_rollbacks,
    COUNT(DISTINCT CAST(t.c_start_time AS DATE)) AS rollback_days
  FROM fund_ec2.tbl_real_fund_txn t
  WHERE t.c_start_time >= (SELECT start_ts FROM params)
    AND t.c_start_time <  (SELECT end_ts FROM params)
    AND t.c_txn_type IN (72, 76, 61, 63, 91, 113) -- tipos de rollback
  GROUP BY t.c_ecr_id
),

-- Apostas regulares por jogador
regular_bets AS (
  SELECT
    t.c_ecr_id AS user_id,
    COUNT(*) AS total_bets
  FROM fund_ec2.tbl_real_fund_txn t
  WHERE t.c_start_time >= (SELECT start_ts FROM params)
    AND t.c_start_time <  (SELECT end_ts FROM params)
    AND t.c_txn_type IN (27, 28, 41, 43, 59, 127) -- apostas
    AND t.c_txn_status = 'SUCCESS'
  GROUP BY t.c_ecr_id
),

-- Qualifica: >= 5 rollbacks E taxa > 10%
qualifying AS (
  SELECT r.user_id
  FROM rollback_transactions r
  LEFT JOIN regular_bets b ON r.user_id = b.user_id
  WHERE r.total_rollbacks >= 5
    AND CASE
          WHEN COALESCE(b.total_bets, 0) > 0
          THEN CAST(r.total_rollbacks AS DOUBLE) / b.total_bets
          ELSE 1.0
        END > 0.10
)

SELECT
  CAST(br.label_id AS VARCHAR) AS label_id,
  CAST(q.user_id AS VARCHAR)   AS user_id,
  'ROLLBACK_PLAYER'             AS tag,
  -15                            AS score,
  CURRENT_DATE                   AS snapshot_date,
  CURRENT_TIMESTAMP              AS computed_at
FROM qualifying q
JOIN users u ON q.user_id = u.user_id
LEFT JOIN brand br ON u.crm_brand_id = br.crm_brand_id
WHERE br.label_id IS NOT NULL
