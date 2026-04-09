-- ================================================================
-- Tag: PROMO_CHAINER
-- Matriz de Risco MultiBet — Athena/Trino
-- Score: -10 | Categoria: Comportamental | Tipo: Negativo
-- Descricao: Participa de 3+ promocoes consecutivas sem jogar
--            fora delas — bonus grinder
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

-- Dias com bonus
bonus_days AS (
  SELECT
    b.c_ecr_id AS user_id,
    COUNT(DISTINCT CAST(b.c_created_time AS DATE)) AS bonus_day_count
  FROM bonus_ec2.tbl_bonus_pocket_txn b
  WHERE b.c_created_time >= (SELECT start_ts FROM params)
    AND b.c_created_time <  (SELECT end_ts FROM params)
    AND b.c_bonus_txn_status = 'SUCCESS'
  GROUP BY b.c_ecr_id
),

-- Dias com atividade total (apostas)
activity_days AS (
  SELECT
    t.c_ecr_id AS user_id,
    COUNT(DISTINCT CAST(t.c_start_time AS DATE)) AS active_day_count
  FROM fund_ec2.tbl_real_fund_txn t
  WHERE t.c_start_time >= (SELECT start_ts FROM params)
    AND t.c_start_time <  (SELECT end_ts FROM params)
    AND t.c_txn_type IN (27, 28, 41, 43, 59, 127) -- apostas
    AND t.c_txn_status = 'SUCCESS'
  GROUP BY t.c_ecr_id
),

-- Qualifica: 3+ dias de bonus E >= 80% dos dias ativos coincidem com bonus
qualifying AS (
  SELECT bd.user_id
  FROM bonus_days bd
  LEFT JOIN activity_days ad ON bd.user_id = ad.user_id
  WHERE bd.bonus_day_count >= 3
    AND (
      ad.active_day_count IS NULL  -- zero atividade fora de bonus
      OR CAST(bd.bonus_day_count AS DOUBLE) / GREATEST(ad.active_day_count, 1) >= 0.80
    )
)

SELECT
  CAST(br.label_id AS VARCHAR) AS label_id,
  CAST(q.user_id AS VARCHAR)   AS user_id,
  'PROMO_CHAINER'               AS tag,
  -10                            AS score,
  CURRENT_DATE                   AS snapshot_date,
  CURRENT_TIMESTAMP              AS computed_at
FROM qualifying q
JOIN users u ON q.user_id = u.user_id
LEFT JOIN brand br ON u.crm_brand_id = br.crm_brand_id
WHERE br.label_id IS NOT NULL
