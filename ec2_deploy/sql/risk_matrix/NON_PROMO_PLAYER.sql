-- ================================================================
-- Tag: NON_PROMO_PLAYER
-- Matriz de Risco MultiBet — Athena/Trino
-- Score: +10 | Categoria: Comportamental | Tipo: Positivo
-- Descricao: Nao participa de promocoes ha 7 dias, mas continua ativo
--            Jogador estavel, sem engajamento de promocoes
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

-- Jogadores com atividade nos ultimos 7 dias
recently_active AS (
  SELECT DISTINCT t.c_ecr_id AS user_id
  FROM fund_ec2.tbl_real_fund_txn t
  WHERE t.c_start_time >= CURRENT_TIMESTAMP - INTERVAL '7' DAY
    AND t.c_start_time <  CURRENT_TIMESTAMP
    AND t.c_txn_type IN (27, 28, 41, 43, 59, 127) -- apostas
    AND t.c_txn_status = 'SUCCESS'
),

-- Jogadores que usaram bonus nos ultimos 7 dias
recent_bonus_users AS (
  SELECT DISTINCT b.c_ecr_id AS user_id
  FROM bonus_ec2.tbl_bonus_pocket_txn b
  WHERE b.c_created_time >= CURRENT_TIMESTAMP - INTERVAL '7' DAY
    AND b.c_created_time <  CURRENT_TIMESTAMP
    AND b.c_bonus_txn_status = 'SUCCESS'
),

-- Qualifica: ativo nos ultimos 7 dias SEM usar bonus
qualifying AS (
  SELECT ra.user_id
  FROM recently_active ra
  WHERE NOT EXISTS (
    SELECT 1 FROM recent_bonus_users rb WHERE rb.user_id = ra.user_id
  )
)

SELECT
  CAST(br.label_id AS VARCHAR) AS label_id,
  CAST(q.user_id AS VARCHAR)   AS user_id,
  'NON_PROMO_PLAYER'            AS tag,
  10                             AS score,
  CURRENT_DATE                   AS snapshot_date,
  CURRENT_TIMESTAMP              AS computed_at
FROM qualifying q
JOIN users u ON q.user_id = u.user_id
LEFT JOIN brand br ON u.crm_brand_id = br.crm_brand_id
WHERE br.label_id IS NOT NULL
