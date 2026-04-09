-- ================================================================
-- Tag: PLAYER_REENGAGED
-- Matriz de Risco MultiBet — Athena/Trino
-- Score: +30 | Categoria: Comportamental | Tipo: Positivo
-- Descricao: Reativacao do jogador e engajamento pos evento
--            Jogador re-ativado que se manteve engajado apos promo
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

-- Atividade por jogador (data da ultima txn antes de 30d atras)
old_activity AS (
  SELECT
    t.c_ecr_id AS user_id,
    MAX(t.c_start_time) AS last_old_activity
  FROM fund_ec2.tbl_real_fund_txn t
  WHERE t.c_start_time >= (SELECT start_ts FROM params)
    AND t.c_start_time <  CURRENT_TIMESTAMP - INTERVAL '30' DAY
    AND t.c_txn_status = 'SUCCESS'
  GROUP BY t.c_ecr_id
),

-- Atividade recente (ultimos 14 dias)
recent_activity AS (
  SELECT
    t.c_ecr_id AS user_id,
    COUNT(DISTINCT CAST(t.c_start_time AS DATE)) AS recent_active_days,
    MIN(t.c_start_time) AS first_recent_activity
  FROM fund_ec2.tbl_real_fund_txn t
  WHERE t.c_start_time >= CURRENT_TIMESTAMP - INTERVAL '14' DAY
    AND t.c_start_time <  CURRENT_TIMESTAMP
    AND t.c_txn_status = 'SUCCESS'
  GROUP BY t.c_ecr_id
),

-- Gap de inatividade: periodo sem atividade entre old e recent
-- Qualifica: gap >= 30 dias E pelo menos 3 dias ativos nos ultimos 14
qualifying AS (
  SELECT ra.user_id
  FROM recent_activity ra
  JOIN old_activity oa ON ra.user_id = oa.user_id
  WHERE date_diff('day', oa.last_old_activity, ra.first_recent_activity) >= 30
    AND ra.recent_active_days >= 3
)

SELECT
  CAST(br.label_id AS VARCHAR) AS label_id,
  CAST(q.user_id AS VARCHAR)   AS user_id,
  'PLAYER_REENGAGED'            AS tag,
  30                             AS score,
  CURRENT_DATE                   AS snapshot_date,
  CURRENT_TIMESTAMP              AS computed_at
FROM qualifying q
JOIN users u ON q.user_id = u.user_id
LEFT JOIN brand br ON u.crm_brand_id = br.crm_brand_id
WHERE br.label_id IS NOT NULL
