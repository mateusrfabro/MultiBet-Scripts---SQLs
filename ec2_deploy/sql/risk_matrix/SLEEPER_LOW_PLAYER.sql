-- ================================================================
-- Tag: SLEEPER_LOW_PLAYER
-- Matriz de Risco MultiBet — Athena/Trino
-- Score: +5 | Categoria: Comportamental | Tipo: Positivo
-- Descricao: Jogador aparece apenas em eventos sazonais,
--            cumpre missoes e fica inativo — nao possui comportamento
--            de abusador, porem demonstra interesse apenas sazonal
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

-- Total de dias ativos por jogador nos 90 dias
activity_summary AS (
  SELECT
    t.c_ecr_id AS user_id,
    COUNT(DISTINCT CAST(t.c_start_time AS DATE)) AS active_days
  FROM fund_ec2.tbl_real_fund_txn t
  WHERE t.c_start_time >= (SELECT start_ts FROM params)
    AND t.c_start_time <  (SELECT end_ts FROM params)
    AND t.c_txn_status = 'SUCCESS'
  GROUP BY t.c_ecr_id
),

-- Jogadores com participacao em bonus/promos
has_bonus AS (
  SELECT DISTINCT b.c_ecr_id AS user_id
  FROM bonus_ec2.tbl_bonus_pocket_txn b
  WHERE b.c_created_time >= (SELECT start_ts FROM params)
    AND b.c_created_time <  (SELECT end_ts FROM params)
    AND b.c_bonus_txn_status = 'SUCCESS'
),

-- Qualifica: poucos dias ativos (< 15 em 90d) MAS com uso de bonus
qualifying AS (
  SELECT a.user_id
  FROM activity_summary a
  JOIN has_bonus hb ON a.user_id = hb.user_id
  WHERE a.active_days <= 15
    AND a.active_days >= 2  -- pelo menos alguma atividade
)

SELECT
  CAST(br.label_id AS VARCHAR) AS label_id,
  CAST(q.user_id AS VARCHAR)   AS user_id,
  'SLEEPER_LOW_PLAYER'          AS tag,
  5                              AS score,
  CURRENT_DATE                   AS snapshot_date,
  CURRENT_TIMESTAMP              AS computed_at
FROM qualifying q
JOIN users u ON q.user_id = u.user_id
LEFT JOIN brand br ON u.crm_brand_id = br.crm_brand_id
WHERE br.label_id IS NOT NULL
