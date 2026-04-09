-- ================================================================
-- Tag: WINBACK_HI_VAL_PLAYER
-- Matriz de Risco MultiBet — Athena/Trino
-- Score: +25 | Categoria: Transacao e Comportamental | Tipo: Positivo
-- Descricao: Jogador reativado com alto indice de apostas e
--            frequencia pos ativacao, 8k < GGR < 15k
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

-- Atividade antiga (antes de 30 dias atras)
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

-- Atividade recente (ultimos 30 dias) com GGR
recent_ggr AS (
  SELECT
    t.c_ecr_id AS user_id,
    MIN(t.c_start_time) AS first_recent_activity,
    COUNT(DISTINCT CAST(t.c_start_time AS DATE)) AS recent_active_days,
    SUM(CASE WHEN t.c_txn_type IN (27, 28, 41, 43, 59, 127)
             THEN t.c_amount_in_ecr_ccy ELSE 0 END) / 100.0 AS bets_brl,
    SUM(CASE WHEN t.c_txn_type IN (45, 80, 112)
             THEN t.c_amount_in_ecr_ccy ELSE 0 END) / 100.0 AS wins_brl
  FROM fund_ec2.tbl_real_fund_txn t
  WHERE t.c_start_time >= CURRENT_TIMESTAMP - INTERVAL '30' DAY
    AND t.c_start_time <  CURRENT_TIMESTAMP
    AND t.c_txn_status = 'SUCCESS'
  GROUP BY t.c_ecr_id
),

-- Qualifica: reativado (gap 30d+) + GGR entre 8k e 15k + alta frequencia
qualifying AS (
  SELECT rg.user_id
  FROM recent_ggr rg
  JOIN old_activity oa ON rg.user_id = oa.user_id
  WHERE date_diff('day', oa.last_old_activity, rg.first_recent_activity) >= 30
    AND (rg.bets_brl - rg.wins_brl) BETWEEN 8000 AND 15000
    AND rg.recent_active_days >= 5
)

SELECT
  CAST(br.label_id AS VARCHAR) AS label_id,
  CAST(q.user_id AS VARCHAR)   AS user_id,
  'WINBACK_HI_VAL_PLAYER'      AS tag,
  25                             AS score,
  CURRENT_DATE                   AS snapshot_date,
  CURRENT_TIMESTAMP              AS computed_at
FROM qualifying q
JOIN users u ON q.user_id = u.user_id
LEFT JOIN brand br ON u.crm_brand_id = br.crm_brand_id
WHERE br.label_id IS NOT NULL
