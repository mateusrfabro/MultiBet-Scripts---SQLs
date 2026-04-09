-- ================================================================
-- Tag: VIP_WHALE_PLAYER
-- Matriz de Risco MultiBet — Athena/Trino
-- Score: +30 | Categoria: Transacao e Comportamental | Tipo: Positivo
-- Descricao: GGR > 15k e alta frequencia — jogador altamente valioso
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

-- GGR por jogador (bets - wins) em BRL
-- fund_ec2 usa centavos, dividir por 100
-- Nota: c_txn_status='SUCCESS' ja exclui rollbacks, nao precisa calcular separado
player_ggr AS (
  SELECT
    t.c_ecr_id AS user_id,
    SUM(CASE WHEN t.c_txn_type IN (27, 28, 41, 43, 59, 127)
             THEN t.c_amount_in_ecr_ccy ELSE 0 END) / 100.0 AS total_bets_brl,
    SUM(CASE WHEN t.c_txn_type IN (45, 80, 112)
             THEN t.c_amount_in_ecr_ccy ELSE 0 END) / 100.0 AS total_wins_brl,
    COUNT(DISTINCT CAST(t.c_start_time AS DATE)) AS active_days
  FROM fund_ec2.tbl_real_fund_txn t
  WHERE t.c_start_time >= (SELECT start_ts FROM params)
    AND t.c_start_time <  (SELECT end_ts FROM params)
    AND t.c_txn_status = 'SUCCESS'
  GROUP BY t.c_ecr_id
),

-- GGR = bets - wins (rollbacks ja sao devolvidos na pratica)
qualifying AS (
  SELECT
    user_id,
    total_bets_brl - total_wins_brl AS ggr_brl
  FROM player_ggr
  WHERE (total_bets_brl - total_wins_brl) > 15000  -- GGR > R$15k
    AND active_days >= 10                           -- alta frequencia
)

SELECT
  CAST(br.label_id AS VARCHAR) AS label_id,
  CAST(q.user_id AS VARCHAR)   AS user_id,
  'VIP_WHALE_PLAYER'            AS tag,
  30                             AS score,
  CURRENT_DATE                   AS snapshot_date,
  CURRENT_TIMESTAMP              AS computed_at
FROM qualifying q
JOIN users u ON q.user_id = u.user_id
LEFT JOIN brand br ON u.crm_brand_id = br.crm_brand_id
WHERE br.label_id IS NOT NULL
