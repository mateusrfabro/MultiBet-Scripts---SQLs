-- =============================================================================
-- TOP JOGOS HORA A HORA — CASINO (D0 vs D-1)
-- =============================================================================
-- Objetivo: Top 10 jogos por WINS e quantidade de jogadores, hora a hora,
--           comparando hoje (D0) vs ontem (D-1).
--
-- Fonte: fund_ec2.tbl_real_fund_txn (bruto, centavos /100.0)
-- Motor: Presto/Trino (Athena)
--
-- Correcoes vs query original do Mauro:
--   1. c_product_id = 'CASINO' no WHERE (fund_ec2 mistura casino + sports)
--   2. Timezone com duplo AT TIME ZONE (UTC -> BRT)
--   3. Adicionado c_txn_status = 'SUCCESS'
--   4. ROW_NUMBER para top 10 por hora/dia
--   5. Filtro game_name <> 'altenar-games' removido (desnecessario com CASINO)
-- =============================================================================

WITH base AS (
    SELECT
        -- Dia rotulado: D0 (hoje) ou D-1 (ontem)
        CASE
            WHEN CAST(t.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE)
                 = CURRENT_DATE
            THEN 'D0'
            ELSE 'D-1'
        END AS dia,

        -- Hora BRT (0-23)
        HOUR(t.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS hora,

        t.c_game_id AS game_id,
        v.c_game_desc AS game_name,
        v.c_game_category AS game_category,

        -- Jogadores unicos
        COUNT(DISTINCT t.c_ecr_id) AS qtd_jogadores,

        -- Turnover (Bets): DB normal + CR cancel (revertendo wins cancelados)
        SUM(
            CASE
                WHEN m.c_op_type = 'DB' AND m.c_is_cancel_txn = false
                    THEN (COALESCE(r.c_amount_in_ecr_ccy, 0)
                        + COALESCE(b.c_drp_amount_in_ecr_ccy, 0)
                        + COALESCE(b.c_crp_amount_in_ecr_ccy, 0)
                        + COALESCE(b.c_wrp_amount_in_ecr_ccy, 0)
                        + COALESCE(b.c_rrp_amount_in_ecr_ccy, 0))
                WHEN m.c_op_type = 'CR' AND m.c_is_cancel_txn = true
                    THEN -(COALESCE(r.c_amount_in_ecr_ccy, 0)
                        + COALESCE(b.c_drp_amount_in_ecr_ccy, 0)
                        + COALESCE(b.c_crp_amount_in_ecr_ccy, 0)
                        + COALESCE(b.c_wrp_amount_in_ecr_ccy, 0)
                        + COALESCE(b.c_rrp_amount_in_ecr_ccy, 0))
                ELSE 0
            END
        ) AS bet_amount,

        -- Wins: CR normal + DB cancel (revertendo bets canceladas)
        SUM(
            CASE
                WHEN m.c_op_type = 'CR' AND m.c_is_cancel_txn = false
                    THEN (COALESCE(r.c_amount_in_ecr_ccy, 0)
                        + COALESCE(b.c_drp_amount_in_ecr_ccy, 0)
                        + COALESCE(b.c_crp_amount_in_ecr_ccy, 0)
                        + COALESCE(b.c_wrp_amount_in_ecr_ccy, 0)
                        + COALESCE(b.c_rrp_amount_in_ecr_ccy, 0))
                WHEN m.c_op_type = 'DB' AND m.c_is_cancel_txn = true
                    THEN -(COALESCE(r.c_amount_in_ecr_ccy, 0)
                        + COALESCE(b.c_drp_amount_in_ecr_ccy, 0)
                        + COALESCE(b.c_crp_amount_in_ecr_ccy, 0)
                        + COALESCE(b.c_wrp_amount_in_ecr_ccy, 0)
                        + COALESCE(b.c_rrp_amount_in_ecr_ccy, 0))
                ELSE 0
            END
        ) AS win_amount

    FROM fund_ec2.tbl_real_fund_txn t

    -- Sub-fund isolation: parcela real cash
    LEFT JOIN fund_ec2.tbl_realcash_sub_fund_txn r
        ON t.c_txn_id = r.c_fund_txn_id

    -- Sub-fund isolation: parcelas bonus (DRP, CRP, WRP, RRP)
    LEFT JOIN fund_ec2.tbl_bonus_sub_fund_txn b
        ON t.c_txn_id = b.c_fund_txn_id

    -- Master tipos: classifica DB/CR, cancel, gaming
    JOIN fund_ec2.tbl_real_fund_txn_type_mst m
        ON t.c_txn_type = m.c_txn_type

    -- Filtro test users
    JOIN ecr_ec2.tbl_ecr_flags f
        ON t.c_ecr_id = f.c_ecr_id

    -- Catalogo de jogos (nome + categoria)
    JOIN bireports_ec2.tbl_vendor_games_mapping_data v
        ON t.c_sub_product_id = v.c_vendor_id
       AND t.c_game_id = v.c_game_id

    WHERE
        -- >>> CORRECAO 1: Somente CASINO (exclui SPORTS_BOOK) <<<
        t.c_product_id = 'CASINO'
        -- >>> CORRECAO 3: Somente transacoes com sucesso <<<
        AND t.c_txn_status = 'SUCCESS'
        -- Somente transacoes de jogo
        AND m.c_is_gaming_txn = 'Y'
        -- Excluir test users
        AND f.c_test_user = false
        -- Jogo deve existir e estar mapeado
        AND t.c_game_id IS NOT NULL
        AND v.c_game_desc IS NOT NULL
        -- >>> CORRECAO 2: Range D-1 e D0 em UTC (com margem BRT = UTC-3) <<<
        -- D-1 BRT (30/03) comeca em 29/03 03:00 UTC
        -- D0  BRT (31/03) termina em 01/04 02:59 UTC
        AND t.c_start_time >= TIMESTAMP '2026-03-29 03:00:00'
        AND t.c_start_time <  TIMESTAMP '2026-04-01 03:00:00'

    GROUP BY 1, 2, 3, 4, 5
),

-- Ranking por wins dentro de cada hora/dia
ranked AS (
    SELECT
        dia,
        hora,
        game_id,
        game_name,
        game_category,
        qtd_jogadores,
        ROUND(bet_amount / 100.0, 2) AS turnover_brl,
        ROUND(win_amount / 100.0, 2) AS wins_brl,
        ROUND((bet_amount - win_amount) / 100.0, 2) AS ggr_brl,
        ROUND(CASE WHEN bet_amount > 0 THEN (win_amount * 100.0 / bet_amount) ELSE 0 END, 2) AS rtp_percent,
        ROUND(CASE WHEN qtd_jogadores > 0 THEN (win_amount / 100.0 / qtd_jogadores) ELSE 0 END, 2) AS win_por_jogador,
        ROW_NUMBER() OVER (
            PARTITION BY dia, hora
            ORDER BY win_amount DESC
        ) AS rank_por_hora
    FROM base
    WHERE win_amount > 0
)

-- Output final: top 10 por hora
SELECT
    dia,
    hora,
    rank_por_hora,
    game_id,
    game_name,
    game_category,
    qtd_jogadores,
    turnover_brl,
    wins_brl,
    ggr_brl,
    rtp_percent,
    win_por_jogador
FROM ranked
WHERE rank_por_hora <= 10
ORDER BY
    dia DESC,           -- D0 primeiro
    hora ASC,           -- 0h -> 23h
    rank_por_hora ASC;  -- 1o lugar primeiro
