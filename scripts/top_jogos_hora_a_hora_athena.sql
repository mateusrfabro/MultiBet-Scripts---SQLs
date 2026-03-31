WITH game_rounds AS (
    SELECT
        CASE
            WHEN CAST(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = CURRENT_DATE THEN 'D0'
            ELSE 'D-1'
        END AS dia,
        HOUR(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS hora,
        f.c_game_id,
        COUNT(DISTINCT f.c_ecr_id) AS unique_players,
        SUM(CASE WHEN f.c_txn_type = 45 THEN f.c_amount_in_ecr_ccy ELSE 0 END) / 100.0 AS total_wins_brl
    FROM fund_ec2.tbl_real_fund_txn f
    JOIN bireports_ec2.tbl_ecr br
        ON f.c_ecr_id = br.c_ecr_id
       AND br.c_test_user = false
    WHERE f.c_txn_status = 'SUCCESS'
        AND f.c_txn_type IN (27, 45)
        AND f.c_product_id = 'CASINO'
        AND f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' >= date_add('day', -1, current_date)
    GROUP BY 1, 2, 3
),
ranked AS (
    SELECT
        gr.dia,
        gr.hora,
        gr.c_game_id,
        COALESCE(g.c_game_desc, 'ID: ' || gr.c_game_id) AS game_name,
        gr.total_wins_brl,
        gr.unique_players,
        ROW_NUMBER() OVER (PARTITION BY gr.dia, gr.hora ORDER BY gr.total_wins_brl DESC) AS rank_hora
    FROM game_rounds gr
    LEFT JOIN (
        SELECT DISTINCT c_game_id, c_game_desc
        FROM bireports_ec2.tbl_vendor_games_mapping_data
    ) g ON gr.c_game_id = g.c_game_id
    WHERE gr.total_wins_brl > 0
)
SELECT
    dia,
    hora,
    rank_hora,
    c_game_id,
    game_name,
    total_wins_brl,
    unique_players
FROM ranked
WHERE rank_hora <= 10
ORDER BY dia DESC, hora ASC, rank_hora ASC
