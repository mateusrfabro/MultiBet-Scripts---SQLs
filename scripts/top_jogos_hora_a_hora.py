"""
Top jogos hora a hora — CASINO (D0 vs D-1)
Extrai top 10 jogos por WINS e qtd jogadores, hora a hora.
Exporta Excel com aba de dados + aba de legenda.
"""

import sys
import os
import logging
from datetime import date, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from db.athena import query_athena

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# Datas BRT → timestamps UTC (BRT = UTC-3)
HOJE = date.today()
ONTEM = HOJE - timedelta(days=1)
UTC_START = (ONTEM - timedelta(days=1)).strftime("%Y-%m-%d") + " 03:00:00"  # D-2 03:00 UTC = D-1 00:00 BRT
UTC_END = (HOJE + timedelta(days=1)).strftime("%Y-%m-%d") + " 03:00:00"    # D+1 03:00 UTC = D0 23:59 BRT

SQL = f"""
WITH base AS (
    SELECT
        CASE
            WHEN CAST(t.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE)
                 = CURRENT_DATE
            THEN 'D0'
            ELSE 'D-1'
        END AS dia,
        HOUR(t.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS hora,
        t.c_game_id AS game_id,
        v.c_game_desc AS game_name,
        v.c_game_category AS game_category,
        COUNT(DISTINCT t.c_ecr_id) AS qtd_jogadores,
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
    LEFT JOIN fund_ec2.tbl_realcash_sub_fund_txn r
        ON t.c_txn_id = r.c_fund_txn_id
    LEFT JOIN fund_ec2.tbl_bonus_sub_fund_txn b
        ON t.c_txn_id = b.c_fund_txn_id
    JOIN fund_ec2.tbl_real_fund_txn_type_mst m
        ON t.c_txn_type = m.c_txn_type
    JOIN ecr_ec2.tbl_ecr_flags f
        ON t.c_ecr_id = f.c_ecr_id
    JOIN bireports_ec2.tbl_vendor_games_mapping_data v
        ON t.c_sub_product_id = v.c_vendor_id
       AND t.c_game_id = v.c_game_id
    WHERE
        t.c_product_id = 'CASINO'
        AND t.c_txn_status = 'SUCCESS'
        AND m.c_is_gaming_txn = 'Y'
        AND f.c_test_user = false
        AND t.c_game_id IS NOT NULL
        AND v.c_game_desc IS NOT NULL
        AND t.c_start_time >= TIMESTAMP '{UTC_START}'
        AND t.c_start_time <  TIMESTAMP '{UTC_END}'
    GROUP BY 1, 2, 3, 4, 5
),
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
ORDER BY dia DESC, hora ASC, rank_por_hora ASC
"""


def main():
    log.info(f"Extraindo top jogos hora a hora: D-1={ONTEM} / D0={HOJE}")
    log.info(f"Range UTC: {UTC_START} ate {UTC_END}")

    df = query_athena(SQL, database="fund_ec2")
    log.info(f"Linhas retornadas: {len(df)}")

    if df.empty:
        log.warning("Nenhum dado retornado. Verifique o range de datas.")
        return

    # Exportar Excel com legenda
    out = f"data/top_jogos_hora_a_hora_{HOJE.strftime('%Y%m%d')}.xlsx"
    os.makedirs("data", exist_ok=True)

    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Top Jogos", index=False)

        # Aba de legenda
        legenda = pd.DataFrame([
            {"Coluna": "dia", "Descricao": "D0 = hoje, D-1 = ontem", "Unidade": "-"},
            {"Coluna": "hora", "Descricao": "Hora BRT (0-23)", "Unidade": "hora"},
            {"Coluna": "rank_por_hora", "Descricao": "Posicao no ranking por hora (1=maior wins)", "Unidade": "-"},
            {"Coluna": "game_id", "Descricao": "ID interno do jogo", "Unidade": "-"},
            {"Coluna": "game_name", "Descricao": "Nome do jogo (catalogo bireports)", "Unidade": "-"},
            {"Coluna": "game_category", "Descricao": "Categoria do jogo (slots, table, etc)", "Unidade": "-"},
            {"Coluna": "qtd_jogadores", "Descricao": "Jogadores unicos naquela hora/jogo", "Unidade": "count"},
            {"Coluna": "turnover_brl", "Descricao": "Volume total apostado (bets)", "Unidade": "R$"},
            {"Coluna": "wins_brl", "Descricao": "Total pago aos jogadores (wins)", "Unidade": "R$"},
            {"Coluna": "ggr_brl", "Descricao": "Receita da casa (turnover - wins)", "Unidade": "R$"},
            {"Coluna": "rtp_percent", "Descricao": "Return to Player (wins/turnover * 100)", "Unidade": "%"},
            {"Coluna": "win_por_jogador", "Descricao": "Media de wins por jogador", "Unidade": "R$"},
        ])
        legenda.to_excel(writer, sheet_name="Legenda", index=False)

    log.info(f"Excel salvo: {out}")
    print(f"\nPreview (primeiras 20 linhas):\n{df.head(20).to_string(index=False)}")


if __name__ == "__main__":
    main()
