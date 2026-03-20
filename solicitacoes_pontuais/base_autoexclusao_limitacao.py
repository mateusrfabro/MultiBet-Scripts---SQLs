"""
Base temporal: Autoexclusão e Limitação Temporária — últimos 30 dias.

Demanda: base com data de solicitação, tipo (autoexclusão definida/indefinida
ou limitação temporária), período, ExtID, GGR total do jogador e data de signup.

Fonte: ecr.tbl_rg_cool_off   → autoexclusões definidas + limitações temporárias
       ecr.tbl_ecr_category  → autoexclusões indefinidas (mudança de categoria para rg_closed)
       ecr.tbl_ecr            → signup, external_id
       bireports.tbl_ecr_wise_daily_bi_summary → GGR lifetime

Autor: Mateus Fabro
Data : 2026-03-11

v6 (2026-03-11): Fonte de indefinidas trocada para tbl_ecr_category.
    A tbl_ecr_rg_closed parou de receber registros após 03/Mar/2026 devido a
    mudança no front-end (confirmado pela IA da PGS). A tbl_ecr_category é a
    fonte universal — registra toda mudança de categoria, independente do endpoint.
    Filtro: c_category = 'rg_closed' AND c_change_source = 'rg_close_request'.
    Timestamps convertidos de UTC → America/Sao_Paulo.
"""

import sys
import os
import logging
from datetime import datetime
import pandas as pd

# Garante que o import do db/ funcione independente de onde rodar
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from db.redshift import query_redshift

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


SQL = """
-- =============================================================
-- Base temporal: Autoexclusão e Limitação Temporária (30 dias)
-- =============================================================
-- Fonte 1: ecr.tbl_rg_cool_off   → definidas + limitações temporárias
-- Fonte 2: ecr.tbl_ecr_category  → indefinidas (mudança para rg_closed)
--          ecr.tbl_ecr            → external_id + signup
--          bireports.tbl_ecr_wise_daily_bi_summary → GGR lifetime
-- =============================================================
-- v6 (2026-03-11):
--   Fonte de indefinidas: tbl_ecr_category (fonte universal de mudança
--   de categoria, confirmada pela IA da PGS como a mais confiável).
--   A tbl_ecr_rg_closed parou de receber registros após 03/Mar/2026
--   devido a mudança no front-end da MultiBet.
--   Timestamps convertidos de UTC → America/Sao_Paulo.
-- =============================================================

WITH

-- =============================================
-- FONTE 1: tbl_rg_cool_off (definidas + temporárias)
-- =============================================
solicitacoes_cool_off AS (
    SELECT
        co.c_ecr_id,
        CONVERT_TIMEZONE('America/Sao_Paulo', co.c_start_time) AS data_solicitacao,
        co.c_status,
        CONVERT_TIMEZONE('America/Sao_Paulo', co.c_end_time)   AS c_end_time,

        -- Tipo de solicitação
        CASE
            WHEN co.c_type = 'COOL_OFF'       THEN 'Limitação Temporária'
            WHEN co.c_type = 'SELF_EXCLUSION' THEN 'Autoexclusão Definida'
            ELSE co.c_type
        END AS tipo_solicitacao,

        -- Período legível
        CASE
            WHEN co.c_length = 86400     THEN '24h'
            WHEN co.c_length = 259200    THEN '3d'
            WHEN co.c_length = 604800    THEN '1w'
            WHEN co.c_length = 2592000   THEN '1m'
            WHEN co.c_length = 15724800  THEN '6m'
            WHEN co.c_length = 31536000  THEN '1y'
            WHEN co.c_length = 157680000 THEN '5y'
            WHEN co.c_length = 0 THEN
                CASE
                    WHEN DATEDIFF(hour, co.c_start_time, co.c_end_time) <= 48   THEN '24h'
                    WHEN DATEDIFF(hour, co.c_start_time, co.c_end_time) <= 96   THEN '3d'
                    WHEN DATEDIFF(hour, co.c_start_time, co.c_end_time) <= 240  THEN '1w'
                    WHEN DATEDIFF(hour, co.c_start_time, co.c_end_time) <= 1440 THEN '1m'
                    WHEN DATEDIFF(hour, co.c_start_time, co.c_end_time) <= 6480 THEN '6m'
                    WHEN DATEDIFF(hour, co.c_start_time, co.c_end_time) <= 12960 THEN '1y'
                    WHEN DATEDIFF(hour, co.c_start_time, co.c_end_time) > 12960 THEN '5y'
                    ELSE 'N/A'
                END
            ELSE ROUND(co.c_length / 86400.0, 0)::INT::VARCHAR || 'd'
        END AS periodo_solicitacao

    FROM ecr.tbl_rg_cool_off co
    WHERE CONVERT_TIMEZONE('America/Sao_Paulo', co.c_start_time) >= DATEADD(day, -30, CONVERT_TIMEZONE('America/Sao_Paulo', GETDATE()))
      AND LOWER(COALESCE(co.c_comments, '')) NOT LIKE 'test%'
      AND LOWER(COALESCE(co.c_comments, '')) <> 'teste'
),

-- =============================================
-- FONTE 2: tbl_ecr_category (indefinidas)
-- =============================================
-- Toda mudança de categoria do jogador é registrada aqui.
-- Quando a conta é fechada por Jogo Responsável, c_category = 'rg_closed'.
-- c_change_source = 'rg_close_request' → fechamento automático/solicitado
-- c_change_source = 'manual_cs_agent'  → fechamento manual pelo CS
-- Esta é a fonte universal (confirmada pela IA da PGS) que cobre
-- todo o período, independente de mudanças no front-end.
-- IMPORTANTE: usar c_last_modified_date (data da mudança de categoria),
-- NÃO c_creation_date (que é a data de criação do registro/signup).
-- Confirmado pela PGS: tbl_ecr_category tem 1 linha por jogador (PK = c_ecr_id),
-- c_creation_date = data do INSERT original, c_last_modified_date = data do UPDATE.
-- =============================================
solicitacoes_indefinidas AS (
    SELECT
        cat.c_ecr_id,
        CONVERT_TIMEZONE('America/Sao_Paulo', cat.c_last_modified_date) AS data_solicitacao,
        'active'                                AS c_status,
        NULL::TIMESTAMP                         AS c_end_time,
        'Autoexclusão Indefinida'               AS tipo_solicitacao,
        'Indefinida'                            AS periodo_solicitacao
    FROM ecr.tbl_ecr_category cat
    WHERE CONVERT_TIMEZONE('America/Sao_Paulo', cat.c_last_modified_date) >= DATEADD(day, -30, CONVERT_TIMEZONE('America/Sao_Paulo', GETDATE()))
      AND cat.c_category = 'rg_closed'
      AND cat.c_change_source IN ('rg_close_request', 'manual_cs_agent')
),

-- =============================================
-- UNION: todas as solicitações
-- =============================================
todas_solicitacoes AS (
    SELECT * FROM solicitacoes_cool_off
    UNION ALL
    SELECT * FROM solicitacoes_indefinidas
),

-- GGR lifetime por jogador
ggr AS (
    SELECT
        s.c_ecr_id,
        SUM(
            COALESCE(s.c_casino_bet_amount, 0) - COALESCE(s.c_casino_win_amount, 0)
          + COALESCE(s.c_sb_bet_amount, 0)     - COALESCE(s.c_sb_win_amount, 0)
          + COALESCE(s.c_bt_bet_amount, 0)     - COALESCE(s.c_bt_win_amount, 0)
          + COALESCE(s.c_bingo_bet_amount, 0)  - COALESCE(s.c_bingo_win_amount, 0)
          + COALESCE(s.c_lottery_bet_amount, 0)- COALESCE(s.c_lottery_win_amount, 0)
          + COALESCE(s.c_fantasy_bet_amount, 0)- COALESCE(s.c_fantasy_win_amount, 0)
        ) / 100.0 AS ggr_total_brl
    FROM bireports.tbl_ecr_wise_daily_bi_summary s
    GROUP BY s.c_ecr_id
)

SELECT
    sol.data_solicitacao,
    sol.tipo_solicitacao,
    sol.periodo_solicitacao,
    sol.c_status                                AS status_solicitacao,
    CAST(e.c_external_id AS VARCHAR)            AS ext_id,
    CAST(sol.c_ecr_id AS VARCHAR)               AS c_ecr_id,
    ROUND(COALESCE(g.ggr_total_brl, 0), 2)     AS ggr_total_brl,
    CONVERT_TIMEZONE('America/Sao_Paulo', e.c_signup_time) AS data_signup,
    sol.c_end_time                              AS data_fim_restricao

FROM todas_solicitacoes sol
LEFT JOIN ecr.tbl_ecr e
    ON e.c_ecr_id = sol.c_ecr_id
LEFT JOIN ggr g
    ON g.c_ecr_id = sol.c_ecr_id

ORDER BY sol.data_solicitacao DESC
"""


def main():
    log.info("Iniciando extração: base autoexclusão + limitação temporária (últimos 30 dias)")

    df = query_redshift(SQL)
    log.info(f"Registros retornados: {len(df)}")

    if df.empty:
        log.warning("Nenhum registro encontrado nos últimos 30 dias.")
        return

    # Resumo rápido
    log.info("Distribuição por tipo de solicitação:")
    print(df["tipo_solicitacao"].value_counts().to_string())
    print()
    log.info("Distribuição por período:")
    print(df["periodo_solicitacao"].value_counts().to_string())
    print()

    # Cruzamento tipo x período para validação
    log.info("Cruzamento tipo x período:")
    cross = df.groupby(["tipo_solicitacao", "periodo_solicitacao"]).size().reset_index(name="qtd")
    print(cross.sort_values(["tipo_solicitacao", "qtd"], ascending=[True, False]).to_string(index=False))

    # Salvar CSV — separador ; e GGR com vírgula decimal para Excel BR
    output_dir = os.path.join(os.path.dirname(__file__), "..", "output")
    os.makedirs(output_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"base_autoexclusao_limitacao_30d_{timestamp}.csv"
    filepath = os.path.join(output_dir, filename)

    # Garantir que IDs sejam string pura (sem .0)
    df["ext_id"] = df["ext_id"].fillna("").astype(str).str.replace(r"\.0$", "", regex=True)
    df["c_ecr_id"] = df["c_ecr_id"].fillna("").astype(str).str.replace(r"\.0$", "", regex=True)

    # GGR: ponto → vírgula para Excel BR
    df["ggr_total_brl"] = df["ggr_total_brl"].apply(lambda x: str(x).replace(".", ","))

    df.to_csv(filepath, index=False, encoding="utf-8-sig", sep=";")
    log.info(f"Arquivo salvo: {filepath}")
    log.info(f"Total de jogadores únicos: {df['ext_id'].nunique()}")


if __name__ == "__main__":
    main()