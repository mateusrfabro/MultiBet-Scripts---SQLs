"""
Investigação FINAL: GGR Sports negativo em 11/03/2026 (R$ -319.559,05)

SB txn_types identificados:
  59 = SB_BUYIN (aposta, DB)
  112 = SB_WIN (ganho do jogador, CR)
  89 = SB_LOWERING_BET (devolução parcial, CR)
  61 = SB_BUYIN_CANCEL (cancelamento de aposta, CR)

GGR casa = SB_BUYIN - SB_WIN - SB_LOWERING_BET - SB_BUYIN_CANCEL
         = 1.050.784 - 1.249.408 - 164.461 - 573 ≈ -R$ 363.659

Valores em fund.tbl_real_fund_txn são em centavos / 100.0
Timestamps em UTC → BRT: 11/03 BRT = UTC 2026-03-11 03:00:00 a 2026-03-12 03:00:00
"""

import sys
sys.path.insert(0, r"C:\Users\NITRO\OneDrive - PGX\MultiBet")

import pandas as pd
from db.redshift import query_redshift

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 240)
pd.set_option("display.max_colwidth", 60)
pd.set_option("display.float_format", lambda x: f"{x:,.2f}")

SB_TYPES = [59, 61, 89, 112]  # buyin, buyin_cancel, lowering_bet, win

# ──────────────────────────────────────────────
# 1) Top 20 jogadores — maiores ganhadores (GGR mais negativo para a casa)
# ──────────────────────────────────────────────
print("=" * 140)
print("1) TOP 20 JOGADORES — MAIORES GANHADORES NO SPORTSBOOK EM 11/03/2026 BRT")
print("   GGR casa = SB_BUYIN(59) - SB_WIN(112) - SB_LOWERING_BET(89) - SB_BUYIN_CANCEL(61)")
print("=" * 140)

sql1 = """
SELECT
    f.c_ecr_id,
    e.c_external_id,
    e.c_email_id,
    SUM(CASE WHEN f.c_txn_type = 59 THEN f.c_amount_in_ecr_ccy ELSE 0 END) / 100.0 AS sb_buyin,
    SUM(CASE WHEN f.c_txn_type = 112 THEN f.c_amount_in_ecr_ccy ELSE 0 END) / 100.0 AS sb_win,
    SUM(CASE WHEN f.c_txn_type = 89 THEN f.c_amount_in_ecr_ccy ELSE 0 END) / 100.0 AS sb_lowering,
    SUM(CASE WHEN f.c_txn_type = 61 THEN f.c_amount_in_ecr_ccy ELSE 0 END) / 100.0 AS sb_cancel,
    (SUM(CASE WHEN f.c_txn_type = 59 THEN f.c_amount_in_ecr_ccy ELSE 0 END)
     - SUM(CASE WHEN f.c_txn_type IN (112, 89, 61) THEN f.c_amount_in_ecr_ccy ELSE 0 END)
    ) / 100.0 AS ggr_casa,
    COUNT(*) AS total_txns
FROM fund.tbl_real_fund_txn f
LEFT JOIN ecr.tbl_ecr e ON e.c_ecr_id = f.c_ecr_id
WHERE f.c_start_time >= '2026-03-11 03:00:00'
  AND f.c_start_time <  '2026-03-12 03:00:00'
  AND f.c_txn_status = 'SUCCESS'
  AND f.c_txn_type IN (59, 61, 89, 112)
GROUP BY f.c_ecr_id, e.c_external_id, e.c_email_id
ORDER BY ggr_casa ASC
LIMIT 20;
"""
df1 = query_redshift(sql1)
print(df1.to_string(index=False))
print()

# Soma do top 20
if len(df1) > 0:
    top20_ggr = df1["ggr_casa"].sum()
    print(f"Soma GGR Top 20 ganhadores: R$ {top20_ggr:,.2f}")
    print()

# ──────────────────────────────────────────────
# 2) Concentração do prejuízo
# ──────────────────────────────────────────────
print("=" * 140)
print("2) CONCENTRAÇÃO: GANHADORES vs PERDEDORES NO DIA")
print("=" * 140)

sql2 = """
WITH player_ggr AS (
    SELECT
        f.c_ecr_id,
        (SUM(CASE WHEN f.c_txn_type = 59 THEN f.c_amount_in_ecr_ccy ELSE 0 END)
         - SUM(CASE WHEN f.c_txn_type IN (112, 89, 61) THEN f.c_amount_in_ecr_ccy ELSE 0 END)
        ) / 100.0 AS ggr_casa
    FROM fund.tbl_real_fund_txn f
    WHERE f.c_start_time >= '2026-03-11 03:00:00'
      AND f.c_start_time <  '2026-03-12 03:00:00'
      AND f.c_txn_status = 'SUCCESS'
      AND f.c_txn_type IN (59, 61, 89, 112)
    GROUP BY f.c_ecr_id
)
SELECT
    COUNT(*) AS total_jogadores,
    SUM(ggr_casa) AS ggr_total,
    SUM(CASE WHEN ggr_casa < 0 THEN 1 ELSE 0 END) AS jogadores_ganharam,
    SUM(CASE WHEN ggr_casa < 0 THEN ggr_casa ELSE 0 END) AS ggr_ganhadores,
    SUM(CASE WHEN ggr_casa >= 0 THEN 1 ELSE 0 END) AS jogadores_perderam,
    SUM(CASE WHEN ggr_casa >= 0 THEN ggr_casa ELSE 0 END) AS ggr_perdedores
FROM player_ggr;
"""
df2 = query_redshift(sql2)
print(df2.to_string(index=False))
print()

# ──────────────────────────────────────────────
# 3) Distribuição por faixa de GGR
# ──────────────────────────────────────────────
print("=" * 140)
print("3) DISTRIBUIÇÃO POR FAIXA DE GGR (PERSPECTIVA CASA)")
print("=" * 140)

sql3 = """
WITH player_ggr AS (
    SELECT
        f.c_ecr_id,
        (SUM(CASE WHEN f.c_txn_type = 59 THEN f.c_amount_in_ecr_ccy ELSE 0 END)
         - SUM(CASE WHEN f.c_txn_type IN (112, 89, 61) THEN f.c_amount_in_ecr_ccy ELSE 0 END)
        ) / 100.0 AS ggr_casa
    FROM fund.tbl_real_fund_txn f
    WHERE f.c_start_time >= '2026-03-11 03:00:00'
      AND f.c_start_time <  '2026-03-12 03:00:00'
      AND f.c_txn_status = 'SUCCESS'
      AND f.c_txn_type IN (59, 61, 89, 112)
    GROUP BY f.c_ecr_id
)
SELECT
    CASE
        WHEN ggr_casa < -50000 THEN 'A) < -R$50k (GRANDES ganhadores)'
        WHEN ggr_casa < -10000 THEN 'B) -R$50k a -R$10k'
        WHEN ggr_casa < -5000  THEN 'C) -R$10k a -R$5k'
        WHEN ggr_casa < -1000  THEN 'D) -R$5k a -R$1k'
        WHEN ggr_casa < 0      THEN 'E) -R$1k a R$0'
        WHEN ggr_casa < 1000   THEN 'F) R$0 a R$1k (perderam pouco)'
        WHEN ggr_casa < 5000   THEN 'G) R$1k a R$5k'
        ELSE                        'H) > R$5k (perderam muito)'
    END AS faixa,
    COUNT(*) AS jogadores,
    SUM(ggr_casa) AS ggr_total
FROM player_ggr
GROUP BY faixa
ORDER BY faixa;
"""
df3 = query_redshift(sql3)
print(df3.to_string(index=False))
print()

# ──────────────────────────────────────────────
# 4) Maiores transações individuais de SB_WIN (112)
# ──────────────────────────────────────────────
print("=" * 140)
print("4) TOP 30 MAIORES SB_WIN INDIVIDUAIS (ganhos do jogador)")
print("=" * 140)

sql4 = """
SELECT
    f.c_ecr_id,
    e.c_external_id,
    e.c_email_id,
    f.c_amount_in_ecr_ccy / 100.0 AS valor_brl,
    f.c_game_id,
    CONVERT_TIMEZONE('UTC', 'America/Sao_Paulo', f.c_start_time) AS hora_brt
FROM fund.tbl_real_fund_txn f
LEFT JOIN ecr.tbl_ecr e ON e.c_ecr_id = f.c_ecr_id
WHERE f.c_start_time >= '2026-03-11 03:00:00'
  AND f.c_start_time <  '2026-03-12 03:00:00'
  AND f.c_txn_status = 'SUCCESS'
  AND f.c_txn_type = 112
ORDER BY f.c_amount_in_ecr_ccy DESC
LIMIT 30;
"""
df4 = query_redshift(sql4)
print(df4.to_string(index=False))
print()

# ──────────────────────────────────────────────
# 5) Maiores transações de SB_LOWERING_BET (89)
# ──────────────────────────────────────────────
print("=" * 140)
print("5) TOP 20 MAIORES SB_LOWERING_BET (89) — devoluções parciais")
print("=" * 140)

sql5 = """
SELECT
    f.c_ecr_id,
    e.c_external_id,
    f.c_amount_in_ecr_ccy / 100.0 AS valor_brl,
    f.c_game_id,
    CONVERT_TIMEZONE('UTC', 'America/Sao_Paulo', f.c_start_time) AS hora_brt
FROM fund.tbl_real_fund_txn f
LEFT JOIN ecr.tbl_ecr e ON e.c_ecr_id = f.c_ecr_id
WHERE f.c_start_time >= '2026-03-11 03:00:00'
  AND f.c_start_time <  '2026-03-12 03:00:00'
  AND f.c_txn_status = 'SUCCESS'
  AND f.c_txn_type = 89
ORDER BY f.c_amount_in_ecr_ccy DESC
LIMIT 20;
"""
df5 = query_redshift(sql5)
print(df5.to_string(index=False))
print()

# ──────────────────────────────────────────────
# 6) Comparação: GGR Sports diário últimos 10 dias
# ──────────────────────────────────────────────
print("=" * 140)
print("6) COMPARAÇÃO: GGR SPORTS DIÁRIO ÚLTIMOS 10 DIAS")
print("=" * 140)

sql6 = """
SELECT
    DATE(CONVERT_TIMEZONE('UTC', 'America/Sao_Paulo', f.c_start_time)) AS dia_brt,
    COUNT(DISTINCT f.c_ecr_id) AS jogadores,
    SUM(CASE WHEN f.c_txn_type = 59 THEN f.c_amount_in_ecr_ccy ELSE 0 END) / 100.0 AS sb_buyin,
    SUM(CASE WHEN f.c_txn_type = 112 THEN f.c_amount_in_ecr_ccy ELSE 0 END) / 100.0 AS sb_win,
    SUM(CASE WHEN f.c_txn_type = 89 THEN f.c_amount_in_ecr_ccy ELSE 0 END) / 100.0 AS sb_lowering,
    (SUM(CASE WHEN f.c_txn_type = 59 THEN f.c_amount_in_ecr_ccy ELSE 0 END)
     - SUM(CASE WHEN f.c_txn_type IN (112, 89, 61) THEN f.c_amount_in_ecr_ccy ELSE 0 END)
    ) / 100.0 AS ggr_casa,
    COUNT(*) AS total_txns
FROM fund.tbl_real_fund_txn f
WHERE f.c_start_time >= '2026-03-02 03:00:00'
  AND f.c_start_time <  '2026-03-12 03:00:00'
  AND f.c_txn_status = 'SUCCESS'
  AND f.c_txn_type IN (59, 61, 89, 112)
GROUP BY dia_brt
ORDER BY dia_brt;
"""
df6 = query_redshift(sql6)
print(df6.to_string(index=False))
print()

# ──────────────────────────────────────────────
# 7) Top 5 ganhadores — histórico nos últimos 7 dias
# ──────────────────────────────────────────────
print("=" * 140)
print("7) TOP 5 GANHADORES DE 11/03 — HISTÓRICO NOS ÚLTIMOS 7 DIAS")
print("=" * 140)

if len(df1) >= 5:
    top5_ecr = df1.head(5)["c_ecr_id"].tolist()
    ecr_str = ", ".join([str(x) for x in top5_ecr])

    sql7 = f"""
    SELECT
        DATE(CONVERT_TIMEZONE('UTC', 'America/Sao_Paulo', f.c_start_time)) AS dia_brt,
        f.c_ecr_id,
        SUM(CASE WHEN f.c_txn_type = 59 THEN f.c_amount_in_ecr_ccy ELSE 0 END) / 100.0 AS sb_buyin,
        SUM(CASE WHEN f.c_txn_type = 112 THEN f.c_amount_in_ecr_ccy ELSE 0 END) / 100.0 AS sb_win,
        (SUM(CASE WHEN f.c_txn_type = 59 THEN f.c_amount_in_ecr_ccy ELSE 0 END)
         - SUM(CASE WHEN f.c_txn_type IN (112, 89, 61) THEN f.c_amount_in_ecr_ccy ELSE 0 END)
        ) / 100.0 AS ggr_casa
    FROM fund.tbl_real_fund_txn f
    WHERE f.c_ecr_id IN ({ecr_str})
      AND f.c_start_time >= '2026-03-05 03:00:00'
      AND f.c_start_time <  '2026-03-12 03:00:00'
      AND f.c_txn_status = 'SUCCESS'
      AND f.c_txn_type IN (59, 61, 89, 112)
    GROUP BY dia_brt, f.c_ecr_id
    ORDER BY f.c_ecr_id, dia_brt;
    """
    df7 = query_redshift(sql7)
    print(df7.to_string(index=False))
    print()

# ──────────────────────────────────────────────
# 8) Top 5 ganhadores — depósitos vs saques no dia (contexto financeiro)
# ──────────────────────────────────────────────
print("=" * 140)
print("8) TOP 5 GANHADORES — DEPÓSITOS E SAQUES NO DIA 11/03")
print("=" * 140)

if len(df1) >= 5:
    sql8 = f"""
    SELECT
        f.c_ecr_id,
        SUM(CASE WHEN f.c_txn_type = 1 THEN f.c_amount_in_ecr_ccy ELSE 0 END) / 100.0 AS depositos,
        SUM(CASE WHEN f.c_txn_type = 2 THEN f.c_amount_in_ecr_ccy ELSE 0 END) / 100.0 AS saques,
        SUM(CASE WHEN f.c_txn_type = 27 THEN f.c_amount_in_ecr_ccy ELSE 0 END) / 100.0 AS casino_bet,
        SUM(CASE WHEN f.c_txn_type = 45 THEN f.c_amount_in_ecr_ccy ELSE 0 END) / 100.0 AS casino_win
    FROM fund.tbl_real_fund_txn f
    WHERE f.c_ecr_id IN ({ecr_str})
      AND f.c_start_time >= '2026-03-11 03:00:00'
      AND f.c_start_time <  '2026-03-12 03:00:00'
      AND f.c_txn_status = 'SUCCESS'
    GROUP BY f.c_ecr_id
    ORDER BY f.c_ecr_id;
    """
    df8 = query_redshift(sql8)
    print(df8.to_string(index=False))
    print()

print("=" * 140)
print("INVESTIGAÇÃO CONCLUÍDA")
print("=" * 140)
