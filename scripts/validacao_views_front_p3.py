"""Parte 3 — encontrar fonte horaria para janela 24h rolante."""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.athena import query_athena

def secao(t):
    print("\n" + "=" * 70); print(t); print("=" * 70)


# A. silver_game_15min existe? Tem grão por 15min?
secao("A. silver_game_15min — schema e amostra")
try:
    df = query_athena("SHOW COLUMNS FROM silver.silver_game_15min", database="silver")
    print(df.to_string(index=False))
except Exception as e:
    print(f"FALHOU: {e}")

# B. fund_ec2 — campo timestamp para janela rolante
secao("B. fund_ec2.tbl_real_fund_txn — campos timestamp disponiveis")
df = query_athena("SHOW COLUMNS FROM fund_ec2.tbl_real_fund_txn", database="fund_ec2")
print(df[df['Column'].str.contains('time|date', case=False, na=False)].to_string(index=False))

# C. Top jogos ultimas 24h via fund_ec2 (custo Athena alto, mas valida)
secao("C. TOP 15 jogos por rounds nas ULTIMAS 24h (timestamp BRT) — fund_ec2")
q = """
WITH ultimas_24h AS (
    SELECT
        c_sub_vendor_id AS game_id_proxy,
        COUNT(*) AS rounds
    FROM fund_ec2.tbl_real_fund_txn
    WHERE c_start_time >= (current_timestamp - interval '24' hour)
      AND c_txn_status = 'SUCCESS'
      AND c_product_id = 'CASINO'
      AND c_txn_type = 11  -- Bet
    GROUP BY c_sub_vendor_id
)
SELECT * FROM ultimas_24h ORDER BY rounds DESC LIMIT 15
"""
try:
    df = query_athena(q, database="fund_ec2")
    print(df.to_string(index=False))
except Exception as e:
    print(f"FALHOU: {e}")

# D. silver_game_activity / silver_jogos_jogadores_ativos — alguma com hora?
secao("D. silver — listar tabelas (procurando granularidade horaria)")
df = query_athena("SHOW TABLES IN silver", database="silver")
print(df.to_string(index=False))
