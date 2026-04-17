"""
Validacao empirica: o que ja temos no Athena para montar as views do front.

Pergunta do CTO/Castrin:
    1. Conseguimos categorizar Cassino ao Vivo em Roleta / Blackjack / Baccarat?
    2. Conseguimos rankear "mais jogados da semana"?
    3. Temos vendor / sub-vendor / categoria para filtros tipo "Jogos Pragmatic"?
    4. game_image_mapping cobre todos os jogos com atividade real?

Roda em <=15s e gera output texto para anexar ao plano.
"""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.athena import query_athena
from db.supernova import execute_supernova


def secao(titulo):
    print("\n" + "=" * 70)
    print(titulo)
    print("=" * 70)


# ---------------------------------------------------------------------------
# 1. Categorias e tipos no catalogo (bireports — fonte oficial Pragmatic)
# ---------------------------------------------------------------------------
secao("1. CATEGORIAS DISTINTAS (bireports_ec2.tbl_vendor_games_mapping_data)")

q1 = """
SELECT
    UPPER(c_product_id)       AS product,
    LOWER(c_game_category)    AS game_category,
    c_game_category_desc      AS category_desc,
    COUNT(*)                  AS qtd_jogos
FROM bireports_ec2.tbl_vendor_games_mapping_data
WHERE c_status = 'active'
GROUP BY 1, 2, 3
ORDER BY product, qtd_jogos DESC
"""
df = query_athena(q1, database="bireports_ec2")
print(df.to_string(index=False))


# ---------------------------------------------------------------------------
# 2. Tipos de jogo (game_type_desc) — para LIVE: roleta/blackjack/baccarat?
# ---------------------------------------------------------------------------
secao("2. GAME_TYPE_DESC para CASINO LIVE (Roleta / Blackjack / Baccarat?)")

q2 = """
SELECT
    LOWER(c_game_category)        AS category,
    c_game_type_desc              AS game_type_desc,
    COUNT(*)                      AS qtd_jogos
FROM bireports_ec2.tbl_vendor_games_mapping_data
WHERE c_status = 'active'
  AND UPPER(c_product_id) = 'CASINO'
  AND LOWER(c_game_category) IN ('live', 'livecasino', 'live casino')
GROUP BY 1, 2
ORDER BY qtd_jogos DESC
"""
df = query_athena(q2, database="bireports_ec2")
print(df.to_string(index=False))


# ---------------------------------------------------------------------------
# 3. Vendors e sub-vendors mais relevantes (filtro "Jogos Pragmatic")
# ---------------------------------------------------------------------------
secao("3. VENDOR / SUB_VENDOR (top 20 — para filtros tipo 'Jogos Pragmatic')")

q3 = """
SELECT
    c_vendor_id           AS vendor,
    c_sub_vendor_id       AS sub_vendor,
    COUNT(*)              AS qtd_jogos
FROM bireports_ec2.tbl_vendor_games_mapping_data
WHERE c_status = 'active'
  AND UPPER(c_product_id) = 'CASINO'
GROUP BY 1, 2
ORDER BY qtd_jogos DESC
LIMIT 20
"""
df = query_athena(q3, database="bireports_ec2")
print(df.to_string(index=False))


# ---------------------------------------------------------------------------
# 4. "Mais jogados" — top 20 da ULTIMA SEMANA por rounds
# ---------------------------------------------------------------------------
secao("4. TOP 20 JOGOS por RODADAS (ultimos 7 dias) — fonte 'mais jogados da semana'")

q4 = """
WITH valid_players AS (
    SELECT c_ecr_id FROM bireports_ec2.tbl_ecr WHERE c_test_user = false
)
SELECT
    fca.game_id,
    MAX(dg.game_desc)         AS game_name,
    MAX(dg.vendor_id)         AS vendor,
    MAX(dg.game_category)     AS category,
    SUM(fca.bet_count)        AS total_rounds,
    COUNT(DISTINCT fca.player_id) AS qty_players,
    ROUND(SUM(fca.bet_amount_local), 2)  AS turnover_brl
FROM ps_bi.fct_casino_activity_daily fca
LEFT JOIN ps_bi.dim_game dg ON fca.game_id = dg.game_id
JOIN valid_players vp ON fca.player_id = vp.c_ecr_id
WHERE fca.activity_date >= date_add('day', -7, current_date)
  AND fca.activity_date < current_date
  AND LOWER(fca.product_id) = 'casino'
GROUP BY fca.game_id
ORDER BY total_rounds DESC
LIMIT 20
"""
df = query_athena(q4, database="ps_bi")
print(df.to_string(index=False))


# ---------------------------------------------------------------------------
# 5. Cobertura — quantos jogos tem atividade na ultima semana mas SEM imagem
# ---------------------------------------------------------------------------
secao("5. COBERTURA game_image_mapping vs jogos ativos (ultimos 7 dias)")

# Top jogos ativos
q5_activos = """
SELECT
    fca.game_id,
    MAX(dg.game_desc) AS game_name
FROM ps_bi.fct_casino_activity_daily fca
LEFT JOIN ps_bi.dim_game dg ON fca.game_id = dg.game_id
WHERE fca.activity_date >= date_add('day', -7, current_date)
  AND fca.activity_date < current_date
  AND LOWER(fca.product_id) = 'casino'
GROUP BY fca.game_id
HAVING SUM(fca.bet_count) > 0
"""
df_act = query_athena(q5_activos, database="ps_bi")
print(f"Jogos com atividade nos ultimos 7d (Athena): {len(df_act)}")

# Cobertura no Super Nova DB
rows = execute_supernova("""
    SELECT
        COUNT(*) AS total_mapeados,
        COUNT(game_image_url) AS com_imagem,
        COUNT(provider_game_id) AS com_provider_id
    FROM multibet.game_image_mapping
""", fetch=True)
total, com_img, com_pid = rows[0]
print(f"game_image_mapping: total={total} | com imagem={com_img} | com provider_game_id={com_pid}")

# Cruzamento — quantos dos ativos tem provider_game_id no mapping?
nomes_ativos = set(df_act["game_name"].dropna().str.upper().str.strip())
mapeados = execute_supernova(
    "SELECT game_name_upper FROM multibet.game_image_mapping WHERE game_image_url IS NOT NULL",
    fetch=True
)
nomes_mapeados = set(r[0] for r in mapeados)

ativos_com_img = nomes_ativos & nomes_mapeados
ativos_sem_img = nomes_ativos - nomes_mapeados
print(f"Ativos COM imagem: {len(ativos_com_img)} | SEM imagem: {len(ativos_sem_img)}")
if ativos_sem_img:
    print(f"Exemplos sem imagem (primeiros 10):")
    for n in sorted(ativos_sem_img)[:10]:
        print(f"   - {n}")


# ---------------------------------------------------------------------------
# 6. Sample de jogos LIVE no catalogo (para validar nomenclatura)
# ---------------------------------------------------------------------------
secao("6. SAMPLE 30 jogos LIVE (para conferir mapeamento Roleta/BJ/Baccarat)")

q6 = """
SELECT
    c_game_id, c_game_desc, c_vendor_id, c_sub_vendor_id,
    c_game_type_desc, c_game_category_desc
FROM bireports_ec2.tbl_vendor_games_mapping_data
WHERE c_status = 'active'
  AND UPPER(c_product_id) = 'CASINO'
  AND LOWER(c_game_category) = 'live'
ORDER BY c_game_desc
LIMIT 30
"""
df = query_athena(q6, database="bireports_ec2")
print(df.to_string(index=False))


print("\n=== Validacao concluida ===")
