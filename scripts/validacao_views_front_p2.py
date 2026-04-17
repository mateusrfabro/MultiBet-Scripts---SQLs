"""Parte 2 da validacao — corrigir colunas que falharam."""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.athena import query_athena
from db.supernova import execute_supernova


def secao(titulo):
    print("\n" + "=" * 70)
    print(titulo)
    print("=" * 70)


# 3 corrigido: vendor sem sub_vendor (nao existe na view bireports)
secao("3. VENDORS top 20 (CASINO ativos) — view bireports nao tem sub_vendor")

q3 = """
SELECT
    c_vendor_id           AS vendor,
    LOWER(c_game_category) AS category,
    COUNT(*)              AS qtd_jogos
FROM bireports_ec2.tbl_vendor_games_mapping_data
WHERE c_status = 'active'
  AND UPPER(c_product_id) = 'CASINO'
GROUP BY 1, 2
ORDER BY qtd_jogos DESC
LIMIT 25
"""
df = query_athena(q3, database="bireports_ec2")
print(df.to_string(index=False))


# 3b — sub_vendor existe na MST do vendor_ec2
secao("3b. SUB_VENDOR via vendor_ec2.tbl_vendor_games_mapping_mst (Pragmatic = pgsoft)")

q3b = """
SELECT
    c_vendor_id, c_sub_vendor_id,
    COUNT(DISTINCT c_game_id) AS qtd_games
FROM vendor_ec2.tbl_vendor_games_mapping_mst
WHERE c_status = 'active'
  AND UPPER(c_product_id) = 'CASINO'
GROUP BY 1, 2
ORDER BY qtd_games DESC
LIMIT 30
"""
df = query_athena(q3b, database="vendor_ec2")
print(df.to_string(index=False))


# 4. Top mais jogados ultimos 7 dias
secao("4. TOP 20 jogos por RODADAS (ultimos 7d) — fonte 'mais jogados da semana'")

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
    COUNT(DISTINCT fca.player_id) AS qty_players
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


# 5. Cobertura — quantos jogos ativos sem imagem
secao("5. COBERTURA game_image_mapping vs jogos ativos (ultimos 7 dias)")

q5 = """
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
df_act = query_athena(q5, database="ps_bi")
print(f"Jogos com atividade nos ultimos 7d (Athena ps_bi): {len(df_act)}")

rows = execute_supernova("""
    SELECT
        COUNT(*) AS total_mapeados,
        COUNT(game_image_url) AS com_imagem,
        COUNT(provider_game_id) AS com_provider_id
    FROM multibet.game_image_mapping
""", fetch=True)
total, com_img, com_pid = rows[0]
print(f"game_image_mapping: total={total} | com imagem={com_img} | com provider_game_id={com_pid}")

nomes_ativos = set(df_act["game_name"].dropna().str.upper().str.strip())
mapeados = execute_supernova(
    "SELECT game_name_upper FROM multibet.game_image_mapping WHERE game_image_url IS NOT NULL",
    fetch=True
)
nomes_mapeados = set(r[0] for r in mapeados)

ativos_sem_img = nomes_ativos - nomes_mapeados
print(f"Ativos COM imagem: {len(nomes_ativos & nomes_mapeados)} | SEM imagem: {len(ativos_sem_img)}")
if ativos_sem_img:
    print("Exemplos sem imagem (primeiros 15):")
    for n in sorted(ativos_sem_img)[:15]:
        print(f"   - {n}")


# 6. Estado atual do dim_games_catalog (esta populado?)
secao("6. dim_games_catalog (Super Nova) — populacao atual")

rows = execute_supernova("""
    SELECT
        COUNT(*) AS total,
        COUNT(DISTINCT vendor_id) AS qtd_vendors,
        COUNT(DISTINCT game_category) AS qtd_categorias,
        MAX(refreshed_at) AS ultima_atualizacao
    FROM multibet.dim_games_catalog
""", fetch=True)
if rows:
    t, v, c, r = rows[0]
    print(f"Total jogos: {t} | Vendors: {v} | Categorias: {c} | Ultima: {r}")

print("\nBreakdown por categoria:")
rows = execute_supernova("""
    SELECT game_category, COUNT(*) AS qtd
    FROM multibet.dim_games_catalog
    GROUP BY game_category
    ORDER BY qtd DESC
""", fetch=True)
for cat, q in rows:
    print(f"  {cat or '(NULL)':<30} {q}")


# 7. Existe alguma view ja chamada view_front_*?
secao("7. Views existentes em multibet.* (procurando view_front_*)")

rows = execute_supernova("""
    SELECT table_name, table_type
    FROM information_schema.tables
    WHERE table_schema = 'multibet'
      AND table_type IN ('VIEW', 'BASE TABLE')
    ORDER BY table_type, table_name
""", fetch=True)
for nm, tp in rows:
    print(f"  [{tp[:6]}] {nm}")

print("\n=== Validacao parte 2 concluida ===")
