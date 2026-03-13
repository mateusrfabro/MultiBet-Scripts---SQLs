"""
Investigação: buscar detalhes dos jogos ALEA no Redshift para encontrar
padrões de CDN/image_url dos provedores:
  - ALEA_PGSOFT  (FORTUNE SNAKE, CASH MANIA)
  - ALEA_PLAYTECH (ROLETA BRASILEIRA LIVE)
  - ALEA_RUBYPLAY (VOLCANO RISING SE)
  - ALEA_SPRIBE   (AVIATOR)
"""
import sys
sys.path.insert(0, "c:/Users/NITRO/OneDrive - PGX/MultiBet")

from db.redshift import query_redshift

TARGET_GAMES = ['FORTUNE SNAKE', 'CASH MANIA', 'ROLETA BRASILEIRA LIVE', 'VOLCANO RISING SE', 'AVIATOR']
TARGET_SLUGS = ['18949', '14182', '6263', '18649', '8369']

print("=" * 70)
print("1. Todos os campos da view para os 5 jogos")
print("=" * 70)
df1 = query_redshift(f"""
SELECT *
FROM lake.vw_bireports_vendor_games_mapping_data
WHERE UPPER(TRIM(c_game_desc)) IN ({','.join(f"'{g}'" for g in TARGET_GAMES)})
   OR c_game_id IN ({','.join(f"'{s}'" for s in TARGET_SLUGS)})
""")
print(df1.to_string())
print(f"\nColunas: {list(df1.columns)}")

print("\n" + "=" * 70)
print("2. Listando tabelas do schema 'vendor' (e 'lake') com 'image' ou 'icon' no nome")
print("=" * 70)
df2 = query_redshift("""
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema IN ('vendor', 'lake', 'ecr', 'bireports')
  AND (table_name ILIKE '%image%' OR table_name ILIKE '%icon%' OR table_name ILIKE '%cdn%'
       OR table_name ILIKE '%asset%' OR table_name ILIKE '%thumbnail%' OR table_name ILIKE '%game%')
ORDER BY table_schema, table_name
""")
print(df2.to_string())

print("\n" + "=" * 70)
print("3. Colunas de todas as tabelas do schema 'vendor'")
print("=" * 70)
df3 = query_redshift("""
SELECT table_name, column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'vendor'
ORDER BY table_name, ordinal_position
""")
print(df3.to_string())

print("\n" + "=" * 70)
print("4. Busca por colunas com 'image', 'icon', 'url', 'cdn', 'thumb' em todos os schemas relevantes")
print("=" * 70)
df4 = query_redshift("""
SELECT table_schema, table_name, column_name
FROM information_schema.columns
WHERE table_schema IN ('vendor', 'lake', 'ecr', 'bireports')
  AND (column_name ILIKE '%image%' OR column_name ILIKE '%icon%'
       OR column_name ILIKE '%cdn%'  OR column_name ILIKE '%thumb%'
       OR column_name ILIKE '%url%'  OR column_name ILIKE '%asset%')
ORDER BY table_schema, table_name, column_name
""")
print(df4.to_string())

print("\n" + "=" * 70)
print("5. Procurando os slugs numéricos em qualquer tabela de games")
print("=" * 70)
try:
    df5 = query_redshift("""
    SELECT *
    FROM vendor.tbl_vendor_games_mapping_mst
    WHERE c_game_id IN ('18949', '14182', '6263', '18649', '8369')
    LIMIT 20
    """)
    print(df5.to_string())
except Exception as e:
    print(f"Erro: {e}")

print("\n" + "=" * 70)
print("6. Amostras de jogos ALEA_PGSOFT para ver o padrão")
print("=" * 70)
df6 = query_redshift("""
SELECT c_game_desc, c_vendor_id, c_game_id, c_game_image_url
FROM lake.vw_bireports_vendor_games_mapping_data
WHERE c_vendor_id ILIKE 'alea%'
LIMIT 30
""")
print(df6.to_string())
print(f"\nColunas: {list(df6.columns)}")
