"""Smoke test final: amostras das views + investigacao do has_jackpot=0."""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.supernova import execute_supernova
from db.athena import query_athena


def secao(t):
    print("\n" + "=" * 70); print(t); print("=" * 70)


# 1. Top 15 do vw_front_top_24h (com horario do refresh)
secao("1. vw_front_top_24h — TOP 15 (mais jogados ultimas 24h)")
rows = execute_supernova("""
    SELECT rank, game_name, vendor, category, live_subtype, rounds_24h, players_24h, window_end_utc
    FROM multibet.vw_front_top_24h
    ORDER BY rank
    LIMIT 15
""", fetch=True)
print(f"{'rank':>4}  {'game_name':<28} {'vendor':<18} {'cat':<6} {'subtype':<10} {'rounds':>10} {'players':>8}")
for r, n, v, c, ls, rd, pl, we in rows:
    print(f"{r:>4}  {n[:28]:<28} {(v or '')[:18]:<18} {(c or '')[:6]:<6} {(ls or '')[:10]:<10} {rd:>10} {pl:>8}")
print(f"\nJanela atualizada: {rows[0][7]} UTC" if rows else "")


# 2. vw_front_live_casino agrupado por subtipo
secao("2. vw_front_live_casino — distribuicao por live_subtype")
rows = execute_supernova("""
    SELECT live_subtype, COUNT(*) AS qtd
    FROM multibet.vw_front_live_casino
    GROUP BY live_subtype
    ORDER BY qtd DESC
""", fetch=True)
for s, q in rows:
    print(f"  {s or '(NULL)':<15} {q:>5}")


# 3. Top 5 por vendor (Pragmatic, alea_redtiger, etc)
secao("3. vw_front_by_vendor — top 5 vendors por volume de jogos")
rows = execute_supernova("""
    SELECT vendor, COUNT(*) AS qtd
    FROM multibet.vw_front_by_vendor
    GROUP BY vendor
    ORDER BY qtd DESC
    LIMIT 10
""", fetch=True)
for v, q in rows:
    print(f"  {v:<25} {q:>5}")


# 4. INVESTIGAR vw_front_jackpot=0 — checar se has_jackpot foi populado
secao("4. INVESTIGACAO has_jackpot (vw_front_jackpot=0)")
rows = execute_supernova("""
    SELECT
        COUNT(*) AS total,
        SUM(CASE WHEN has_jackpot = TRUE THEN 1 ELSE 0 END) AS com_jackpot,
        SUM(CASE WHEN has_jackpot IS NULL THEN 1 ELSE 0 END) AS null_jackpot
    FROM multibet.game_image_mapping
""", fetch=True)
t, j, n = rows[0]
print(f"Total: {t} | has_jackpot=TRUE: {j} | has_jackpot=NULL: {n}")

# Investigar no Athena (pode ser que c_has_jackpot venha como tinyint INT)
print("\nInvestigando direto no vendor_ec2 (DESCRIBE c_has_jackpot)...")
df = query_athena("""
    SELECT c_has_jackpot, COUNT(*) AS qtd
    FROM vendor_ec2.tbl_vendor_games_mapping_mst
    WHERE c_status = 'active' AND UPPER(c_product_id) = 'CASINO'
    GROUP BY c_has_jackpot
    ORDER BY qtd DESC
""", database="vendor_ec2")
print(df.to_string(index=False))


# 5. Cobertura imagem para top 24h (devia ser ~100%)
secao("5. Cobertura imagem nos jogos com atividade nas ultimas 24h")
rows = execute_supernova("""
    SELECT
        COUNT(*) AS total,
        SUM(CASE WHEN game_image_url IS NOT NULL THEN 1 ELSE 0 END) AS com_img,
        SUM(CASE WHEN game_image_url IS NULL THEN 1 ELSE 0 END) AS sem_img
    FROM multibet.game_image_mapping
    WHERE popularity_rank_24h IS NOT NULL
""", fetch=True)
t, c, s = rows[0]
print(f"Jogos com atividade 24h: {t} | Com imagem: {c} | Sem imagem: {s}")

if s > 0:
    print("\nJogos SEM imagem (precisa rodar scraper ou auto_fix do grandes_ganhos):")
    rows = execute_supernova("""
        SELECT popularity_rank_24h, game_name, vendor_id, rounds_24h
        FROM multibet.game_image_mapping
        WHERE popularity_rank_24h IS NOT NULL AND game_image_url IS NULL
        ORDER BY popularity_rank_24h
        LIMIT 10
    """, fetch=True)
    for r, n, v, rd in rows:
        print(f"  rank={r:>3}  {n:<35} vendor={v:<18} rounds={rd}")


# 6. Distribuicao live_subtype no banco (vs amostra dry-run)
secao("6. Distribuicao live_subtype (regex aplicado aos dados em producao)")
rows = execute_supernova("""
    SELECT live_subtype, COUNT(*) AS qtd
    FROM multibet.game_image_mapping
    WHERE game_category = 'live'
    GROUP BY live_subtype
    ORDER BY qtd DESC
""", fetch=True)
for s, q in rows:
    print(f"  {s or '(NULL)':<15} {q:>5}")
