"""
Dry-run: valida queries enriquecidas + regex de subtipo Live ANTES de aplicar
ALTER TABLE/views em producao.

Mostra:
  1. Amostras do catalogo enriquecido Athena (categoria+vendor+jackpot)
  2. Distribuicao do regex live_subtype (Roleta/BJ/Baccarat/etc)
  3. Top 20 jogos por rounds nas ULTIMAS 24h rolantes (PostgreSQL)
  4. Sample de jogos que ficariam em cada view vw_front_*
"""
import sys, os, re
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.athena import query_athena
from db.supernova import execute_supernova
# Reutiliza a logica que esta no pipeline
from pipelines.game_image_mapper import (
    QUERY_ATHENA_GAMES, QUERY_PG_RANK_24H, classify_live_subtype
)


def secao(t):
    print("\n" + "=" * 70); print(t); print("=" * 70)


# 1. Catalogo enriquecido Athena
secao("1. Athena — catalogo enriquecido (10 amostras)")
df = query_athena(QUERY_ATHENA_GAMES, database="bireports_ec2")
print(f"Total: {len(df)} jogos (1 por nome, Pragmatic prioritario)")
print(df.head(10).to_string(index=False))


# 2. Distribuicao live_subtype apos regex
secao("2. Distribuicao live_subtype (regex aplicado em Live)")
df["live_subtype"] = df.apply(
    lambda r: classify_live_subtype(r["game_type_desc"], r["game_category"]),
    axis=1
)
dist = df[df["game_category"] == "live"]["live_subtype"].value_counts(dropna=False)
print(dist.to_string())

# Amostras por subtipo (validar visualmente)
print("\n--- Amostras por subtipo (5 cada) ---")
for sub in ["Roleta", "Blackjack", "Baccarat", "GameShow", "Outros"]:
    amostra = df[(df["game_category"] == "live") & (df["live_subtype"] == sub)]\
        .head(5)[["game_name_upper", "game_type_desc"]]
    print(f"\n  [{sub}]")
    for _, r in amostra.iterrows():
        print(f"    {r['game_name_upper']:<40}  ({r['game_type_desc']})")


# 3. Distribuicao categoria geral
secao("3. Distribuicao por categoria (slots/live/...)")
print(df.groupby(["product_id", "game_category"]).size().to_string())


# 4. Distribuicao vendor
secao("4. Top 10 vendors")
print(df["vendor_id"].value_counts().head(10).to_string())


# 5. Has jackpot
secao("5. Jogos com has_jackpot = TRUE")
jackpot_count = df["has_jackpot"].sum()
print(f"Total: {jackpot_count} jogos com jackpot")
print(df[df["has_jackpot"]].head(10)[["game_name_upper", "vendor_id", "game_category"]].to_string(index=False))


# 6. Status active
secao("6. Status active vs inactive")
print(df["status"].value_counts().to_string())


# 7. PostgreSQL — rank 24h rolantes
secao("7. PostgreSQL — TOP 20 nas ULTIMAS 24h rolantes (silver_game_15min)")
rows = execute_supernova(QUERY_PG_RANK_24H + " ORDER BY popularity_rank_24h LIMIT 20", fetch=True)
print(f"{'rank':>4}  {'game_name':<35} {'rounds_24h':>12} {'players_24h':>12}")
for nm, rd, pl, rk in rows:
    print(f"{rk:>4}  {nm:<35} {rd:>12} {pl:>12}")


# 8. Cobertura: quantos top 50 do rank 24h tem imagem no game_image_mapping?
secao("8. Cobertura imagem para top 50 do rank 24h")
top50_rows = execute_supernova(
    QUERY_PG_RANK_24H + " ORDER BY popularity_rank_24h LIMIT 50",
    fetch=True
)
top50_names = set(r[0] for r in top50_rows)
mapeados = execute_supernova(
    "SELECT game_name_upper FROM multibet.game_image_mapping WHERE game_image_url IS NOT NULL",
    fetch=True
)
mapeados_set = set(r[0] for r in mapeados)
sem_img = top50_names - mapeados_set
print(f"Top 50: {len(top50_names)} | Com imagem: {len(top50_names & mapeados_set)} | Sem imagem: {len(sem_img)}")
if sem_img:
    print(f"Top 50 SEM imagem (precisa scraper):")
    for n in sorted(sem_img):
        print(f"   - {n}")

print("\n=== DRY-RUN concluido — nada foi escrito no banco ===")
