"""
Diagnóstico: game_image_mapping — quantos jogos com/sem imagem.
Roda via Super Nova DB.
"""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.supernova import execute_supernova

def main():
    print("=== Diagnóstico game_image_mapping ===\n")

    # 1. Totais
    rows = execute_supernova("""
        SELECT
            COUNT(*) AS total,
            COUNT(game_image_url) AS com_imagem,
            COUNT(*) - COUNT(game_image_url) AS sem_imagem,
            ROUND(100.0 * COUNT(game_image_url) / NULLIF(COUNT(*), 0), 1) AS pct_cobertura
        FROM multibet.game_image_mapping
    """, fetch=True)
    if rows:
        total, com, sem, pct = rows[0]
        print(f"Total jogos:    {total}")
        print(f"Com imagem:     {com} ({pct}%)")
        print(f"Sem imagem:     {sem} ({100 - float(pct):.1f}%)")

    # 2. Breakdown por source
    print("\n--- Por source ---")
    rows = execute_supernova("""
        SELECT
            source,
            COUNT(*) AS total,
            COUNT(game_image_url) AS com_img,
            COUNT(*) - COUNT(game_image_url) AS sem_img
        FROM multibet.game_image_mapping
        GROUP BY source
        ORDER BY total DESC
    """, fetch=True)
    for source, total, com, sem in rows:
        print(f"  {source:<25} total={total:>5}  com_img={com:>5}  sem_img={sem:>5}")

    # 3. Breakdown por vendor (sem imagem)
    print("\n--- Top 15 vendors SEM imagem ---")
    rows = execute_supernova("""
        SELECT
            COALESCE(vendor_id, 'NULL') AS vendor,
            COUNT(*) AS sem_img
        FROM multibet.game_image_mapping
        WHERE game_image_url IS NULL
        GROUP BY vendor_id
        ORDER BY sem_img DESC
        LIMIT 15
    """, fetch=True)
    for vendor, sem in rows:
        print(f"  {vendor:<35} {sem:>5} sem imagem")

    # 4. Exemplos sem imagem (primeiros 20)
    print("\n--- 20 jogos sem imagem (amostra) ---")
    rows = execute_supernova("""
        SELECT game_name, vendor_id, provider_game_id, source
        FROM multibet.game_image_mapping
        WHERE game_image_url IS NULL
        ORDER BY game_name
        LIMIT 20
    """, fetch=True)
    for name, vendor, gid, src in rows:
        print(f"  {name:<40} vendor={vendor}  game_id={gid}  source={src}")

    # 5. Jogos que aparecem no grandes_ganhos mas sem imagem
    print("\n--- Jogos em grandes_ganhos SEM imagem ---")
    rows = execute_supernova("""
        SELECT DISTINCT g.game_name, g.provider_name, g.game_image_url IS NOT NULL AS has_img
        FROM multibet.grandes_ganhos g
        WHERE g.game_image_url IS NULL OR g.game_image_url = ''
        ORDER BY g.game_name
    """, fetch=True)
    if rows:
        for name, provider, _ in rows:
            print(f"  {name:<40} provider={provider}")
    else:
        print("  Nenhum! Todos os grandes_ganhos têm imagem.")

    print("\n=== Diagnóstico concluído ===")

if __name__ == "__main__":
    main()
