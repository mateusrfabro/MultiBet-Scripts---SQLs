"""Puxa 10 user_ext_ids variados do snapshot 2026-04-29 para validacao no Smartico.

Distribuicao: 1S + 2A + 2B + 2C + 1D + 1E + 1NEW = 10
Selecao: top N por PVS dentro de cada rating (jogadores mais representativos do tier).
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.supernova import execute_supernova

# distribuicao: rating -> qtd
DIST = [("S", 1), ("A", 2), ("B", 2), ("C", 2), ("D", 1), ("E", 1), ("NEW", 1)]

print("=" * 110)
print("AMOSTRA DE VALIDACAO SMARTICO - snapshot 2026-04-29")
print("=" * 110)
print()
print(f"{'#':<3} {'user_ext_id':<14} {'Rating':<7} {'Tag esperada Smartico':<25} {'PVS':<6} {'GGR Total':<14} {'#Dep':<5}")
print("-" * 110)

idx = 1
all_rows = []
for rating, qtd in DIST:
    rows = execute_supernova(f"""
        SELECT external_id, rating, pvs, ggr_total, num_deposits
        FROM multibet.pcr_atual
        WHERE snapshot_date = '2026-04-29'
          AND rating = '{rating}'
          AND external_id IS NOT NULL
        ORDER BY pvs DESC NULLS LAST
        LIMIT {qtd}
    """, fetch=True)
    for r in rows:
        ext_id, rating_val, pvs, ggr, ndep = r
        tag = f"PCR_RATING_{rating_val}"
        ggr_str = f"R$ {float(ggr or 0):>10,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        pvs_str = f"{float(pvs or 0):.2f}" if pvs is not None else "—"
        print(f"{idx:<3} {ext_id:<14} {rating_val:<7} {tag:<25} {pvs_str:<6} {ggr_str:<14} {ndep or 0:<5}")
        all_rows.append({"external_id": ext_id, "rating": rating_val, "tag_esperada": tag, "pvs": pvs, "ggr_total": ggr})
        idx += 1

print()
print("=" * 110)
print("COMO VALIDAR")
print("=" * 110)
print("""
1. Abre o Smartico Backoffice
2. Para cada user_ext_id acima, busca o jogador
3. Confere a aba 'Tags' / 'External Markers' (core_external_markers)
4. Deve aparecer EXATAMENTE 1 tag PCR_RATING_* (a tag esperada da coluna)
5. NAO deve ter 2+ tags PCR_RATING_* simultaneas (ex: ter S e A juntos = bug)

OK = todos os 10 com tag correta, sem duplicatas
PROBLEMA = qualquer um sem tag, com tag errada, ou com 2+ tags PCR_RATING_*
""")

# salva amostra em CSV pra registro
import csv
out_path = "reports/smartico_amostra_validacao_2026-04-29.csv"
os.makedirs("reports", exist_ok=True)
with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
    w = csv.DictWriter(f, fieldnames=["external_id", "rating", "tag_esperada", "pvs", "ggr_total"])
    w.writeheader()
    w.writerows(all_rows)
print(f"Amostra salva em: {out_path}")
