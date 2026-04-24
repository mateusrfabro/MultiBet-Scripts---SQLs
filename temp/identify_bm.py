"""Identifica o BM exato (nome + id) de cada conta Multibet."""
import os, json, urllib.request, urllib.error

TOKEN = os.environ["TEST_TOKEN"]
V = "v21.0"

accounts = {
    "act_1418521646228655": "Multibet (principal)",
    "act_1531679918112645": "Multibet Verified",
    "act_1282215803969842": "Multibet Verified 3",
    "act_4397365763819913": "Multibet Verified 4",
    "act_26153688877615850": "Multibet Verified 5",
    "act_1394438821997847": "Multibet Verified 2 (disabled)",
    "act_846913941192022": "Multibet sem BM (sem permissão)",
}

print(f"{'conta':<30} {'label':<35} {'BM id':<20} {'BM nome'}")
print("-" * 130)
for acc, label in accounts.items():
    url = (f"https://graph.facebook.com/{V}/{acc}"
           f"?fields=business,account_status,name,owner"
           f"&access_token={TOKEN}")
    try:
        with urllib.request.urlopen(url, timeout=15) as r:
            d = json.loads(r.read())
        biz = d.get("business", {})
        bm_id = biz.get("id", "[sem BM / sem acesso]") if biz else "[sem BM]"
        bm_name = biz.get("name", "-") if biz else "-"
        print(f"{acc:<30} {label:<35} {bm_id:<20} {bm_name}")
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"{acc:<30} {label:<35} ERRO: {body[:60]}")
