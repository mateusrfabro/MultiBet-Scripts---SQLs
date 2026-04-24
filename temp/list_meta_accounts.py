"""Lista TODAS as contas de anuncio acessiveis pelo token BM2 (/me/adaccounts)."""
import os, json, urllib.request, urllib.error, urllib.parse

TOKEN = os.environ["TEST_TOKEN"]
API_VERSION = "v21.0"

url = (f"https://graph.facebook.com/{API_VERSION}/me/adaccounts"
       f"?fields=account_id,name,account_status,currency,business_name,timezone_name"
       f"&limit=200&access_token={TOKEN}")

all_accounts = []
page = 0
while url:
    with urllib.request.urlopen(url, timeout=30) as r:
        d = json.loads(r.read())
    all_accounts.extend(d.get("data", []))
    url = d.get("paging", {}).get("next")
    page += 1

status_map = {1: "ACTIVE", 2: "DISABLED", 3: "UNSETTLED", 7: "PENDING_RISK_REVIEW",
              8: "PENDING_SETTLEMENT", 9: "IN_GRACE_PERIOD", 100: "PENDING_CLOSURE", 101: "CLOSED"}

print(f"Total: {len(all_accounts)} contas | {page} paginas\n")

# Agrupar por moeda (BRL=MultiBet, PKR=Play4Tune)
by_ccy = {}
for a in all_accounts:
    by_ccy.setdefault(a.get("currency", "?"), []).append(a)

for ccy in sorted(by_ccy.keys()):
    accs = by_ccy[ccy]
    print(f"\n=== MOEDA {ccy} ({len(accs)} contas) ===")
    print(f"{'act_id':<28} {'status':<10} {'BM':<30} {'name'}")
    for a in sorted(accs, key=lambda x: x.get("name", "")):
        act_id = f"act_{a['account_id']}"
        st = status_map.get(a.get("account_status"), a.get("account_status"))
        bm = (a.get("business_name") or "[sem BM]")[:28]
        nm = a.get("name", "-")
        print(f"{act_id:<28} {st:<10} {bm:<30} {nm}")
