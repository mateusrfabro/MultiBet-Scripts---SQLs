"""Mapeia estrutura BM do Augusto: quais BMs ele admin, em qual BM estão as contas MultiBet."""
import os, json, urllib.request, urllib.error

TOKEN = os.environ["TEST_TOKEN"]
V = "v21.0"

def get(path):
    url = f"https://graph.facebook.com/{V}{path}{'&' if '?' in path else '?'}access_token={TOKEN}"
    try:
        with urllib.request.urlopen(url, timeout=15) as r: return json.loads(r.read())
    except urllib.error.HTTPError as e:
        try: return {"error": json.loads(e.read().decode())['error']}
        except Exception: return {"error": str(e)}

# 1. /me — user info
me = get("/me?fields=id,name")
print(f"[/me] id={me.get('id')} name={me.get('name')}\n")

# 2. BMs onde o user é admin/member
bms = get("/me/businesses?fields=id,name,primary_page,verification_status")
print("=== BUSINESS MANAGERS acessíveis ===")
for b in bms.get("data", []):
    print(f"  BM id={b['id']:<20} name={b.get('name','-'):<40} verif={b.get('verification_status','-')}")

# 3. Pra cada BM, listar as contas de anúncio QUE ELE OWNER/CLIENT
print("\n=== CONTAS DE ANÚNCIO POR BM ===")
for b in bms.get("data", []):
    bid = b['id']; bname = b.get('name', '-')
    owned = get(f"/{bid}/owned_ad_accounts?fields=account_id,name,account_status&limit=200")
    client = get(f"/{bid}/client_ad_accounts?fields=account_id,name,account_status&limit=200")
    print(f"\nBM: {bname} ({bid})")
    print(f"  OWNED: {len(owned.get('data', []))} contas")
    for a in owned.get('data', [])[:15]:
        print(f"    act_{a['account_id']:<25} {a.get('name','-')[:40]:<40}")
    print(f"  CLIENT: {len(client.get('data', []))} contas")
    for a in client.get('data', [])[:15]:
        print(f"    act_{a['account_id']:<25} {a.get('name','-')[:40]:<40}")

# 4. Info do app "Caixinha" — em qual BM ele vive
print("\n=== APP CAIXINHA (1272866485031838) ===")
app = get("/1272866485031838?fields=id,name,namespace,category,link,business")
print(json.dumps(app, indent=2, ensure_ascii=False)[:800])

# 5. Quais BMs ja tem System Users configurados (pro Augusto saber se reaproveita)
print("\n=== SYSTEM USERS POR BM ===")
for b in bms.get("data", []):
    sys_users = get(f"/{b['id']}/system_users?fields=id,name,role")
    data = sys_users.get("data", [])
    print(f"\nBM: {b.get('name','-')} ({b['id']}) — {len(data)} system users")
    for su in data[:10]:
        print(f"  id={su['id']:<20} name={su.get('name','-'):<30} role={su.get('role','-')}")
