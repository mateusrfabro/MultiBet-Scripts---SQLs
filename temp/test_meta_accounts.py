"""Testa cada conta Meta individualmente — isola se e token ou conta especifica."""
import sys, os, json, urllib.request, urllib.error, urllib.parse
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from dotenv import load_dotenv
load_dotenv()

TOKEN = os.getenv("META_ADS_ACCESS_TOKEN")
ACCOUNTS = [a.strip() for a in os.getenv("META_ADS_ACCOUNT_IDS", "").split(",") if a.strip()]
API_VERSION = "v21.0"

print(f"Token presente: {bool(TOKEN)} | len={len(TOKEN) if TOKEN else 0}")
print(f"Contas: {len(ACCOUNTS)}\n")

# Teste 1: /me — token basico funciona?
try:
    url = f"https://graph.facebook.com/{API_VERSION}/me?access_token={TOKEN}"
    with urllib.request.urlopen(url, timeout=15) as r:
        me = json.loads(r.read())
    print(f"[/me] OK: id={me.get('id')} name={me.get('name', '-')}")
except urllib.error.HTTPError as e:
    body = e.read().decode()
    print(f"[/me] ERRO HTTP {e.code}: {body[:300]}")

# Teste 2: ping rapido em cada conta (sem puxar insights, so metadata)
print("\n=== TESTE POR CONTA (GET metadata) ===")
print(f"{'conta':<30} {'status':<10} {'detalhe'}")
ok = fail = 0
for acc in ACCOUNTS:
    url = f"https://graph.facebook.com/{API_VERSION}/{acc}?fields=name,account_status&access_token={TOKEN}"
    try:
        with urllib.request.urlopen(url, timeout=15) as r:
            d = json.loads(r.read())
            status_map = {1: "ACTIVE", 2: "DISABLED", 3: "UNSETTLED", 7: "PENDING_RISK_REVIEW",
                          8: "PENDING_SETTLEMENT", 9: "IN_GRACE_PERIOD", 100: "PENDING_CLOSURE",
                          101: "CLOSED", 201: "ANY_ACTIVE", 202: "ANY_CLOSED"}
            st = status_map.get(d.get('account_status'), d.get('account_status'))
            print(f"{acc:<30} {'OK':<10} {d.get('name', '-')} [{st}]")
            ok += 1
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        try:
            err = json.loads(body)['error']
            msg = f"code={err.get('code')} sub={err.get('error_subcode', '-')}: {err.get('message', '')[:100]}"
        except Exception:
            msg = body[:120]
        print(f"{acc:<30} {'FAIL':<10} {msg}")
        fail += 1

print(f"\nResumo: {ok} OK | {fail} FAIL de {len(ACCOUNTS)} contas")
