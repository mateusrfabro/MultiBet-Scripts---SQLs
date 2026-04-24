"""Testa token BM2 passado via env TEST_TOKEN (nao sobrescreve .env)."""
import os, json, urllib.request, urllib.error
from dotenv import load_dotenv
load_dotenv()

TOKEN = os.environ.get("TEST_TOKEN") or ""
ACCOUNTS = [a.strip() for a in os.getenv("META_ADS_ACCOUNT_IDS", "").split(",") if a.strip()]
API_VERSION = "v21.0"

assert TOKEN, "TEST_TOKEN vazio"
print(f"Token len={len(TOKEN)} | {len(ACCOUNTS)} contas\n")

# /me
try:
    url = f"https://graph.facebook.com/{API_VERSION}/me?access_token={TOKEN}"
    with urllib.request.urlopen(url, timeout=15) as r:
        me = json.loads(r.read())
    print(f"[/me] OK id={me.get('id')} name={me.get('name', '-')}")
except urllib.error.HTTPError as e:
    body = e.read().decode()
    print(f"[/me] ERRO HTTP {e.code}: {body[:250]}")

# debug_token — valida o token (app, escopo, expiracao)
try:
    url = (f"https://graph.facebook.com/{API_VERSION}/debug_token"
           f"?input_token={TOKEN}&access_token={TOKEN}")
    with urllib.request.urlopen(url, timeout=15) as r:
        d = json.loads(r.read())
    print(f"[debug_token] {json.dumps(d.get('data', {}), indent=2, default=str)[:500]}")
except Exception as e:
    print(f"[debug_token] erro: {e}")

# Cada conta
print("\n=== TESTE POR CONTA ===")
print(f"{'conta':<30} {'status':<6} {'nome/erro'}")
ok = fail = 0
status_map = {1: "ACTIVE", 2: "DISABLED", 3: "UNSETTLED", 7: "PENDING_RISK_REVIEW",
              8: "PENDING_SETTLEMENT", 9: "IN_GRACE_PERIOD", 100: "PENDING_CLOSURE",
              101: "CLOSED"}
for acc in ACCOUNTS:
    url = f"https://graph.facebook.com/{API_VERSION}/{acc}?fields=name,account_status&access_token={TOKEN}"
    try:
        with urllib.request.urlopen(url, timeout=15) as r:
            d = json.loads(r.read())
            st = status_map.get(d.get('account_status'), d.get('account_status'))
            print(f"{acc:<30} {'OK':<6} {d.get('name', '-')} [{st}]")
            ok += 1
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        try:
            err = json.loads(body)['error']
            msg = f"code={err.get('code')} sub={err.get('error_subcode','-')}: {err.get('message','')[:100]}"
        except Exception:
            msg = body[:120]
        print(f"{acc:<30} {'FAIL':<6} {msg}")
        fail += 1
print(f"\nResumo: {ok} OK | {fail} FAIL de {len(ACCOUNTS)}")
