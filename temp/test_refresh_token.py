"""Testa refresh do Meta user token via fb_exchange_token."""
import os, json, urllib.request, urllib.error
from datetime import datetime, timezone
from dotenv import load_dotenv
load_dotenv()

APP_ID = os.getenv("META_APP_ID")
APP_SECRET = os.getenv("META_APP_SECRET")
CURRENT = os.getenv("META_ADS_ACCESS_TOKEN")
V = "v21.0"

assert APP_ID, "META_APP_ID ausente no .env"
assert APP_SECRET, "META_APP_SECRET ausente no .env"
assert CURRENT, "META_ADS_ACCESS_TOKEN ausente no .env"

print(f"app_id={APP_ID} | app_secret len={len(APP_SECRET)} | token len={len(CURRENT)}\n")

# 1. debug_token do token atual — ver expiração atual
url = f"https://graph.facebook.com/{V}/debug_token?input_token={CURRENT}&access_token={APP_ID}|{APP_SECRET}"
try:
    with urllib.request.urlopen(url, timeout=15) as r:
        d = json.loads(r.read())["data"]
    exp = datetime.fromtimestamp(d.get("expires_at", 0), tz=timezone.utc)
    print(f"[debug_token ANTES] valid={d.get('is_valid')} expires_at={exp.isoformat()} scopes={d.get('scopes')}")
except urllib.error.HTTPError as e:
    print(f"[debug_token] ERRO: {e.read().decode()[:300]}")

# 2. Refresh: fb_exchange_token
url = (f"https://graph.facebook.com/{V}/oauth/access_token"
       f"?grant_type=fb_exchange_token"
       f"&client_id={APP_ID}"
       f"&client_secret={APP_SECRET}"
       f"&fb_exchange_token={CURRENT}")
try:
    with urllib.request.urlopen(url, timeout=15) as r:
        d = json.loads(r.read())
    new_token = d.get("access_token")
    expires_in = d.get("expires_in", 0)
    print(f"\n[REFRESH OK]")
    print(f"  token_type: {d.get('token_type', '?')}")
    print(f"  expires_in: {expires_in}s ({expires_in//86400} dias)")
    print(f"  novo token len: {len(new_token) if new_token else 0}")
    print(f"  novo token prefixo: {new_token[:30]}..." if new_token else "  (sem token)")

    # 3. debug do novo token
    url2 = f"https://graph.facebook.com/{V}/debug_token?input_token={new_token}&access_token={APP_ID}|{APP_SECRET}"
    with urllib.request.urlopen(url2, timeout=15) as r:
        dn = json.loads(r.read())["data"]
    exp_new = datetime.fromtimestamp(dn.get("expires_at", 0), tz=timezone.utc)
    print(f"\n[debug_token NOVO] valid={dn.get('is_valid')} expires_at={exp_new.isoformat()}")
    print(f"  ganho de prazo: {(exp_new - exp).days} dias a mais")
except urllib.error.HTTPError as e:
    body = e.read().decode()
    print(f"\n[REFRESH ERRO HTTP {e.code}]: {body[:500]}")
