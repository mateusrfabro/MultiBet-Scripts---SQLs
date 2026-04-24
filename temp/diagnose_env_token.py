"""Diagnóstica qual token está no .env e de qual app ele é."""
import os, json, urllib.request, urllib.error
from datetime import datetime, timezone
from dotenv import load_dotenv
load_dotenv()

T = os.getenv("META_ADS_ACCESS_TOKEN")
V = "v21.0"
print(f"Token prefixo: {T[:30]}..." if T else "SEM TOKEN")
print(f"Token len: {len(T) if T else 0}\n")

# Debug usando o proprio token como access (sem app creds)
url = f"https://graph.facebook.com/{V}/debug_token?input_token={T}&access_token={T}"
try:
    with urllib.request.urlopen(url, timeout=15) as r:
        d = json.loads(r.read())["data"]
    exp = d.get("expires_at", 0)
    print(f"app_id do token: {d.get('app_id')}")
    print(f"application: {d.get('application')}")
    print(f"type: {d.get('type')}")
    print(f"valid: {d.get('is_valid')}")
    print(f"user_id: {d.get('user_id')}")
    if exp:
        print(f"expires_at: {datetime.fromtimestamp(exp, tz=timezone.utc).isoformat()}")
    print(f"scopes: {d.get('scopes')}")
except urllib.error.HTTPError as e:
    print(f"ERRO: {e.read().decode()[:300]}")
