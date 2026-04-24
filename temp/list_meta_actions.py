"""Lista TODOS os action_types que a Meta reporta nas campanhas MultiBet (1 dia, 1 conta)."""
import os, json, urllib.request, urllib.parse
from collections import Counter
from datetime import date, timedelta

TOKEN = os.environ["TEST_TOKEN"]
API_VERSION = "v21.0"
ACCOUNT = "act_1418521646228655"   # Multibet principal (volume maior)
end_date = date.today() - timedelta(days=1)
start_date = end_date - timedelta(days=6)

time_range = urllib.parse.quote(json.dumps({"since": str(start_date), "until": str(end_date)}))
url = (f"https://graph.facebook.com/{API_VERSION}/{ACCOUNT}/insights"
       f"?access_token={TOKEN}"
       f"&fields=campaign_id,campaign_name,spend,impressions,clicks,inline_link_clicks,"
       f"reach,actions,cost_per_action_type"
       f"&level=campaign&time_increment=1&time_range={time_range}&limit=500")

data = json.loads(urllib.request.urlopen(url, timeout=30).read())
rows = data.get("data", [])
print(f"Periodo: {start_date} a {end_date} | Conta: {ACCOUNT}")
print(f"Linhas retornadas: {len(rows)}\n")

# Coleta todos os action_types com contagem de linhas que tem + soma total
action_counter = Counter()   # numero de linhas com o action_type
action_values = Counter()    # soma dos valores
cost_per_action_counter = Counter()  # numero de linhas onde cost_per_action_type reporta

for r in rows:
    for a in (r.get("actions") or []):
        atype = a.get("action_type", "")
        val = float(a.get("value", 0) or 0)
        action_counter[atype] += 1
        action_values[atype] += val
    for a in (r.get("cost_per_action_type") or []):
        atype = a.get("action_type", "")
        cost_per_action_counter[atype] += 1

print("=== action_types encontrados (ranqueados por soma de valores) ===")
print(f"{'action_type':<55} {'linhas':<8} {'soma_val':<12} {'tem_cost_per':<6}")
for atype, _ in action_values.most_common():
    print(f"{atype:<55} {action_counter[atype]:<8} {int(action_values[atype]):<12} "
          f"{'sim' if cost_per_action_counter[atype] > 0 else 'nao'}")
