"""Check Slack channel for automated post on 29/04/2026."""
import os
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from slack_sdk import WebClient

load_dotenv()
c = WebClient(token=os.getenv("SLACK_BOT_TOKEN"))
channel = os.getenv("SLACK_CHANNEL_ID")
print(f"Canal: {channel}\n")

resp = c.conversations_history(channel=channel, limit=30)
msgs = resp.data.get("messages", [])

brt = timezone(timedelta(hours=-3))
print(f"{'Data/Hora BRT':<25} {'Tipo':<5} {'Quem':<25} Texto / Anexos")
print("-" * 110)
for m in msgs:
    ts = float(m.get("ts", 0))
    dt_brt = datetime.fromtimestamp(ts, tz=brt)
    is_bot = bool(m.get("bot_id"))
    flag = "BOT" if is_bot else "USER"
    who = m.get("username") or m.get("user") or m.get("bot_id", "?")
    text = (m.get("text", "") or "")[:60].replace("\n", " | ")
    files = m.get("files", []) or []
    files_str = f" [{len(files)} files]" if files else ""
    print(f"{dt_brt.strftime('%Y-%m-%d %H:%M:%S BRT'):<25} {flag:<5} {who:<25} {text}{files_str}")
