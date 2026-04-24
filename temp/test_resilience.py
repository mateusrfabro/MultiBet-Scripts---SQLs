"""Testa que get_campaign_spend tolera conta invalida (nao aborta)."""
import sys, os
os.environ["META_ADS_ACCESS_TOKEN"] = "EAASFqlKv054BRQredZAPZBOVxA3ztZBZC8C8ZB5oV0ZC1G9qGZB3YzFRZAXWnW6WtwhjYne3bdoO8Afo19en1tMrijMwF6h1mzwplbWwn6R0etsboWyJHqdeUzlWBS09DQjXQd6ttSJ6SW9wTCSK60ZCXnwa3vvhNaBmUKTy30XciUOg6EsrgTGrMayz6AgignNfondWM"

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import logging
logging.basicConfig(level=logging.INFO, format="%(message)s")

from db.meta_ads import get_campaign_spend
from datetime import date, timedelta

# Mistura conta valida + conta invalida + conta sem permissao
accounts = [
    "act_1418521646228655",      # VALIDA (Multibet principal)
    "act_846913941192022",       # sem permissao BM2 (deveria dar warning e pular)
    "act_0000000000000000",      # invalida sintaticamente
]

rows = get_campaign_spend(
    start_date=date.today() - timedelta(days=2),
    end_date=date.today() - timedelta(days=1),
    account_ids=accounts,
)
print(f"\nRESULTADO: {len(rows)} linhas retornadas (deveria ter dados da conta valida apenas)")
