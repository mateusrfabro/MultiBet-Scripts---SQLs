"""
Extrai saques (cashout_success_base) do Athena ps_bi e adiciona ao financeiro_coorte.csv
Batches de 200 para evitar timeout.
"""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import pandas as pd
import logging
from db.athena import query_athena

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

CSV = "data/crm_csvs/financeiro_coorte.csv"

log.info("Carregando financeiro...")
df = pd.read_csv(CSV, sep=";", encoding="utf-8-sig")
all_users = df["user_ext_id"].unique().tolist()
user_strs = [str(u) for u in all_users]
log.info(f"  {len(user_strs)} users")

log.info("Extraindo saques do Athena (batches de 200)...")
all_saques = []
batch_size = 200
total_batches = (len(user_strs) - 1) // batch_size + 1

for i in range(0, len(user_strs), batch_size):
    batch = user_strs[i:i + batch_size]
    ids = ",".join(batch)
    sql = f"""SELECT du.external_id AS user_ext_id,
        p.activity_date AS report_date,
        SUM(p.cashout_success_base) AS saques_brl
    FROM ps_bi.dim_user du
    JOIN ps_bi.fct_player_activity_daily p ON p.player_id = du.ecr_id
    WHERE du.external_id IN ({ids})
      AND du.is_test = false
      AND p.activity_date BETWEEN DATE '2026-03-01' AND DATE '2026-03-30'
      AND p.cashout_success_base > 0
    GROUP BY du.external_id, p.activity_date"""
    try:
        df_b = query_athena(sql, database="ps_bi")
        all_saques.append(df_b)
        bn = i // batch_size + 1
        if bn % 20 == 0 or bn == total_batches:
            log.info(f"  Batch {bn}/{total_batches}: {len(df_b)} registros")
    except Exception as e:
        log.warning(f"  Batch {i // batch_size + 1} falhou: {e}")

if all_saques:
    df_saques = pd.concat(all_saques, ignore_index=True)
    df_saques["saques_brl"] = pd.to_numeric(df_saques["saques_brl"], errors="coerce").fillna(0)
    df_saques["user_ext_id"] = df_saques["user_ext_id"].astype(str)
    df_saques["report_date"] = df_saques["report_date"].astype(str)
    log.info(f"  Total registros com saques: {len(df_saques)}")
    log.info(f"  Total saques: R$ {df_saques['saques_brl'].sum():,.2f}")
else:
    df_saques = pd.DataFrame(columns=["user_ext_id", "report_date", "saques_brl"])
    log.info("  Nenhum saque encontrado")

# Merge
df["user_ext_id"] = df["user_ext_id"].astype(str)
df["report_date"] = df["report_date"].astype(str)

# Remover coluna saques_brl se ja existe
if "saques_brl" in df.columns:
    df = df.drop(columns=["saques_brl"])
if "net_deposit_brl" in df.columns:
    df = df.drop(columns=["net_deposit_brl"])

df = df.merge(df_saques, on=["user_ext_id", "report_date"], how="left")
df["saques_brl"] = df["saques_brl"].fillna(0)
df["depositos_brl"] = pd.to_numeric(df["depositos_brl"], errors="coerce").fillna(0)
df["net_deposit_brl"] = df["depositos_brl"] - df["saques_brl"]

df.to_csv(CSV, index=False, sep=";", encoding="utf-8-sig")
log.info(f"Salvo: {CSV}")
log.info(f"  Depositos: R$ {df['depositos_brl'].sum():,.2f}")
log.info(f"  Saques: R$ {df['saques_brl'].sum():,.2f}")
log.info(f"  Net Deposit: R$ {df['net_deposit_brl'].sum():,.2f}")
log.info("DONE")
