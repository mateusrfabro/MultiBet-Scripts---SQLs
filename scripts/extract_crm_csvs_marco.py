"""
Extração de CSVs — CRM Performance Março 2026
===============================================
Extrai dados de BigQuery (Smartico CRM) e Athena (Data Lake) para gerar
CSVs que alimentam o dashboard CRM v0.

Período: 01/03/2026 a 30/03/2026 (BRT = America/Sao_Paulo)
Destino: data/crm_csvs/

CSVs gerados:
  1. campanhas_diarias.csv    — 1 linha por campanha × dia, com funil
  2. financeiro_coorte.csv    — GGR/turnover/depositos dos users que completaram
  3. disparos_custos.csv      — volume e custo por canal/provedor/dia
  4. top_jogos.csv            — jogos mais jogados pela coorte CRM
  5. vip_groups.csv           — classificação VIP dos users da coorte
  6. recovery.csv             — inativos reengajados D+1/D+3

FONTES DOCUMENTADAS:
  BigQuery: smartico-bq6.dwh_ext_24105
    - j_bonuses: bonus_status_id (1=Oferecido, 3=Completou, 4=Expirou)
    - j_communication: fact_type_id (1=Enviado) + activity_type_id (canal)
    - dm_automation_rule: status campanhas (active=1)
  Athena: ps_bi (BRL, is_test=false)
    - fct_player_activity_daily: GGR, turnover, depositos
    - fct_casino_activity_daily: jogos por user
    - dim_user: external_id = BigQuery user_ext_id
    - dim_game: game_desc = nome do jogo (cobertura parcial)
  Athena: bireports_ec2 (centavos /100)
    - tbl_ecr_wise_daily_bi_summary: fallback financeiro

REGRAS:
  - Timezone: Athena = UTC, converter com AT TIME ZONE; BigQuery = fact_date em UTC
  - ps_bi valores em BRL reais; bireports em centavos (/100)
  - Filtrar test users: is_test = false (ps_bi) ou c_test_user = false (bireports)
  - Funil: Oferecidos (status 1) → Completaram (status 3) → Expirados (status 4)

Autor: Mateus F. (Squad Intelligence Engine)
Data: 31/03/2026
"""

import sys
import os
import logging
from datetime import date

import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.bigquery import query_bigquery
from db.athena import query_athena

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DS = "smartico-bq6.dwh_ext_24105"
DATE_FROM = "2026-03-01"
DATE_TO = "2026-03-30"
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "crm_csvs")

# Mapeamento PG Soft game IDs (nao cobertos pelo dim_game)
KNOWN_GAMES = {
    '8842': 'Fortune Ox', '13097': 'Fortune Tiger', '4776': 'Fortune Mouse',
    '8369': 'Fortune Rabbit', '18974': 'Fortune Dragon', '18993': 'Fortune Snake',
    '18949': 'Ratinho', '3221': 'Bikini Paradise', '833': 'Muay Thai Champion',
    '990': 'Dragon Tiger', '227': 'Baccarat', '16409': 'Fortune Gems 2',
    '14182': 'Candy Burst', '14878': 'Wild Bandito',
}

# Custos por provider_id
CUSTO_PROVIDER = {
    1536: ("SMS", "DisparoPro", 0.045),
    1545: ("SMS", "PushFY", 0.060),
    1268: ("SMS", "Comtele", 0.063),
    1261: ("WhatsApp", "Loyalty", 0.16),
    1553: ("Push", "PushFY", 0.060),
    611: ("Popup", "Smartico", 0.0),
}

TIPO_MAP = {
    "RETEM": ["RETEM", "CORUJ"],
    "Challenge": ["CHALLENGE", "QUEST", "MULTIVERSO", "PGS"],
    "DailyFS": ["DESAFIO", "GIRE", "DAILYFS", "DAILY_FS"],
    "Cashback": ["CASHBACK"],
    "Torneio": ["TORNEIO", "TOURNAMENT"],
    "Freebet": ["FREEBET", "FREE BET", "DEPOSITE"],
    "FreeSpins": ["CA_FS_", "CAS_FS_", "FREESPIN"],
    "Lifecycle": ["LIFECYCLE", "LIFE_CYCLE"],
    "Welcome": ["WELCOME", "BEM.VINDO", "BEMVINDO"],
}


def classify_type(name):
    if not name:
        return "outro"
    n = name.upper()
    for tipo, keywords in TIPO_MAP.items():
        for kw in keywords:
            if kw in n:
                return tipo
    return "outro"


def ensure_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    log.info(f"Output: {OUTPUT_DIR}")


# ================================================================
# CSV 1: Campanhas diárias com funil
# ================================================================
def extract_campanhas_diarias():
    log.info("=" * 60)
    log.info("CSV 1: Campanhas diárias com funil")
    log.info("Fonte: BigQuery j_bonuses + dm_automation_rule")

    # Campanhas ativas (dm_automation_rule.is_active = true)
    sql_active = f"""
    SELECT DISTINCT CAST(rule_id AS STRING) AS campaign_id
    FROM `{DS}.dm_automation_rule`
    WHERE is_active = true
    """
    try:
        df_active = query_bigquery(sql_active)
        active_ids = set(df_active["campaign_id"].tolist())
        log.info(f"  Campanhas ativas no Smartico: {len(active_ids)}")
    except Exception as e:
        log.warning(f"  dm_automation_rule falhou: {e}. Usando todas.")
        active_ids = None

    # Funil por campanha × dia
    # Status 1 = Oferecido (universo segmentado)
    # Status 3 = Completou (recebeu bonus)
    # Status 4 = Expirou (nao completou)
    sql = f"""
    SELECT
        DATE(fact_date, 'America/Sao_Paulo') AS report_date,
        CAST(entity_id AS STRING) AS campaign_id,
        MAX(JSON_EXTRACT_SCALAR(activity_details, '$.campaign_name')) AS campaign_name,
        COUNT(DISTINCT CASE WHEN bonus_status_id = 1 THEN user_ext_id END) AS oferecidos,
        COUNT(DISTINCT CASE WHEN bonus_status_id IN (1, 3) THEN user_ext_id END) AS opt_in,
        COUNT(DISTINCT CASE WHEN bonus_status_id = 3 THEN user_ext_id END) AS completaram,
        COUNT(DISTINCT CASE WHEN bonus_status_id = 4 THEN user_ext_id END) AS expiraram,
        SUM(CASE WHEN bonus_status_id = 3 THEN CAST(bonus_cost_value AS FLOAT64) ELSE 0 END) AS custo_bonus_brl,
        COUNT(DISTINCT user_ext_id) AS users_total
    FROM `{DS}.j_bonuses`
    WHERE DATE(fact_date, 'America/Sao_Paulo') BETWEEN '{DATE_FROM}' AND '{DATE_TO}'
    GROUP BY report_date, entity_id
    ORDER BY report_date, completaram DESC
    """
    df = query_bigquery(sql)
    log.info(f"  Registros: {len(df)}")

    # Classificar tipo e status
    df["campaign_type"] = df["campaign_name"].apply(classify_type)
    if active_ids is not None:
        df["status"] = df["campaign_id"].apply(lambda x: "ativa" if x in active_ids else "inativa")
    else:
        df["status"] = "ativa"

    # Salvar
    path = os.path.join(OUTPUT_DIR, "campanhas_diarias.csv")
    df.to_csv(path, index=False, sep=";", encoding="utf-8-sig")
    log.info(f"  Salvo: {path}")
    log.info(f"  Período: {df['report_date'].min()} a {df['report_date'].max()}")
    log.info(f"  Campanhas únicas: {df['campaign_id'].nunique()}")
    log.info(f"  Tipos: {df['campaign_type'].value_counts().to_dict()}")
    return df


# ================================================================
# CSV 2: Disparos e custos por canal/provedor/dia
# ================================================================
def extract_disparos():
    log.info("=" * 60)
    log.info("CSV 2: Disparos e custos")
    log.info("Fonte: BigQuery j_communication (fact_type_id=1)")

    sql = f"""
    SELECT
        DATE(fact_date, 'America/Sao_Paulo') AS report_date,
        activity_type_id,
        label_provider_id,
        COUNT(*) AS total_sent,
        COUNT(DISTINCT user_ext_id) AS users_impactados
    FROM `{DS}.j_communication`
    WHERE fact_type_id = 1
      AND DATE(fact_date, 'America/Sao_Paulo') BETWEEN '{DATE_FROM}' AND '{DATE_TO}'
    GROUP BY report_date, activity_type_id, label_provider_id
    ORDER BY report_date
    """
    df = query_bigquery(sql)

    # Mapear canal/provedor/custo
    rows = []
    for _, r in df.iterrows():
        pid = int(r["label_provider_id"]) if pd.notna(r["label_provider_id"]) else 0
        info = CUSTO_PROVIDER.get(pid, ("outro", "desconhecido", 0.0))
        canal, prov, custo = info
        total = int(r["total_sent"])
        rows.append({
            "report_date": r["report_date"],
            "channel": canal,
            "provider": prov,
            "provider_id": pid,
            "activity_type_id": int(r["activity_type_id"]) if pd.notna(r["activity_type_id"]) else 0,
            "total_sent": total,
            "users_impactados": int(r["users_impactados"]),
            "cost_per_unit": custo,
            "total_cost_brl": round(total * custo, 2),
        })

    df_out = pd.DataFrame(rows)
    path = os.path.join(OUTPUT_DIR, "disparos_custos.csv")
    df_out.to_csv(path, index=False, sep=";", encoding="utf-8-sig")
    log.info(f"  Salvo: {path}")
    log.info(f"  Total envios março: {df_out['total_sent'].sum():,}")
    log.info(f"  Custo total março: R$ {df_out['total_cost_brl'].sum():,.2f}")
    return df_out


# ================================================================
# CSV 3: Coorte financeiro (Athena) — users que completaram
# ================================================================
def extract_financeiro(df_campanhas):
    log.info("=" * 60)
    log.info("CSV 3: Financeiro da coorte CRM")
    log.info("Fonte: BigQuery j_bonuses (coorte) + Athena ps_bi (financeiro)")

    # Extrair coorte completa de março: users que completaram campanhas
    sql_coorte = f"""
    SELECT DISTINCT
        user_ext_id,
        CAST(entity_id AS STRING) AS campaign_id,
        DATE(fact_date, 'America/Sao_Paulo') AS completion_date
    FROM `{DS}.j_bonuses`
    WHERE bonus_status_id = 3
      AND DATE(fact_date, 'America/Sao_Paulo') BETWEEN '{DATE_FROM}' AND '{DATE_TO}'
    """
    df_coorte = query_bigquery(sql_coorte)
    all_users = df_coorte["user_ext_id"].unique().tolist()
    log.info(f"  Coorte março: {len(all_users)} users únicos")

    # Buscar financeiro no Athena (batches de 500)
    all_fin = []
    user_strs = [str(u) for u in all_users]
    for i in range(0, len(user_strs), 500):
        batch = user_strs[i:i + 500]
        ids = ",".join(batch)
        sql = f"""
        SELECT du.external_id AS user_ext_id,
            p.activity_date AS report_date,
            SUM(p.ggr_base) AS ggr_brl,
            SUM(p.ngr_base) AS ngr_brl,
            SUM(p.bet_amount_base) AS turnover_brl,
            SUM(p.deposit_success_base) AS depositos_brl,
            SUM(p.login_count) AS sessoes,
            SUM(p.casino_realbet_base) AS turnover_casino_brl,
            SUM(p.sb_realbet_base) AS turnover_sports_brl
        FROM ps_bi.dim_user du
        JOIN ps_bi.fct_player_activity_daily p ON p.player_id = du.ecr_id
        WHERE du.external_id IN ({ids})
          AND du.is_test = false
          AND p.activity_date BETWEEN DATE '{DATE_FROM}' AND DATE '{DATE_TO}'
        GROUP BY du.external_id, p.activity_date
        """
        df_batch = query_athena(sql, database="ps_bi")
        all_fin.append(df_batch)
        log.info(f"  Athena batch {i // 500 + 1}/{(len(user_strs) - 1) // 500 + 1}: {len(df_batch)} registros")

    df_fin = pd.concat(all_fin, ignore_index=True) if all_fin else pd.DataFrame()
    if not df_fin.empty:
        for c in ["ggr_brl", "ngr_brl", "turnover_brl", "depositos_brl", "sessoes",
                   "turnover_casino_brl", "turnover_sports_brl"]:
            df_fin[c] = pd.to_numeric(df_fin[c], errors="coerce").fillna(0)

    path = os.path.join(OUTPUT_DIR, "financeiro_coorte.csv")
    df_fin.to_csv(path, index=False, sep=";", encoding="utf-8-sig")
    log.info(f"  Salvo: {path}")
    if not df_fin.empty:
        log.info(f"  GGR total março: R$ {df_fin['ggr_brl'].sum():,.2f}")
        log.info(f"  Turnover total: R$ {df_fin['turnover_brl'].sum():,.2f}")
        log.info(f"  Depósitos total: R$ {df_fin['depositos_brl'].sum():,.2f}")
    return df_fin, df_coorte


# ================================================================
# CSV 4: Top jogos da coorte
# ================================================================
def extract_top_jogos(all_users):
    log.info("=" * 60)
    log.info("CSV 4: Top jogos da coorte CRM")
    log.info("Fonte: Athena ps_bi.fct_casino_activity_daily + dim_game")

    user_strs = [str(u) for u in all_users]
    all_games = []

    # Buscar em 2 batches grandes para manter agregação correta
    half = len(user_strs) // 2
    batches = [user_strs[:half], user_strs[half:]]

    for i, batch in enumerate(batches):
        ids = ",".join(batch)
        sql = f"""
        SELECT ca.game_id,
            COUNT(DISTINCT ca.player_id) AS users,
            SUM(ca.bet_amount_base) AS turnover_brl,
            SUM(ca.ggr_base) AS ggr_brl
        FROM ps_bi.fct_casino_activity_daily ca
        JOIN ps_bi.dim_user du ON du.ecr_id = ca.player_id
        WHERE du.external_id IN ({ids})
          AND du.is_test = false
          AND ca.activity_date BETWEEN DATE '{DATE_FROM}' AND DATE '{DATE_TO}'
        GROUP BY ca.game_id
        """
        df_batch = query_athena(sql, database="ps_bi")
        all_games.append(df_batch)
        log.info(f"  Batch {i + 1}/2: {len(df_batch)} jogos")

    df_games = pd.concat(all_games, ignore_index=True)
    for c in ["users", "turnover_brl", "ggr_brl"]:
        df_games[c] = pd.to_numeric(df_games[c], errors="coerce").fillna(0)

    # Agregar por game_id
    agg = df_games.groupby("game_id").agg(
        users=("users", "sum"),
        turnover_brl=("turnover_brl", "sum"),
        ggr_brl=("ggr_brl", "sum"),
    ).reset_index().sort_values("turnover_brl", ascending=False)

    # Buscar nomes do dim_game
    gids = agg["game_id"].head(50).tolist()
    gids_str = ",".join(f"'{g}'" for g in gids)
    try:
        sql_names = f"SELECT game_id, game_desc FROM ps_bi.dim_game WHERE game_id IN ({gids_str})"
        df_names = query_athena(sql_names, database="ps_bi")
        name_map = dict(zip(df_names["game_id"], df_names["game_desc"]))
    except Exception:
        name_map = {}

    # Adicionar mapeamento manual
    name_map.update(KNOWN_GAMES)

    agg["game_name"] = agg["game_id"].map(lambda x: name_map.get(str(x), str(x)))
    agg["rtp_pct"] = agg.apply(
        lambda r: round((1 - r["ggr_brl"] / r["turnover_brl"]) * 100, 1) if r["turnover_brl"] > 0 else 0,
        axis=1,
    )

    path = os.path.join(OUTPUT_DIR, "top_jogos.csv")
    agg.head(30).to_csv(path, index=False, sep=";", encoding="utf-8-sig")
    log.info(f"  Salvo: {path}")
    log.info(f"  Top 5: {agg.head(5)[['game_name', 'users', 'turnover_brl']].to_string(index=False)}")
    return agg


# ================================================================
# CSV 5: VIP groups
# ================================================================
def extract_vip(all_users):
    log.info("=" * 60)
    log.info("CSV 5: VIP groups")
    log.info("Fonte: Athena ps_bi (NGR acumulado março)")

    user_strs = [str(u) for u in all_users]
    all_ngr = []

    for i in range(0, len(user_strs), 500):
        batch = user_strs[i:i + 500]
        ids = ",".join(batch)
        sql = f"""
        SELECT du.external_id AS user_ext_id,
            SUM(p.ngr_base) AS ngr_brl,
            COUNT(DISTINCT p.activity_date) AS play_days
        FROM ps_bi.dim_user du
        JOIN ps_bi.fct_player_activity_daily p ON p.player_id = du.ecr_id
        WHERE du.external_id IN ({ids})
          AND du.is_test = false
          AND p.activity_date BETWEEN DATE '{DATE_FROM}' AND DATE '{DATE_TO}'
        GROUP BY du.external_id
        """
        df_batch = query_athena(sql, database="ps_bi")
        all_ngr.append(df_batch)

    df = pd.concat(all_ngr, ignore_index=True)
    for c in ["ngr_brl", "play_days"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    def vip_tier(ngr):
        if ngr >= 10000:
            return "Elite"
        if ngr >= 5000:
            return "Key Account"
        if ngr >= 3000:
            return "High Value"
        return "Standard"

    df["vip_tier"] = df["ngr_brl"].apply(vip_tier)

    path = os.path.join(OUTPUT_DIR, "vip_groups.csv")
    df.to_csv(path, index=False, sep=";", encoding="utf-8-sig")
    log.info(f"  Salvo: {path}")

    summary = df.groupby("vip_tier").agg(
        users=("user_ext_id", "count"),
        ngr_total=("ngr_brl", "sum"),
        apd_medio=("play_days", "mean"),
    ).reset_index()
    log.info(f"  VIP summary:\n{summary.to_string(index=False)}")
    return df


# ================================================================
# MAIN
# ================================================================
def main():
    ensure_dir()

    log.info("Extração CRM CSVs — Março 2026")
    log.info(f"Período: {DATE_FROM} a {DATE_TO} (BRT)")
    log.info("")

    # 1. Campanhas diárias
    df_camps = extract_campanhas_diarias()

    # 2. Disparos
    df_disp = extract_disparos()

    # 3. Financeiro (inclui coorte)
    df_fin, df_coorte = extract_financeiro(df_camps)

    # 4. Top jogos
    all_users = df_coorte["user_ext_id"].unique().tolist()
    df_jogos = extract_top_jogos(all_users)

    # 5. VIP
    df_vip = extract_vip(all_users)

    log.info("")
    log.info("=" * 60)
    log.info("EXTRAÇÃO COMPLETA")
    log.info(f"Arquivos em: {OUTPUT_DIR}")
    for f in sorted(os.listdir(OUTPUT_DIR)):
        if f.endswith(".csv"):
            size = os.path.getsize(os.path.join(OUTPUT_DIR, f))
            log.info(f"  {f} ({size:,} bytes)")


if __name__ == "__main__":
    main()