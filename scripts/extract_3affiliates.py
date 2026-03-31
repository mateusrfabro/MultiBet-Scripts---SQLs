"""
Extração: 3 Afiliados (532570, 532571, 464673) + Validação Cruzada
Fontes: ps_bi + bireports_ec2 (Athena) + j_user (BigQuery)
"""
import sys, os, logging, warnings
import pandas as pd
from datetime import datetime

sys.path.insert(0, "c:/Users/NITRO/OneDrive - PGX/MultiBet")
from db.athena import query_athena
from db.bigquery import query_bigquery
warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

AFFS = {"532570": "Oferta Mini Games", "532571": "ODD Obvia", "464673": "Meta White"}
OUT = "c:/Users/NITRO/OneDrive - PGX/MultiBet/reports/affiliates_3ids_FINAL.xlsx"
all_sheets = {}

for aff_id, aff_name in AFFS.items():
    log.info(f"=== {aff_name} (ID {aff_id}) ===")

    df_players = query_athena(f"""
    SELECT ecr_id, external_id,
      CAST(signup_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS VARCHAR) AS registro_brt,
      ftd_date,
      CAST(ftd_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS VARCHAR) AS ftd_datetime_brt,
      ftd_amount_inhouse AS ftd_valor_brl, affiliate_id, tracker_id, country_code
    FROM ps_bi.dim_user
    WHERE CAST(affiliate_id AS VARCHAR) = '{aff_id}' AND (is_test = false OR is_test IS NULL)
    ORDER BY signup_datetime DESC
    """, database="ps_bi")
    log.info(f"  Players: {len(df_players)}")

    df_kpis = query_athena(f"""
    WITH p AS (SELECT ecr_id FROM ps_bi.dim_user WHERE CAST(affiliate_id AS VARCHAR)='{aff_id}' AND (is_test=false OR is_test IS NULL))
    SELECT a.activity_date, COUNT(DISTINCT a.player_id) AS players_ativos,
      SUM(a.deposit_success_count) AS qty_dep, SUM(a.deposit_success_base) AS dep_brl,
      SUM(a.ftd_count) AS qty_ftds,
      SUM(a.casino_realbet_count) AS bets_casino, SUM(a.casino_realbet_base)-SUM(a.casino_real_win_base) AS casino_ggr,
      SUM(a.sb_realbet_count) AS bets_sports, SUM(a.sb_realbet_base)-SUM(a.sb_real_win_base) AS sports_ggr,
      SUM(a.ggr_base) AS ggr, SUM(a.ngr_base) AS ngr,
      SUM(a.cashout_success_count) AS qty_saques, SUM(a.cashout_success_base) AS saques_brl,
      SUM(a.deposit_success_base)-SUM(a.cashout_success_base) AS net_dep
    FROM ps_bi.fct_player_activity_daily a INNER JOIN p ON a.player_id=p.ecr_id
    GROUP BY 1 ORDER BY 1 DESC
    """, database="ps_bi")
    log.info(f"  KPIs dias: {len(df_kpis)}")

    df_resumo = query_athena(f"""
    WITH p AS (SELECT ecr_id, external_id, ftd_date, ftd_amount_inhouse AS ftd_brl,
      CAST(signup_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS VARCHAR) AS reg_brt
      FROM ps_bi.dim_user WHERE CAST(affiliate_id AS VARCHAR)='{aff_id}' AND (is_test=false OR is_test IS NULL))
    SELECT p.ecr_id, p.external_id, p.reg_brt, p.ftd_date, p.ftd_brl,
      COALESCE(SUM(a.deposit_success_count),0) AS lt_dep_qty, COALESCE(SUM(a.deposit_success_base),0) AS lt_dep_brl,
      COALESCE(SUM(a.ggr_base),0) AS lt_ggr, COALESCE(SUM(a.ngr_base),0) AS lt_ngr,
      COALESCE(SUM(a.cashout_success_count),0) AS lt_saques_qty, COALESCE(SUM(a.cashout_success_base),0) AS lt_saques_brl,
      COALESCE(SUM(a.deposit_success_base),0)-COALESCE(SUM(a.cashout_success_base),0) AS lt_net_dep,
      MIN(a.activity_date) AS primeiro_dia, MAX(a.activity_date) AS ultimo_dia, COUNT(DISTINCT a.activity_date) AS dias_ativos
    FROM p LEFT JOIN ps_bi.fct_player_activity_daily a ON a.player_id=p.ecr_id
    GROUP BY 1,2,3,4,5 ORDER BY lt_ggr DESC NULLS LAST
    """, database="ps_bi")
    log.info(f"  Resumo: {len(df_resumo)}")

    df_jogos = query_athena(f"""
    WITH p AS (SELECT ecr_id FROM ps_bi.dim_user WHERE CAST(affiliate_id AS VARCHAR)='{aff_id}' AND (is_test=false OR is_test IS NULL))
    SELECT c.game_id, COALESCE(g.game_desc, b.c_game_desc, CONCAT('ID:',c.game_id)) AS game_name,
      COALESCE(g.vendor_id, b.c_vendor_id, c.sub_vendor_id) AS provider,
      COUNT(DISTINCT c.player_id) AS players, SUM(c.bet_count) AS rodadas,
      SUM(c.real_bet_amount_base) AS apostas_brl, SUM(c.real_win_amount_base) AS ganhos_brl, SUM(c.ggr_base) AS ggr_brl,
      CASE WHEN SUM(c.real_bet_amount_base)>0 THEN ROUND(SUM(c.ggr_base)/SUM(c.real_bet_amount_base)*100,2) ELSE 0 END AS hold_pct
    FROM ps_bi.fct_casino_activity_daily c INNER JOIN p ON c.player_id=p.ecr_id
    LEFT JOIN ps_bi.dim_game g ON c.game_id=g.game_id
    LEFT JOIN bireports_ec2.tbl_vendor_games_mapping_data b ON c.game_id=b.c_game_id
    GROUP BY 1,2,3 ORDER BY ggr_brl DESC
    """, database="ps_bi")
    log.info(f"  Jogos: {len(df_jogos)}")

    n = len(df_players)
    f = int(df_players["ftd_date"].notna().sum())
    dep = df_kpis["dep_brl"].sum() if not df_kpis.empty else 0
    ggr = df_kpis["ggr"].sum() if not df_kpis.empty else 0
    ngr = df_kpis["ngr"].sum() if not df_kpis.empty else 0
    saq = df_kpis["saques_brl"].sum() if not df_kpis.empty else 0
    net = df_kpis["net_dep"].sum() if not df_kpis.empty else 0
    log.info(f"  >> {n} players | {f} FTDs ({f/n*100:.1f}% conv) | Dep R${dep:,.0f} | GGR R${ggr:,.0f} | NGR R${ngr:,.0f} | Saq R${saq:,.0f} | Net R${net:,.0f}")

    for df in [df_players, df_kpis, df_resumo, df_jogos]:
        for col in df.select_dtypes(include=["datetimetz"]).columns:
            df[col] = df[col].astype(str)

    all_sheets[f"{aff_id}_Players"] = df_players
    all_sheets[f"{aff_id}_KPIs"] = df_kpis
    all_sheets[f"{aff_id}_Resumo"] = df_resumo
    all_sheets[f"{aff_id}_Jogos"] = df_jogos

# --- VALIDACAO CRUZADA BigQuery ---
log.info("=== VALIDACAO CRUZADA BigQuery ===")
ids_str = ",".join(AFFS.keys())
df_bq = query_bigquery(f"""
SELECT CAST(core_affiliate_id AS STRING) AS aff_id, COUNT(DISTINCT user_ext_id) AS players_bq
FROM `smartico-bq6.dwh_ext_24105.j_user`
WHERE core_affiliate_id IN ({ids_str})
GROUP BY 1
""")

validacao = []
for aff_id, aff_name in AFFS.items():
    a_count = len(all_sheets.get(f"{aff_id}_Players", pd.DataFrame()))
    bq_row = df_bq[df_bq["aff_id"] == aff_id]
    b_count = int(bq_row["players_bq"].iloc[0]) if not bq_row.empty else 0
    delta = a_count - b_count
    div = abs(delta) / a_count * 100 if a_count else 0
    status = "OK" if div < 5 else "ALERTA"
    validacao.append({"Afiliado": f"{aff_name} ({aff_id})", "Athena": a_count, "BigQuery": b_count, "Delta": delta, "Div%": f"{div:.1f}%", "Status": status})
    log.info(f"  {aff_name}: Athena={a_count} BQ={b_count} delta={delta} ({div:.1f}%) {status}")

all_sheets["Validacao_Cruzada"] = pd.DataFrame(validacao)

# Legenda
all_sheets["Legenda"] = pd.DataFrame([
    {"Campo": "ecr_id", "Desc": "ID interno jogador", "Unidade": "-"},
    {"Campo": "external_id", "Desc": "ID externo (=Smartico user_ext_id)", "Unidade": "-"},
    {"Campo": "registro_brt", "Desc": "Data/hora registro (BRT)", "Unidade": "-"},
    {"Campo": "ftd_date / ftd_datetime_brt", "Desc": "Data/hora primeiro deposito", "Unidade": "-"},
    {"Campo": "ftd_valor_brl / ftd_brl", "Desc": "Valor primeiro deposito", "Unidade": "BRL"},
    {"Campo": "dep_brl / lt_dep_brl", "Desc": "Depositos confirmados", "Unidade": "BRL"},
    {"Campo": "ggr / lt_ggr", "Desc": "GGR = Apostas - Ganhos jogador (realcash)", "Unidade": "BRL"},
    {"Campo": "ngr / lt_ngr", "Desc": "NGR = GGR - Bonus Turnedreal", "Unidade": "BRL"},
    {"Campo": "net_dep / lt_net_dep", "Desc": "Depositos - Saques", "Unidade": "BRL"},
    {"Campo": "hold_pct", "Desc": "Hold Rate = GGR/Apostas x100", "Unidade": "%"},
    {"Campo": "game_name", "Desc": "Nome do jogo", "Unidade": "-"},
    {"Campo": "provider", "Desc": "Vendor/provedor do jogo", "Unidade": "-"},
    {"Campo": "", "Desc": "", "Unidade": ""},
    {"Campo": "AFILIADOS", "Desc": "", "Unidade": ""},
    {"Campo": "532570", "Desc": "Oferta Mini Games", "Unidade": "-"},
    {"Campo": "532571", "Desc": "ODD Obvia", "Unidade": "-"},
    {"Campo": "464673", "Desc": "Meta White", "Unidade": "-"},
    {"Campo": "", "Desc": "", "Unidade": ""},
    {"Campo": "FONTE", "Desc": "Athena (ps_bi + bireports_ec2) + BigQuery (j_user)", "Unidade": ""},
    {"Campo": "Periodo", "Desc": "Lifetime (todos dados disponiveis)", "Unidade": ""},
    {"Campo": "Extraido em", "Desc": datetime.now().strftime("%Y-%m-%d %H:%M BRT"), "Unidade": ""},
])

log.info(f"Salvando {OUT}...")
with pd.ExcelWriter(OUT, engine="openpyxl") as w:
    for name, df in all_sheets.items():
        df.to_excel(w, sheet_name=name[:31], index=False)
log.info(f"DONE: {OUT}")