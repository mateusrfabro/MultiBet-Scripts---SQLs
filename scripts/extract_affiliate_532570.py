"""
Extração: Dados do Afiliado 532570 (Elisa)
Task: Dudu pediu números (utm_source=ig, campaign=roleta).
Fontes: ps_bi + ecr_ec2 + bireports_ec2 (Athena)
"""
import sys, os, logging
from datetime import datetime
sys.path.insert(0, "c:/Users/NITRO/OneDrive - PGX/MultiBet")
from db.athena import query_athena
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

AFF = "532570"
OUT = "c:/Users/NITRO/OneDrive - PGX/MultiBet/reports/affiliate_532570_FINAL.xlsx"

SQL_INFO = f"""
SELECT CAST(c_affiliate_id AS VARCHAR) AS affiliate_id,
  MAX(NULLIF(c_affiliate_name,'')) AS affiliate_name,
  MAX(REGEXP_EXTRACT(c_reference_url,'utm_source=([^&]+)',1)) AS utm_source,
  MAX(REGEXP_EXTRACT(c_reference_url,'utm_medium=([^&]+)',1)) AS utm_medium,
  MAX(REGEXP_EXTRACT(c_reference_url,'utm_campaign=([^&]+)',1)) AS utm_campaign,
  COUNT(DISTINCT c_ecr_id) AS total_clicks,
  CAST(MIN(c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS VARCHAR) AS first_click_brt,
  CAST(MAX(c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS VARCHAR) AS last_click_brt
FROM ecr_ec2.tbl_ecr_banner
WHERE CAST(c_affiliate_id AS VARCHAR) = '{AFF}' GROUP BY 1"""

SQL_PLAYERS = f"""
SELECT u.ecr_id, u.external_id,
  CAST(u.signup_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS VARCHAR) AS registro_brt,
  u.ftd_date,
  CAST(u.ftd_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS VARCHAR) AS ftd_datetime_brt,
  u.ftd_amount_inhouse AS ftd_valor_brl, u.affiliate_id, u.tracker_id, u.country_code, u.ecr_currency
FROM ps_bi.dim_user u
WHERE CAST(u.affiliate_id AS VARCHAR) = '{AFF}' AND (u.is_test = false OR u.is_test IS NULL)
ORDER BY u.signup_datetime DESC"""

SQL_KPIS = f"""
WITH p AS (SELECT ecr_id FROM ps_bi.dim_user WHERE CAST(affiliate_id AS VARCHAR)='{AFF}' AND (is_test=false OR is_test IS NULL))
SELECT a.activity_date, COUNT(DISTINCT a.player_id) AS players_ativos,
  SUM(a.deposit_success_count) AS qty_depositos, SUM(a.deposit_success_base) AS total_depositos_brl,
  SUM(a.ftd_count) AS qty_ftds,
  SUM(a.casino_realbet_count) AS qty_bets_casino,
  SUM(a.casino_realbet_base)-SUM(a.casino_real_win_base) AS casino_ggr_brl,
  SUM(a.sb_realbet_count) AS qty_bets_sports,
  SUM(a.sb_realbet_base)-SUM(a.sb_real_win_base) AS sports_ggr_brl,
  SUM(a.ggr_base) AS total_ggr_brl, SUM(a.ngr_base) AS total_ngr_brl,
  SUM(a.cashout_success_count) AS qty_saques, SUM(a.cashout_success_base) AS total_saques_brl,
  SUM(a.deposit_success_base)-SUM(a.cashout_success_base) AS net_deposit_brl,
  SUM(a.nrc_count) AS nrc, SUM(a.login_count) AS logins
FROM ps_bi.fct_player_activity_daily a INNER JOIN p ON a.player_id=p.ecr_id
GROUP BY 1 ORDER BY 1 DESC"""

SQL_RESUMO = f"""
WITH p AS (SELECT ecr_id,external_id,ftd_date,ftd_amount_inhouse AS ftd_brl,
  CAST(signup_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS VARCHAR) AS reg_brt
  FROM ps_bi.dim_user WHERE CAST(affiliate_id AS VARCHAR)='{AFF}' AND (is_test=false OR is_test IS NULL))
SELECT p.ecr_id,p.external_id,p.reg_brt,p.ftd_date,p.ftd_brl,
  COALESCE(SUM(a.deposit_success_count),0) AS lt_depositos,
  COALESCE(SUM(a.deposit_success_base),0) AS lt_depositos_brl,
  COALESCE(SUM(a.ggr_base),0) AS lt_ggr_brl,
  COALESCE(SUM(a.ngr_base),0) AS lt_ngr_brl,
  COALESCE(SUM(a.cashout_success_count),0) AS lt_saques,
  COALESCE(SUM(a.cashout_success_base),0) AS lt_saques_brl,
  COALESCE(SUM(a.deposit_success_base),0)-COALESCE(SUM(a.cashout_success_base),0) AS lt_net_dep_brl,
  MIN(a.activity_date) AS primeiro_dia, MAX(a.activity_date) AS ultimo_dia,
  COUNT(DISTINCT a.activity_date) AS dias_ativos
FROM p LEFT JOIN ps_bi.fct_player_activity_daily a ON a.player_id=p.ecr_id
GROUP BY 1,2,3,4,5 ORDER BY lt_ggr_brl DESC NULLS LAST"""

SQL_JOGOS = f"""
WITH p AS (SELECT ecr_id FROM ps_bi.dim_user WHERE CAST(affiliate_id AS VARCHAR)='{AFF}' AND (is_test=false OR is_test IS NULL))
SELECT c.game_id,
  COALESCE(g.game_desc, b.c_game_desc, CONCAT('ID:',c.game_id)) AS game_name,
  COALESCE(g.vendor_id, b.c_vendor_id, c.sub_vendor_id) AS provider,
  COUNT(DISTINCT c.player_id) AS players, SUM(c.bet_count) AS rodadas,
  SUM(c.real_bet_amount_base) AS apostas_brl, SUM(c.real_win_amount_base) AS ganhos_brl,
  SUM(c.ggr_base) AS ggr_brl,
  CASE WHEN SUM(c.real_bet_amount_base)>0 THEN ROUND(SUM(c.ggr_base)/SUM(c.real_bet_amount_base)*100,2) ELSE 0 END AS hold_pct
FROM ps_bi.fct_casino_activity_daily c
INNER JOIN p ON c.player_id=p.ecr_id
LEFT JOIN ps_bi.dim_game g ON c.game_id=g.game_id
LEFT JOIN bireports_ec2.tbl_vendor_games_mapping_data b ON c.game_id=b.c_game_id
GROUP BY 1,2,3 ORDER BY ggr_brl DESC"""

def run():
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    R = {}
    queries = [
        ("Info_Afiliado", SQL_INFO, "ecr_ec2"),
        ("Players", SQL_PLAYERS, "ps_bi"),
        ("KPIs_Diarios", SQL_KPIS, "ps_bi"),
        ("Resumo_Player", SQL_RESUMO, "ps_bi"),
        ("Jogos", SQL_JOGOS, "ps_bi"),
    ]
    for name, sql, db in queries:
        log.info(">>> %s ...", name)
        try:
            R[name] = query_athena(sql, database=db)
            log.info("   OK: %d rows", len(R[name]))
        except Exception as e:
            log.error("   ERRO: %s", e)
            R[name] = pd.DataFrame()

    # Sumario
    log.info("=" * 50)
    if not R["Players"].empty:
        n = len(R["Players"]); f = R["Players"]["ftd_date"].notna().sum()
        log.info("Players: %d | FTDs: %d (%.1f%%)", n, f, f/n*100 if n else 0)
    if not R["KPIs_Diarios"].empty:
        t = R["KPIs_Diarios"]
        log.info("Dep: R$%.2f | GGR: R$%.2f | NGR: R$%.2f | Saques: R$%.2f | Net: R$%.2f",
                 t["total_depositos_brl"].sum(), t["total_ggr_brl"].sum(), t["total_ngr_brl"].sum(),
                 t["total_saques_brl"].sum(), t["net_deposit_brl"].sum())

    # Timezone cleanup
    for df in R.values():
        for col in df.columns:
            if hasattr(df[col],'dt') and hasattr(df[col].dt,'tz') and df[col].dt.tz is not None:
                df[col] = df[col].astype(str)

    # Excel
    log.info(">>> Salvando %s", OUT)
    with pd.ExcelWriter(OUT, engine="openpyxl") as w:
        for name, df in R.items():
            df.to_excel(w, sheet_name=name, index=False)
        pd.DataFrame([
            {"Coluna":"affiliate_id","Desc":"ID afiliado Pragmatic","Unidade":"-"},
            {"Coluna":"utm_source/medium/campaign","Desc":"Parametros UTM do clique","Unidade":"-"},
            {"Coluna":"ecr_id","Desc":"ID interno jogador","Unidade":"-"},
            {"Coluna":"external_id","Desc":"ID externo (=Smartico user_ext_id)","Unidade":"-"},
            {"Coluna":"registro_brt","Desc":"Data/hora registro (BRT)","Unidade":"-"},
            {"Coluna":"ftd_date / ftd_datetime_brt","Desc":"Data/hora primeiro deposito","Unidade":"-"},
            {"Coluna":"ftd_valor_brl / ftd_brl","Desc":"Valor primeiro deposito","Unidade":"BRL"},
            {"Coluna":"*_depositos_brl","Desc":"Depositos confirmados","Unidade":"BRL"},
            {"Coluna":"*_ggr_brl","Desc":"GGR = Apostas - Ganhos jogador (realcash)","Unidade":"BRL"},
            {"Coluna":"*_ngr_brl","Desc":"NGR = GGR - Bonus Turnedreal","Unidade":"BRL"},
            {"Coluna":"net_deposit_brl","Desc":"Depositos - Saques","Unidade":"BRL"},
            {"Coluna":"hold_pct","Desc":"Hold Rate = GGR/Apostas x100","Unidade":"%"},
            {"Coluna":"game_name","Desc":"Nome do jogo (dim_game + bireports catalog)","Unidade":"-"},
            {"Coluna":"provider","Desc":"Vendor/provedor do jogo","Unidade":"-"},
            {"Coluna":"","Desc":"","Unidade":""},
            {"Coluna":"GLOSSARIO","Desc":"","Unidade":""},
            {"Coluna":"GGR","Desc":"Gross Gaming Revenue","Unidade":"BRL"},
            {"Coluna":"NGR","Desc":"Net Gaming Revenue = GGR - Bonus","Unidade":"BRL"},
            {"Coluna":"FTD","Desc":"First Time Deposit","Unidade":"BRL"},
            {"Coluna":"NRC","Desc":"New Registered Customer","Unidade":"qty"},
            {"Coluna":"","Desc":"","Unidade":""},
            {"Coluna":"FONTE","Desc":"AWS Athena (ps_bi+ecr_ec2+bireports_ec2)","Unidade":""},
            {"Coluna":"Periodo","Desc":"Lifetime (todos dados disponiveis)","Unidade":""},
            {"Coluna":"Extraido","Desc":datetime.now().strftime("%Y-%m-%d %H:%M BRT"),"Unidade":""},
            {"Coluna":"","Desc":"","Unidade":""},
            {"Coluna":"ACAO SUGERIDA","Desc":"","Unidade":""},
            {"Coluna":"1","Desc":"Avaliar conversao NRC->FTD (5.5% baixo) e custo vs GGR","Unidade":""},
            {"Coluna":"2","Desc":"Afiliado novo (2 dias) - acompanhar evolucao semanal","Unidade":""},
            {"Coluna":"3","Desc":"Top jogo: Aviator (R$750 GGR) - verificar concentracao","Unidade":""},
        ]).to_excel(w, sheet_name="Legenda", index=False)
    log.info("DONE: %s", OUT)
    return R

if __name__ == "__main__":
    run()
