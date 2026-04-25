"""
Extracao: Base total de players do affiliate 363722 (Pri Simoes)
Demanda: Rapha (CRM) via Head - precisa CSV com ext_id Smartico, nomes e info do affiliate.
Fonte: Athena (ps_bi.dim_user) + validacao cruzada com ecr_ec2.tbl_ecr_banner
Regras aplicadas:
  - timezone UTC -> America/Sao_Paulo
  - affiliate_id VARCHAR (CAST)
  - is_test = false OR IS NULL
  - BigQuery desativado -> validacao cruzada 100% Athena
"""
import sys
import os
import logging
from datetime import datetime

sys.path.insert(0, "c:/Users/NITRO/OneDrive - PGX/Projetos - Super Nova/MultiBet")
from db.athena import query_athena
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

AFF_ID = "363722"
AFF_NAME = "Pri Simoes"
BASE_DIR = "c:/Users/NITRO/OneDrive - PGX/Projetos - Super Nova/MultiBet/reports"
CSV_OUT = f"{BASE_DIR}/affiliate_363722_pri_simoes_base_players_FINAL.csv"
LEG_OUT = f"{BASE_DIR}/affiliate_363722_pri_simoes_base_players_FINAL_legenda.txt"


# =============================================================================
# 1. EXTRACAO - base de players do affiliate
# =============================================================================
SQL_PLAYERS = f"""
SELECT
    u.ecr_id,
    u.external_id                                                                         AS smartico_id,
    TRIM(
        COALESCE(u.first_name,'') || ' ' ||
        COALESCE(u.middle_name,'') || ' ' ||
        COALESCE(u.last_name,'')  || ' ' ||
        COALESCE(u.second_last_name,'')
    )                                                                                      AS nome_completo,
    u.screen_name,
    CAST(u.affiliate_id AS VARCHAR)                                                        AS affiliate_id,
    CAST(u.signup_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS VARCHAR) AS signup_brt,
    u.tracker_id,
    u.country_code
FROM ps_bi.dim_user u
WHERE CAST(u.affiliate_id AS VARCHAR) = '{AFF_ID}'
  AND (u.is_test = false OR u.is_test IS NULL)
  AND u.external_id IS NOT NULL
ORDER BY u.signup_datetime ASC
"""


# =============================================================================
# 2. AUDITOR - validacao cruzada 100% Athena
# =============================================================================
# 2a. Count reconciliation: ps_bi.dim_user vs ecr_ec2.tbl_ecr (cadastro)
SQL_AUDIT_ECR = f"""
SELECT COUNT(DISTINCT c_ecr_id) AS ecr_ec2_count
FROM ecr_ec2.tbl_ecr
WHERE CAST(c_affiliate_id AS VARCHAR) = '{AFF_ID}'
"""

# 2b. Cliques no banner (dimensao atribuicao)
SQL_AUDIT_BANNER = f"""
SELECT COUNT(DISTINCT c_ecr_id) AS banner_count
FROM ecr_ec2.tbl_ecr_banner
WHERE CAST(c_affiliate_id AS VARCHAR) = '{AFF_ID}'
"""

# 2c. Integridade: external_id unico? duplicatas?
SQL_AUDIT_DUP = f"""
SELECT COUNT(*) AS total, COUNT(DISTINCT external_id) AS ext_id_unicos,
       COUNT(*) - COUNT(DISTINCT external_id) AS duplicatas
FROM ps_bi.dim_user
WHERE CAST(affiliate_id AS VARCHAR) = '{AFF_ID}'
  AND (is_test = false OR is_test IS NULL)
  AND external_id IS NOT NULL
"""


def run():
    os.makedirs(BASE_DIR, exist_ok=True)

    log.info(">>> EXTRACAO: players do affiliate %s (%s)", AFF_ID, AFF_NAME)
    df = query_athena(SQL_PLAYERS, database="ps_bi")
    log.info("    OK: %d players extraidos", len(df))

    # =====================================================================
    # AUDITOR
    # =====================================================================
    log.info(">>> AUDITOR (validacao cruzada 100% Athena)")
    df_ecr    = query_athena(SQL_AUDIT_ECR,    database="ecr_ec2")
    df_banner = query_athena(SQL_AUDIT_BANNER, database="ecr_ec2")
    df_dup    = query_athena(SQL_AUDIT_DUP,    database="ps_bi")

    n_psbi   = len(df)
    n_ecr    = int(df_ecr["ecr_ec2_count"].iloc[0])
    n_banner = int(df_banner["banner_count"].iloc[0])
    n_tot    = int(df_dup["total"].iloc[0])
    n_uniq   = int(df_dup["ext_id_unicos"].iloc[0])
    n_dup    = int(df_dup["duplicatas"].iloc[0])

    # Divergencia ps_bi vs ecr_ec2.tbl_ecr (fonte de cadastro bruto)
    delta_ecr = n_psbi - n_ecr
    div_ecr   = abs(delta_ecr) / n_ecr * 100 if n_ecr else 0
    status_ecr = "OK" if div_ecr < 2 else ("ALERTA" if div_ecr < 5 else "FALHA")

    # Divergencia ps_bi vs banner (atribuicao)
    delta_ban = n_psbi - n_banner
    div_ban   = abs(delta_ban) / n_banner * 100 if n_banner else 0
    status_ban = "OK" if div_ban < 2 else ("ALERTA" if div_ban < 5 else "FALHA")

    status_uniq = "OK" if n_dup == 0 else "FALHA"

    log.info("-" * 70)
    log.info("  ps_bi.dim_user           : %d players", n_psbi)
    log.info("  ecr_ec2.tbl_ecr          : %d (delta %+d | %.2f%%) [%s]",
             n_ecr, delta_ecr, div_ecr, status_ecr)
    log.info("  ecr_ec2.tbl_ecr_banner   : %d (delta %+d | %.2f%%) [%s]",
             n_banner, delta_ban, div_ban, status_ban)
    log.info("  Unicidade external_id    : %d/%d unicos | %d dup [%s]",
             n_uniq, n_tot, n_dup, status_uniq)
    log.info("-" * 70)

    fail = any(s == "FALHA" for s in (status_ecr, status_ban, status_uniq))
    if fail:
        log.error("AUDITOR REPROVOU a extracao - revisar antes de entregar")
    else:
        log.info("AUDITOR APROVADO - divergencias dentro da tolerancia")

    # =====================================================================
    # CSV
    # =====================================================================
    log.info(">>> Salvando CSV: %s", CSV_OUT)
    df.to_csv(CSV_OUT, index=False, encoding="utf-8-sig")
    log.info("    OK: %d linhas | %.1f KB", len(df), os.path.getsize(CSV_OUT) / 1024)

    # =====================================================================
    # LEGENDA
    # =====================================================================
    legenda = f"""============================================================
BASE TOTAL DE PLAYERS - AFFILIATE 363722 (Pri Simoes)
============================================================

DEMANDANTE    : Rapha (CRM)
EXTRAIDO POR  : Squad Intelligence Engine
DATA EXTRACAO : {datetime.now().strftime("%Y-%m-%d %H:%M BRT")}
FONTE         : AWS Athena - ps_bi.dim_user (camada BI dbt)

------------------------------------------------------------
DICIONARIO DE COLUNAS
------------------------------------------------------------

ecr_id          Identificador interno do player (ID 18 digitos MultiBet).
                Unico por player.

smartico_id     ID do player no Smartico (= ps_bi.dim_user.external_id).
                Este e o campo que deve ser usado para matching na
                plataforma do CRM (Smartico user_ext_id).

nome_completo   Nome concatenado: first_name + middle_name + last_name +
                second_last_name (espacos extras foram removidos).

screen_name     Nickname do player na plataforma MultiBet.

affiliate_id    ID do afiliado (363722 - Pri Simoes).

signup_brt      Data e hora do cadastro em horario de Brasilia (UTC-3).

tracker_id      ID do tracker/campanha de aquisicao.
                Pode vir vazio se o player nao foi capturado por
                tracker especifico (so affiliate).

country_code    ISO 3166-1 alpha-2 (ex: BR = Brasil).

------------------------------------------------------------
REGRAS DE EXTRACAO
------------------------------------------------------------

- Filtro principal: affiliate_id = 363722 (Pri Simoes)
- Exclusao: players is_test = true
- Exclusao: players sem external_id (sem ID Smartico -> nao sao
  utilizaveis no CRM)
- Ordenacao: data de cadastro crescente

------------------------------------------------------------
VALIDACAO CRUZADA (100% ATHENA)
------------------------------------------------------------

Fonte / Contagem / Delta vs ps_bi / Status
ps_bi.dim_user            {n_psbi}      -            BASE
ecr_ec2.tbl_ecr           {n_ecr}      {delta_ecr:+d}  ({div_ecr:.2f}%)  [{status_ecr}]
ecr_ec2.tbl_ecr_banner    {n_banner}   {delta_ban:+d}  ({div_ban:.2f}%)  [{status_ban}]

Integridade Smartico ID:
  Total de linhas              : {n_tot}
  external_id unicos           : {n_uniq}
  Duplicatas                   : {n_dup}  [{status_uniq}]

OBS: BigQuery da Smartico foi desativado em 19/04/2026, portanto a
validacao cruzada via j_user nao foi possivel. A reconciliacao foi
feita 100% no Athena com 3 fontes independentes (ps_bi.dim_user,
ecr_ec2.tbl_ecr, ecr_ec2.tbl_ecr_banner).

STATUS FINAL DO AUDITOR: {"APROVADO" if not fail else "REPROVADO"}

------------------------------------------------------------
ACAO SUGERIDA
------------------------------------------------------------

Usar a coluna "smartico_id" como chave de matching na plataforma
Smartico. Campo equivale a user_ext_id no BigQuery/Smartico.

Qualquer duvida sobre a extracao, falar com o time de dados.
"""
    with open(LEG_OUT, "w", encoding="utf-8") as f:
        f.write(legenda)
    log.info(">>> Salvando LEGENDA: %s", LEG_OUT)

    # =====================================================================
    # SUMARIO FINAL
    # =====================================================================
    log.info("=" * 70)
    log.info("ENTREGA: affiliate 363722 - Pri Simoes")
    log.info("Players .......: %d", n_psbi)
    log.info("Periodo signup : %s -> %s", df["signup_brt"].min()[:10], df["signup_brt"].max()[:10])
    log.info("CSV ...........: %s", CSV_OUT)
    log.info("Legenda .......: %s", LEG_OUT)
    log.info("Auditor .......: %s", "APROVADO" if not fail else "REPROVADO")
    log.info("=" * 70)

    return df, {"ecr": status_ecr, "banner": status_ban, "uniq": status_uniq, "fail": fail}


if __name__ == "__main__":
    run()
