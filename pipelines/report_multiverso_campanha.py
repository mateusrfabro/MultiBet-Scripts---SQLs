"""
Report Campanha Multiverso — D0 a D7 (atualizado 20/03)
====================================================
Fontes:
  - BigQuery (Smartico): funil CRM, participantes, completadores, bônus
  - Athena  (Data Lake): turnover, GGR, depósitos, retenção

Uso:
    python pipelines/report_multiverso_campanha.py
"""

import sys
sys.path.insert(0, ".")

import logging
from datetime import datetime

import pandas as pd

from db.bigquery import query_bigquery
from db.athena import query_athena

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ─── CONFIG ──────────────────────────────────────────────────────────────────
# Campanha iniciou 13/03/2026 às 17h BRT = 20h UTC
START_UTC = "2026-03-13 20:00:00"
# Automation rule IDs das missões Multiverso
RULE_IDS = (
    11547, 11548, 11549, 11550, 11551, 11552,
    11555, 11554, 11553, 11561, 11557, 11558,
    11562, 11563, 11564, 11556, 11559, 11560,
)
# Template IDs dos bônus (Free Spins) por animal/quest
BONUS_TEMPLATES = (
    30614, 30615, 30765,  # Tiger Q1/Q2/Q3
    30363, 30364, 30083,  # Rabbit
    30511, 30512, 30777,  # Ox
    30783, 30784, 30780,  # Snake
    30781, 30785, 30771,  # Dragon
    30787, 30786, 30774,  # Mouse
)
# Game IDs dos títulos da campanha (Fortune Tiger, Rabbit, Ox, Dragon, Mouse, Snake)
GAME_IDS = ("4776", "13097", "8842", "833", "2603", "18949")
# Resource ID do popup CRM
RESOURCE_ID = 164110

RULE_IDS_STR = ",".join(str(r) for r in RULE_IDS)
TEMPLATES_STR = ",".join(str(t) for t in BONUS_TEMPLATES)
GAME_IDS_SQL = ",".join(f"'{g}'" for g in GAME_IDS)


def fmt_brl(valor):
    """Formata valor numérico para R$ brasileiro."""
    if valor is None:
        return "R$ 0,00"
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


# ═════════════════════════════════════════════════════════════════════════════
# 1. BIGQUERY — Funil CRM
# ═════════════════════════════════════════════════════════════════════════════
def get_funil():
    sql = f"""
    SELECT
        fact_type_id,
        CASE fact_type_id
            WHEN 1 THEN 'Enviado'
            WHEN 2 THEN 'Entregue'
            WHEN 3 THEN 'Visualizado'
            WHEN 4 THEN 'Clicou'
            WHEN 5 THEN 'Converteu'
        END AS etapa,
        COUNT(DISTINCT user_id) AS usuarios
    FROM `smartico-bq6.dwh_ext_24105.j_communication`
    WHERE resource_id = {RESOURCE_ID}
      AND fact_date >= '2026-03-13'
    GROUP BY 1, 2
    ORDER BY 1
    """
    return query_bigquery(sql)


# ═════════════════════════════════════════════════════════════════════════════
# 2. BIGQUERY — Participantes (progresso nas automation rules)
# ═════════════════════════════════════════════════════════════════════════════
def get_participantes():
    sql = f"""
    SELECT COUNT(DISTINCT user_id) AS participantes
    FROM `smartico-bq6.dwh_ext_24105.j_automation_rule_progress`
    WHERE automation_rule_id IN ({RULE_IDS_STR})
      AND dt_executed >= TIMESTAMP('2026-03-13 20:00:00')
    """
    return query_bigquery(sql)


def get_participantes_ext_ids():
    """Retorna lista de user_ext_id dos participantes para usar no Athena."""
    sql = f"""
    SELECT DISTINCT user_ext_id
    FROM `smartico-bq6.dwh_ext_24105.j_automation_rule_progress`
    WHERE automation_rule_id IN ({RULE_IDS_STR})
      AND dt_executed >= TIMESTAMP('2026-03-13 20:00:00')
      AND user_ext_id IS NOT NULL
    """
    df = query_bigquery(sql)
    return df["user_ext_id"].tolist()


# ═════════════════════════════════════════════════════════════════════════════
# 3. BIGQUERY — Completadores por animal/quest
# ═════════════════════════════════════════════════════════════════════════════
def get_completadores():
    sql = f"""
    SELECT
        CASE
            WHEN label_bonus_template_id IN (30614,30615,30765) THEN 'Tiger'
            WHEN label_bonus_template_id IN (30363,30364,30083) THEN 'Rabbit'
            WHEN label_bonus_template_id IN (30511,30512,30777) THEN 'Ox'
            WHEN label_bonus_template_id IN (30783,30784,30780) THEN 'Snake'
            WHEN label_bonus_template_id IN (30781,30785,30771) THEN 'Dragon'
            WHEN label_bonus_template_id IN (30787,30786,30774) THEN 'Mouse'
        END AS animal,
        CASE
            WHEN label_bonus_template_id IN (30614,30363,30511,30783,30781,30787) THEN 'Q1 (5 FS)'
            WHEN label_bonus_template_id IN (30615,30364,30512,30784,30785,30786) THEN 'Q2 (15 FS)'
            WHEN label_bonus_template_id IN (30765,30083,30777,30780,30771,30774) THEN 'Q3 (25 FS)'
        END AS quest,
        COUNT(DISTINCT user_id) AS completers,
        COUNT(*) AS entregas
    FROM `smartico-bq6.dwh_ext_24105.j_bonuses`
    WHERE label_bonus_template_id IN ({TEMPLATES_STR})
      AND redeem_date IS NOT NULL
      AND fact_date >= '2026-03-13'
    GROUP BY 1, 2
    ORDER BY 1, 2
    """
    return query_bigquery(sql)


def get_completadores_total():
    sql = f"""
    SELECT
        COUNT(DISTINCT user_id) AS completers_unicos,
        COUNT(*) AS total_entregas,
        SUM(CASE
            WHEN label_bonus_template_id IN (30614,30363,30511,30783,30781,30787) THEN 5
            WHEN label_bonus_template_id IN (30615,30364,30512,30784,30785,30786) THEN 15
            WHEN label_bonus_template_id IN (30765,30083,30777,30780,30771,30774) THEN 25
        END) AS total_fs
    FROM `smartico-bq6.dwh_ext_24105.j_bonuses`
    WHERE label_bonus_template_id IN ({TEMPLATES_STR})
      AND redeem_date IS NOT NULL
      AND fact_date >= '2026-03-13'
    """
    return query_bigquery(sql)


# ═════════════════════════════════════════════════════════════════════════════
# 4. BIGQUERY — Bônus duplicados
# ═════════════════════════════════════════════════════════════════════════════
def get_bonus_duplicados():
    sql = f"""
    SELECT
        user_id,
        user_ext_id,
        label_bonus_template_id,
        entity_id,
        COUNT(*) AS vezes
    FROM `smartico-bq6.dwh_ext_24105.j_bonuses`
    WHERE label_bonus_template_id IN ({TEMPLATES_STR})
      AND redeem_date IS NOT NULL
      AND fact_date >= '2026-03-13'
    GROUP BY 1, 2, 3, 4
    HAVING COUNT(*) > 1
    ORDER BY vezes DESC
    """
    return query_bigquery(sql)


# ═════════════════════════════════════════════════════════════════════════════
# 5. ATHENA — Financeiro (turnover, GGR, depósitos, retenção)
# ═════════════════════════════════════════════════════════════════════════════
def get_financeiro_athena(ext_ids: list):
    """
    Consulta Athena com os ext_ids dos participantes.
    Substitui a query que antes rodava no Redshift.

    Retorna dict com métricas consolidadas.
    """
    if not ext_ids:
        log.warning("Nenhum ext_id recebido - pulando financeiro Athena")
        return {}

    ext_ids_str = ",".join(str(x) for x in ext_ids)

    # ── Métricas de jogo (fund_ec2) ──────────────────────────────────────
    # Apostas, wins, GGR nos jogos da campanha
    sql_gaming = f"""
    WITH participantes AS (
        SELECT DISTINCT c_ecr_id, c_external_id
        FROM ecr_ec2.tbl_ecr
        WHERE c_external_id IN ({ext_ids_str})
    ),
    user_metrics AS (
        SELECT
            f.c_ecr_id,
            -- Apostas (tipo 27) e Wins (tipo 45), valores em centavos
            SUM(CASE WHEN f.c_txn_type = 27 THEN f.c_amount_in_ecr_ccy ELSE 0 END) AS bet_cents,
            SUM(CASE WHEN f.c_txn_type = 45 THEN f.c_amount_in_ecr_ccy ELSE 0 END) AS win_cents,
            COUNT_IF(f.c_txn_type = 27) AS bets_qty
        FROM fund_ec2.tbl_real_fund_txn f
        INNER JOIN participantes p ON f.c_ecr_id = p.c_ecr_id
        WHERE f.c_txn_status = 'SUCCESS'
          AND f.c_game_id IN ({GAME_IDS_SQL})
          AND f.c_start_time >= TIMESTAMP '{START_UTC}'
        GROUP BY f.c_ecr_id
    )
    SELECT
        COUNT(DISTINCT p.c_ecr_id)                        AS total_participantes,
        COUNT(DISTINCT m.c_ecr_id)                         AS com_apostas,
        COALESCE(SUM(m.bet_cents), 0) / 100.0             AS turnover_brl,
        COALESCE(SUM(m.win_cents), 0) / 100.0             AS ganho_brl,
        (COALESCE(SUM(m.bet_cents), 0)
         - COALESCE(SUM(m.win_cents), 0)) / 100.0         AS ggr_brl,
        COALESCE(SUM(m.bets_qty), 0)                       AS total_bets,
        CASE
            WHEN SUM(m.bet_cents) > 0
            THEN ROUND(
                (CAST(SUM(m.bet_cents) - SUM(m.win_cents) AS DOUBLE)
                 / CAST(SUM(m.bet_cents) AS DOUBLE)) * 100, 2)
            ELSE 0.0
        END AS hold_rate_pct
    FROM participantes p
    LEFT JOIN user_metrics m ON p.c_ecr_id = m.c_ecr_id
    """
    log.info("Consultando Athena: metricas de jogo (fund_ec2)...")
    df_gaming = query_athena(sql_gaming, database="fund_ec2")

    # ── Depósitos (cashier_ec2) ──────────────────────────────────────────
    # Cash-in dos participantes, com breakdown por dia (D0 a D5)
    sql_cashier = f"""
    WITH participantes AS (
        SELECT DISTINCT c_ecr_id, c_external_id
        FROM ecr_ec2.tbl_ecr
        WHERE c_external_id IN ({ext_ids_str})
    ),
    user_deposits AS (
        SELECT
            d.c_ecr_id,
            SUM(d.c_confirmed_amount_in_inhouse_ccy) AS dep_cents,
            -- Flags por dia (BRT) — cada dia indica se o user depositou naquele dia
            MAX(CASE WHEN CAST(d.c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '2026-03-13' THEN 1 ELSE 0 END) AS is_d0,
            MAX(CASE WHEN CAST(d.c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '2026-03-14' THEN 1 ELSE 0 END) AS is_d1,
            MAX(CASE WHEN CAST(d.c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '2026-03-15' THEN 1 ELSE 0 END) AS is_d2,
            MAX(CASE WHEN CAST(d.c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '2026-03-16' THEN 1 ELSE 0 END) AS is_d3,
            MAX(CASE WHEN CAST(d.c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '2026-03-17' THEN 1 ELSE 0 END) AS is_d4,
            MAX(CASE WHEN CAST(d.c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '2026-03-18' THEN 1 ELSE 0 END) AS is_d5,
            MAX(CASE WHEN CAST(d.c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '2026-03-19' THEN 1 ELSE 0 END) AS is_d6,
            MAX(CASE WHEN CAST(d.c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '2026-03-20' THEN 1 ELSE 0 END) AS is_d7
        FROM cashier_ec2.tbl_cashier_deposit d
        INNER JOIN participantes p ON d.c_ecr_id = p.c_ecr_id
        WHERE d.c_txn_status = 'txn_confirmed_success'
          AND d.c_created_time >= TIMESTAMP '{START_UTC}'
        GROUP BY d.c_ecr_id
    )
    SELECT
        COALESCE(SUM(dep_cents), 0) / 100.0 AS cashin_brl,
        SUM(is_d0) AS dep_d0,
        SUM(is_d1) AS dep_d1,
        SUM(is_d2) AS dep_d2,
        SUM(is_d3) AS dep_d3,
        SUM(is_d4) AS dep_d4,
        SUM(is_d5) AS dep_d5,
        SUM(is_d6) AS dep_d6,
        SUM(is_d7) AS dep_d7,
        -- Retenção: quem depositou no D0 E também depositou em D1, D2, etc.
        SUM(CASE WHEN is_d0 = 1 AND is_d1 = 1 THEN 1 ELSE 0 END) AS ret_d0d1,
        SUM(CASE WHEN is_d0 = 1 AND is_d2 = 1 THEN 1 ELSE 0 END) AS ret_d0d2,
        SUM(CASE WHEN is_d0 = 1 AND is_d3 = 1 THEN 1 ELSE 0 END) AS ret_d0d3,
        SUM(CASE WHEN is_d0 = 1 AND is_d4 = 1 THEN 1 ELSE 0 END) AS ret_d0d4,
        SUM(CASE WHEN is_d0 = 1 AND is_d5 = 1 THEN 1 ELSE 0 END) AS ret_d0d5,
        SUM(CASE WHEN is_d0 = 1 AND is_d6 = 1 THEN 1 ELSE 0 END) AS ret_d0d6,
        SUM(CASE WHEN is_d0 = 1 AND is_d7 = 1 THEN 1 ELSE 0 END) AS ret_d0d7
    FROM user_deposits
    """
    log.info("Consultando Athena: depositos (cashier_ec2)...")
    df_cashier = query_athena(sql_cashier, database="cashier_ec2")

    # -- BTR / Custo de bonus (bonus_ec2) --
    # BTR isolado por Free Spins (c_freespin_win) = custo real da campanha
    # c_actual_issued_amount inclui bonus de TODAS as campanhas (inflado)
    # c_freespin_win isola apenas o custo de Free Spins resgatados
    # Nota: nao ha cross-reference direto entre template_id do Smartico
    #       e bonus_ec2 (c_bonus_id eh ID interno, c_label_id = 'multibet')
    sql_bonus = f"""
    WITH participantes AS (
        SELECT DISTINCT c_ecr_id
        FROM ecr_ec2.tbl_ecr
        WHERE c_external_id IN ({ext_ids_str})
    )
    SELECT
        COALESCE(SUM(bs.c_freespin_win), 0) / 100.0 AS btr_fs_brl,
        COALESCE(SUM(bs.c_actual_issued_amount), 0) / 100.0 AS btr_total_brl
    FROM bonus_ec2.tbl_bonus_summary_details bs
    INNER JOIN participantes p ON bs.c_ecr_id = p.c_ecr_id
    WHERE bs.c_issue_date >= TIMESTAMP '{START_UTC}'
      AND bs.c_actual_issued_amount > 0
    """
    log.info("Consultando Athena: BTR (bonus_ec2)...")
    df_bonus = query_athena(sql_bonus, database="bonus_ec2")

    # ── Montar resultado ─────────────────────────────────────────────────
    g = df_gaming.iloc[0] if len(df_gaming) > 0 else {}
    c = df_cashier.iloc[0] if len(df_cashier) > 0 else {}
    b = df_bonus.iloc[0] if len(df_bonus) > 0 else {}

    turnover = float(g.get("turnover_brl", 0) or 0)
    ggr = float(g.get("ggr_brl", 0) or 0)
    btr_fs = float(b.get("btr_fs_brl", 0) or 0)       # custo isolado FS
    btr_total = float(b.get("btr_total_brl", 0) or 0)  # custo bruto todos bonus
    ngr = ggr - btr_fs

    return {
        "total_participantes": int(g.get("total_participantes", 0) or 0),
        "com_apostas": int(g.get("com_apostas", 0) or 0),
        "turnover_brl": turnover,
        "ganho_brl": float(g.get("ganho_brl", 0) or 0),
        "ggr_brl": ggr,
        "btr_brl": btr_fs,
        "btr_total_brl": btr_total,
        "ngr_brl": ngr,
        "hold_rate_pct": float(g.get("hold_rate_pct", 0) or 0),
        "total_bets": int(g.get("total_bets", 0) or 0),
        "cashin_brl": float(c.get("cashin_brl", 0) or 0),
        "dep_d0": int(c.get("dep_d0", 0) or 0),
        "dep_d1": int(c.get("dep_d1", 0) or 0),
        "dep_d2": int(c.get("dep_d2", 0) or 0),
        "dep_d3": int(c.get("dep_d3", 0) or 0),
        "dep_d4": int(c.get("dep_d4", 0) or 0),
        "dep_d5": int(c.get("dep_d5", 0) or 0),
        "dep_d6": int(c.get("dep_d6", 0) or 0),
        "dep_d7": int(c.get("dep_d7", 0) or 0),
        "ret_d0d1": int(c.get("ret_d0d1", 0) or 0),
        "ret_d0d2": int(c.get("ret_d0d2", 0) or 0),
        "ret_d0d3": int(c.get("ret_d0d3", 0) or 0),
        "ret_d0d4": int(c.get("ret_d0d4", 0) or 0),
        "ret_d0d5": int(c.get("ret_d0d5", 0) or 0),
        "ret_d0d6": int(c.get("ret_d0d6", 0) or 0),
        "ret_d0d7": int(c.get("ret_d0d7", 0) or 0),
    }


# ═════════════════════════════════════════════════════════════════════════════
# MAIN — Executa tudo e exibe o report
# ═════════════════════════════════════════════════════════════════════════════
def main():
    agora = datetime.now().strftime("%d/%m/%Y %H:%M")
    print("=" * 70)
    print(f"  CAMPANHA MULTIVERSO - REPORT ATUALIZADO ({agora})")
    print(f"  Periodo: 13/03 17h BRT -> hoje")
    print("=" * 70)

    # -- BigQuery --
    log.info("Consultando BigQuery: funil CRM...")
    df_funil = get_funil()
    print("\n-- FUNIL CRM " + "-" * 55)
    print(df_funil.to_string(index=False))

    log.info("Consultando BigQuery: participantes...")
    df_part = get_participantes()
    n_part = int(df_part["participantes"].iloc[0])
    print(f"\n  Participantes (automation rules): {n_part}")

    log.info("Consultando BigQuery: completadores...")
    df_comp = get_completadores()
    print("\n-- COMPLETADORES POR QUEST " + "-" * 42)
    print(df_comp.to_string(index=False))

    df_ct = get_completadores_total()
    completers = int(df_ct["completers_unicos"].iloc[0])
    entregas = int(df_ct["total_entregas"].iloc[0])
    total_fs = int(df_ct["total_fs"].iloc[0])
    print(f"\n  Completers unicos: {completers}")
    print(f"  Entregas totais:   {entregas}")
    print(f"  Free Spins total:  {total_fs}")

    log.info("Consultando BigQuery: bonus duplicados...")
    df_dup = get_bonus_duplicados()
    if len(df_dup) > 0:
        print("\n-- BONUS DUPLICADOS " + "-" * 49)
        print(df_dup.to_string(index=False))
    else:
        print("\n  Bonus duplicados: nenhum encontrado")

    # -- Athena --
    log.info("Buscando ext_ids dos participantes para Athena...")
    ext_ids = get_participantes_ext_ids()
    n_ext = len(ext_ids)
    print(f"\n  ext_ids para consulta financeira: {n_ext}")

    fin = get_financeiro_athena(ext_ids)

    if not fin:
        print("\n  [!] Sem dados financeiros (nenhum ext_id)")
        return

    # -- Report consolidado --
    print("\n" + "=" * 70)
    print("  RESULTADO CONSOLIDADO")
    print("=" * 70)

    # Receita
    print("\n-- RECEITA (jogos da campanha) " + "-" * 38)
    print(f"  Turnover (total apostado):  {fmt_brl(fin['turnover_brl'])}")
    print(f"  Ganho jogadores:            {fmt_brl(fin['ganho_brl'])}")
    print(f"  GGR:                        {fmt_brl(fin['ggr_brl'])}")
    print(f"  Hold Rate:                  {fin['hold_rate_pct']:.2f}%")
    print(f"  BTR Free Spins (campanha):  {fmt_brl(fin['btr_brl'])}")
    print(f"  NGR (GGR - BTR FS):         {fmt_brl(fin['ngr_brl'])}")

    # Engajamento
    alcance = 0
    converteram = 0
    for _, row in df_funil.iterrows():
        if row["fact_type_id"] == 3:  # Visualizado = alcance real
            alcance = int(row["usuarios"])
        if row["fact_type_id"] == 5:  # Converteu
            converteram = int(row["usuarios"])

    print("\n-- ENGAJAMENTO " + "-" * 54)
    print(f"  Alcance real (popup exibido):    {alcance:,}")
    print(f"  Converteram via popup:           {converteram:,}")
    print(f"  Participaram (automation rules): {n_part:,}")
    print(f"  Completaram missao (Free Spins): {completers:,}")
    print(f"  Free Spins entregues:            {total_fs:,}")

    # Depositos
    print("\n-- DEPOSITOS (participantes) " + "-" * 40)
    print(f"  Cash-in total no periodo:  {fmt_brl(fin['cashin_brl'])}")
    print(f"  Depositaram D0 (13/03):    {fin['dep_d0']}")
    print(f"  Depositaram D1 (14/03):    {fin['dep_d1']}")
    print(f"  Depositaram D2 (15/03):    {fin['dep_d2']}")
    print(f"  Depositaram D3 (16/03):    {fin['dep_d3']}")
    print(f"  Depositaram D4 (17/03):    {fin['dep_d4']}")
    print(f"  Depositaram D5 (18/03):    {fin['dep_d5']}")
    print(f"  Depositaram D6 (19/03):    {fin['dep_d6']}")
    print(f"  Depositaram D7 (20/03):    {fin['dep_d7']}  <- parcial")

    # Retencao
    d0 = fin["dep_d0"]
    print("\n-- RETENCAO (base D0) " + "-" * 47)
    if d0 > 0:
        print(f"  D0->D1: {fin['ret_d0d1']}/{d0} = {fin['ret_d0d1']/d0*100:.1f}%")
        print(f"  D0->D2: {fin['ret_d0d2']}/{d0} = {fin['ret_d0d2']/d0*100:.1f}%")
        print(f"  D0->D3: {fin['ret_d0d3']}/{d0} = {fin['ret_d0d3']/d0*100:.1f}%")
        print(f"  D0->D4: {fin['ret_d0d4']}/{d0} = {fin['ret_d0d4']/d0*100:.1f}%")
        print(f"  D0->D5: {fin['ret_d0d5']}/{d0} = {fin['ret_d0d5']/d0*100:.1f}%")
        print(f"  D0->D6: {fin['ret_d0d6']}/{d0} = {fin['ret_d0d6']/d0*100:.1f}%")
        print(f"  D0->D7: {fin['ret_d0d7']}/{d0} = {fin['ret_d0d7']/d0*100:.1f}%  <- parcial")
    else:
        print("  Sem depositos no D0 para calcular retencao")

    # Custo
    print("\n-- CUSTO DA CAMPANHA " + "-" * 48)
    print(f"  BTR Free Spins (custo campanha): {fmt_brl(fin['btr_brl'])}")
    print(f"  BTR Total (todos bonus):         {fmt_brl(fin['btr_total_brl'])}")
    print(f"  Free Spins entregues:            {total_fs}")

    print("\n" + "=" * 70)
    print(f"  Report gerado em {agora} - {n_part} participantes")
    print("=" * 70)


if __name__ == "__main__":
    main()