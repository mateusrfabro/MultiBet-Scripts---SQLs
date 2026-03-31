"""
Cria as views multibet.matriz_financeiro_mensal e multibet.matriz_financeiro_semanal
no Super Nova DB, e valida que os totais batem com a view diaria.

Demanda: quebrar a view matriz_financeiro por mes e por semana.
Destino: Super Nova DB (PostgreSQL)
Autor: Mateus F + Claude
Data: 2026-03-24
"""

import sys
sys.path.insert(0, ".")

from db.supernova import execute_supernova


# =============================================================================
# 1. VIEW MENSAL — agrega por mês (dia 01 até último dia do mês)
# =============================================================================
SQL_VIEW_MENSAL = """
CREATE OR REPLACE VIEW multibet.matriz_financeiro_mensal AS
WITH base AS (
    SELECT
        date_trunc('month', a.data)::date AS mes,
        -- Depositos/Saques (soma dos valores e counts brutos)
        SUM(a.dep_amount)                          AS deposit,
        SUM(a.users_count)                          AS dep_users_count,
        SUM(a.dep_count)                            AS dep_count_total,
        SUM(a.withdrawal_amount)                    AS withdrawal,
        SUM(a.net_deposit)                          AS net_deposit,
        -- Users/FTD (soma — cada dia é incremental)
        SUM(b.users)                                AS users,
        SUM(b.ftd)                                  AS ftd,
        SUM(b.ftd_amount)                           AS ftd_amount,
        -- Casino (soma)
        SUM(COALESCE(c.casino_total_bet_amount_inhouse, 0))  AS turnover_cassino,
        SUM(COALESCE(c.casino_total_win_amount_inhouse, 0))  AS win_cassino,
        SUM(COALESCE(c.casino_bonus_bet_amount_inhouse, 0))  AS bonus_cassino,
        -- Sports (soma)
        SUM(COALESCE(d.sportsbook_total_bet, 0))    AS turnover_sports,
        SUM(COALESCE(d.sportsbook_total_win, 0))    AS win_sports,
        SUM(COALESCE(d.sportsbook_bonus_bet, 0))    AS bonus_sportbook,
        -- Ativos (media diaria — nao pode somar, double-conta jogadores)
        round(AVG(COALESCE(e.active_players_betting, 0))::numeric, 0)::integer AS ativos
    FROM multibet.tab_dep_with a
        LEFT JOIN multibet.tab_user_ftd b ON a.data = b.data AND b.data >= '2025-10-01'::date
        LEFT JOIN multibet.tab_cassino c ON a.data = c.data
        LEFT JOIN multibet.tab_sports d ON a.data = d.data
        LEFT JOIN multibet.tab_ativos e ON a.data = e.data
    GROUP BY date_trunc('month', a.data)
), calc AS (
    SELECT
        base.mes,
        base.deposit,
        -- adpu = deposit total / depositantes unicos do periodo
        round((base.deposit / NULLIF(base.dep_users_count, 0))::numeric, 2) AS adpu,
        -- avg_dep = deposit total / numero de depositos
        round((base.deposit / NULLIF(base.dep_count_total, 0))::numeric, 2) AS avg_dep,
        base.withdrawal,
        base.net_deposit,
        base.users,
        base.ftd,
        -- conversion = ftd / registros * 100
        round((base.ftd::numeric / NULLIF(base.users, 0)::numeric * 100), 2) AS conversion,
        base.ftd_amount,
        -- avg_ftd = valor FTD total / quantidade de FTDs
        round((base.ftd_amount / NULLIF(base.ftd, 0))::numeric, 2) AS avg_ftd_amount,
        base.turnover_cassino,
        base.win_cassino,
        base.bonus_cassino,
        base.turnover_sports,
        base.win_sports,
        base.bonus_sportbook,
        base.ativos,
        base.turnover_cassino - base.win_cassino      AS ggr_cassino,
        base.turnover_sports  - base.win_sports        AS ggr_sport
    FROM base
)
SELECT
    calc.mes                                           AS data,
    calc.deposit,
    calc.adpu,
    calc.avg_dep,
    calc.withdrawal,
    calc.net_deposit,
    calc.users,
    calc.ftd,
    calc.conversion,
    calc.ftd_amount,
    calc.avg_ftd_amount,
    calc.turnover_cassino,
    calc.win_cassino,
    calc.ggr_cassino,
    calc.turnover_sports,
    calc.win_sports,
    calc.ggr_sport,
    calc.ggr_cassino + calc.ggr_sport                  AS ggr_total,
    calc.ggr_cassino + calc.ggr_sport
        - (calc.bonus_cassino + calc.bonus_sportbook)  AS ngr,
    calc.bonus_cassino + calc.bonus_sportbook           AS retencao,
    round(((calc.ggr_cassino + calc.ggr_sport
        - (calc.bonus_cassino + calc.bonus_sportbook))
        / NULLIF(calc.ativos, 0)::double precision)::numeric, 2) AS arpu,
    calc.ativos
FROM calc
ORDER BY calc.mes DESC;
"""


# =============================================================================
# 2. VIEW SEMANAL — agrega por semana (domingo a sabado)
#    PostgreSQL date_trunc('week') retorna segunda-feira.
#    Para domingo: data - extract(dow from data)::int
# =============================================================================
SQL_VIEW_SEMANAL = """
CREATE OR REPLACE VIEW multibet.matriz_financeiro_semanal AS
WITH base AS (
    SELECT
        -- Inicio da semana = domingo
        (a.data - extract(dow from a.data)::int)::date AS semana_inicio,
        -- Fim da semana = sabado
        (a.data - extract(dow from a.data)::int + 6)::date AS semana_fim,
        -- Depositos/Saques
        SUM(a.dep_amount)                          AS deposit,
        SUM(a.users_count)                          AS dep_users_count,
        SUM(a.dep_count)                            AS dep_count_total,
        SUM(a.withdrawal_amount)                    AS withdrawal,
        SUM(a.net_deposit)                          AS net_deposit,
        -- Users/FTD
        SUM(b.users)                                AS users,
        SUM(b.ftd)                                  AS ftd,
        SUM(b.ftd_amount)                           AS ftd_amount,
        -- Casino
        SUM(COALESCE(c.casino_total_bet_amount_inhouse, 0))  AS turnover_cassino,
        SUM(COALESCE(c.casino_total_win_amount_inhouse, 0))  AS win_cassino,
        SUM(COALESCE(c.casino_bonus_bet_amount_inhouse, 0))  AS bonus_cassino,
        -- Sports
        SUM(COALESCE(d.sportsbook_total_bet, 0))    AS turnover_sports,
        SUM(COALESCE(d.sportsbook_total_win, 0))    AS win_sports,
        SUM(COALESCE(d.sportsbook_bonus_bet, 0))    AS bonus_sportbook,
        -- Ativos (media diaria da semana)
        round(AVG(COALESCE(e.active_players_betting, 0))::numeric, 0)::integer AS ativos,
        -- Dias na semana (util para identificar semanas parciais)
        COUNT(a.data)                               AS dias_no_periodo
    FROM multibet.tab_dep_with a
        LEFT JOIN multibet.tab_user_ftd b ON a.data = b.data AND b.data >= '2025-10-01'::date
        LEFT JOIN multibet.tab_cassino c ON a.data = c.data
        LEFT JOIN multibet.tab_sports d ON a.data = d.data
        LEFT JOIN multibet.tab_ativos e ON a.data = e.data
    GROUP BY
        (a.data - extract(dow from a.data)::int),
        (a.data - extract(dow from a.data)::int + 6)
), calc AS (
    SELECT
        base.semana_inicio,
        base.semana_fim,
        base.deposit,
        round((base.deposit / NULLIF(base.dep_users_count, 0))::numeric, 2) AS adpu,
        round((base.deposit / NULLIF(base.dep_count_total, 0))::numeric, 2) AS avg_dep,
        base.withdrawal,
        base.net_deposit,
        base.users,
        base.ftd,
        round((base.ftd::numeric / NULLIF(base.users, 0)::numeric * 100), 2) AS conversion,
        base.ftd_amount,
        round((base.ftd_amount / NULLIF(base.ftd, 0))::numeric, 2) AS avg_ftd_amount,
        base.turnover_cassino,
        base.win_cassino,
        base.bonus_cassino,
        base.turnover_sports,
        base.win_sports,
        base.bonus_sportbook,
        base.ativos,
        base.dias_no_periodo,
        base.turnover_cassino - base.win_cassino      AS ggr_cassino,
        base.turnover_sports  - base.win_sports        AS ggr_sport
    FROM base
)
SELECT
    calc.semana_inicio,
    calc.semana_fim,
    calc.dias_no_periodo,
    calc.deposit,
    calc.adpu,
    calc.avg_dep,
    calc.withdrawal,
    calc.net_deposit,
    calc.users,
    calc.ftd,
    calc.conversion,
    calc.ftd_amount,
    calc.avg_ftd_amount,
    calc.turnover_cassino,
    calc.win_cassino,
    calc.ggr_cassino,
    calc.turnover_sports,
    calc.win_sports,
    calc.ggr_sport,
    calc.ggr_cassino + calc.ggr_sport                  AS ggr_total,
    calc.ggr_cassino + calc.ggr_sport
        - (calc.bonus_cassino + calc.bonus_sportbook)  AS ngr,
    calc.bonus_cassino + calc.bonus_sportbook           AS retencao,
    round(((calc.ggr_cassino + calc.ggr_sport
        - (calc.bonus_cassino + calc.bonus_sportbook))
        / NULLIF(calc.ativos, 0)::double precision)::numeric, 2) AS arpu,
    calc.ativos
FROM calc
ORDER BY calc.semana_inicio DESC;
"""


# =============================================================================
# 3. VALIDACAO — soma da view diaria deve bater com mensal e semanal
# =============================================================================
SQL_VALIDACAO_DIARIA = """
SELECT
    'DIARIA (total)' AS fonte,
    round(SUM(deposit)::numeric, 2) AS deposit,
    SUM(ftd) AS ftd,
    round(SUM(turnover_cassino)::numeric, 2) AS turnover_cassino,
    round(SUM(ggr_total)::numeric, 2) AS ggr_total,
    round(SUM(ngr)::numeric, 2) AS ngr,
    round(SUM(net_deposit)::numeric, 2) AS net_deposit
FROM multibet.matriz_financeiro;
"""

SQL_VALIDACAO_MENSAL = """
SELECT
    'MENSAL (soma)' AS fonte,
    round(SUM(deposit)::numeric, 2) AS deposit,
    SUM(ftd) AS ftd,
    round(SUM(turnover_cassino)::numeric, 2) AS turnover_cassino,
    round(SUM(ggr_total)::numeric, 2) AS ggr_total,
    round(SUM(ngr)::numeric, 2) AS ngr,
    round(SUM(net_deposit)::numeric, 2) AS net_deposit
FROM multibet.matriz_financeiro_mensal;
"""

SQL_VALIDACAO_SEMANAL = """
SELECT
    'SEMANAL (soma)' AS fonte,
    round(SUM(deposit)::numeric, 2) AS deposit,
    SUM(ftd) AS ftd,
    round(SUM(turnover_cassino)::numeric, 2) AS turnover_cassino,
    round(SUM(ggr_total)::numeric, 2) AS ggr_total,
    round(SUM(ngr)::numeric, 2) AS ngr,
    round(SUM(net_deposit)::numeric, 2) AS net_deposit
FROM multibet.matriz_financeiro_semanal;
"""

# Amostra mensal (ultimos 3 meses)
SQL_AMOSTRA_MENSAL = """
SELECT data AS mes, deposit, ftd, ggr_total, ngr, net_deposit, ativos, arpu
FROM multibet.matriz_financeiro_mensal
ORDER BY data DESC
LIMIT 4;
"""

# Amostra semanal (ultimas 4 semanas)
SQL_AMOSTRA_SEMANAL = """
SELECT semana_inicio, semana_fim, dias_no_periodo, deposit, ftd, ggr_total, ngr, ativos, arpu
FROM multibet.matriz_financeiro_semanal
ORDER BY semana_inicio DESC
LIMIT 5;
"""


def main():
    print("=" * 70)
    print("CRIANDO VIEWS MATRIZ FINANCEIRO (MENSAL + SEMANAL)")
    print("=" * 70)

    # --- Criar view mensal ---
    print("\n[1/2] Criando multibet.matriz_financeiro_mensal...")
    try:
        execute_supernova(SQL_VIEW_MENSAL)
        print("      VIEW MENSAL criada com sucesso!")
    except Exception as e:
        print(f"      ERRO ao criar view mensal: {e}")
        return

    # --- Criar view semanal ---
    print("[2/2] Criando multibet.matriz_financeiro_semanal...")
    try:
        execute_supernova(SQL_VIEW_SEMANAL)
        print("      VIEW SEMANAL criada com sucesso!")
    except Exception as e:
        print(f"      ERRO ao criar view semanal: {e}")
        return

    # --- Validacao ---
    print("\n" + "=" * 70)
    print("VALIDACAO: Totais devem bater entre diaria, mensal e semanal")
    print("=" * 70)

    headers = ["fonte", "deposit", "ftd", "turnover_cassino", "ggr_total", "ngr", "net_deposit"]
    print(f"\n{'Fonte':<18} {'Deposit':>15} {'FTD':>8} {'Turnover Casino':>18} {'GGR Total':>15} {'NGR':>15} {'Net Deposit':>15}")
    print("-" * 110)

    for sql_val in [SQL_VALIDACAO_DIARIA, SQL_VALIDACAO_MENSAL, SQL_VALIDACAO_SEMANAL]:
        try:
            rows = execute_supernova(sql_val, fetch=True)
            for r in rows:
                print(f"{str(r[0]):<18} {r[1]:>15,.2f} {r[2]:>8,} {r[3]:>18,.2f} {r[4]:>15,.2f} {r[5]:>15,.2f} {r[6]:>15,.2f}")
        except Exception as e:
            print(f"  ERRO na validacao: {e}")

    # --- Amostra mensal ---
    print("\n" + "=" * 70)
    print("AMOSTRA MENSAL (ultimos 4 meses)")
    print("=" * 70)
    print(f"{'Mes':<12} {'Deposit':>14} {'FTD':>7} {'GGR Total':>14} {'NGR':>14} {'Net Dep':>14} {'Ativos':>8} {'ARPU':>8}")
    print("-" * 95)
    try:
        rows = execute_supernova(SQL_AMOSTRA_MENSAL, fetch=True)
        for r in rows:
            print(f"{str(r[0]):<12} {r[1]:>14,.2f} {r[2] or 0:>7,} {r[3]:>14,.2f} {r[4]:>14,.2f} {r[5]:>14,.2f} {r[6] or 0:>8,} {r[7] or 0:>8}")
    except Exception as e:
        print(f"  ERRO: {e}")

    # --- Amostra semanal ---
    print("\n" + "=" * 70)
    print("AMOSTRA SEMANAL (ultimas 5 semanas)")
    print("=" * 70)
    print(f"{'Inicio':<12} {'Fim':<12} {'Dias':>5} {'Deposit':>14} {'FTD':>7} {'GGR Total':>14} {'NGR':>14} {'Ativos':>8} {'ARPU':>8}")
    print("-" * 105)
    try:
        rows = execute_supernova(SQL_AMOSTRA_SEMANAL, fetch=True)
        for r in rows:
            print(f"{str(r[0]):<12} {str(r[1]):<12} {r[2]:>5} {r[3]:>14,.2f} {r[4] or 0:>7,} {r[5]:>14,.2f} {r[6]:>14,.2f} {r[7] or 0:>8,} {r[8] or 0:>8}")
    except Exception as e:
        print(f"  ERRO: {e}")

    print("\n" + "=" * 70)
    print("CONCLUIDO! Views criadas no Super Nova DB:")
    print("  - multibet.matriz_financeiro_mensal")
    print("  - multibet.matriz_financeiro_semanal")
    print("=" * 70)
    print("\nNOTA sobre 'ativos': usa AVG diario (media de ativos/dia no periodo),")
    print("nao SUM, pois somar ativos diarios double-contaria jogadores.")
    print("ARPU = NGR / media diaria de ativos.")


if __name__ == "__main__":
    main()
