"""
Resultado da Casa — 25/03/2026 (dia corrente, dados parciais até o momento)

Fonte: ps_bi.fct_player_activity_daily (valores em BRL, já divididos)
Exclui test users via dim_user.is_test = true

Compara: Hoje vs D-1 (24/03) vs D-7 (18/03) vs média últimos 7 dias
"""

import sys
sys.path.insert(0, "c:/Users/NITRO/OneDrive - PGX/MultiBet")

import pandas as pd
from datetime import date
from db.athena import query_athena

pd.set_option("display.float_format", lambda x: f"{x:,.2f}")
pd.set_option("display.max_columns", 30)
pd.set_option("display.width", 200)

# ── Query principal: KPIs por dia (hoje + últimos 7 dias) ──────────────
sql = """
WITH daily AS (
    SELECT
        a.activity_date,
        -- Jogadores ativos (login ou qualquer atividade)
        COUNT(DISTINCT a.player_id)                                           AS players_ativos,
        -- Depósitos (sucesso)
        SUM(a.deposit_success_local)                                          AS depositos,
        COUNT(DISTINCT CASE WHEN a.deposit_success_count > 0 THEN a.player_id END) AS depositantes,
        SUM(a.deposit_success_count)                                          AS qtd_depositos,
        -- Saques (sucesso)
        SUM(a.cashout_success_local)                                          AS saques,
        COUNT(DISTINCT CASE WHEN a.cashout_success_count > 0 THEN a.player_id END) AS sacadores,
        SUM(a.cashout_success_count)                                          AS qtd_saques,
        -- Net Deposit
        SUM(a.deposit_success_local) - SUM(a.cashout_success_local)           AS net_deposit,
        -- Casino
        SUM(a.casino_bet_amount_local)                                        AS casino_bet,
        SUM(a.casino_win_amount_local)                                        AS casino_win,
        SUM(a.casino_realbet_local)                                           AS casino_realbet,
        SUM(a.casino_real_win_local)                                          AS casino_real_win,
        SUM(a.casino_bonusbet_local)                                          AS casino_bonusbet,
        -- Sportsbook
        SUM(a.sb_bet_amount_local)                                            AS sb_bet,
        SUM(a.sb_win_amount_local)                                            AS sb_win,
        SUM(a.sb_realbet_local)                                               AS sb_realbet,
        SUM(a.sb_real_win_local)                                              AS sb_real_win,
        -- GGR / NGR
        SUM(a.ggr_local)                                                      AS ggr_total,
        SUM(a.ngr_local)                                                      AS ngr,
        -- Bonus
        SUM(a.bonus_issued_local)                                             AS bonus_issued,
        SUM(a.bonus_granted_local)                                            AS bonus_granted,
        SUM(a.bonus_turnedreal_local)                                         AS bonus_turnedreal,
        -- FTD / NRC / Logins
        SUM(a.ftd_count)                                                      AS ftd_count,
        SUM(a.nrc_count)                                                      AS nrc_count,
        SUM(a.login_count)                                                    AS login_count,
        -- Jackpot
        SUM(a.jackpot_win_amount_local)                                       AS jackpot_win,
        SUM(a.jackpot_contribution_local)                                     AS jackpot_contribution
    FROM ps_bi.fct_player_activity_daily a
    JOIN ps_bi.dim_user u ON a.player_id = u.ecr_id
    WHERE a.activity_date BETWEEN DATE '2026-03-18' AND DATE '2026-03-25'
      AND u.is_test = false
    GROUP BY a.activity_date
)
SELECT * FROM daily
ORDER BY activity_date DESC
"""

print("=" * 80)
print("  RESULTADO DA CASA - MultiBet - 25/03/2026")
print("=" * 80)
print("\nConsultando ps_bi.fct_player_activity_daily...")

df = query_athena(sql, database="ps_bi")
print(f"\n>>> {len(df)} dias retornados.\n")

# ── Destaque de hoje vs comparativos ───────────────────────────────────
hoje = df[df["activity_date"] == date(2026, 3, 25)]
d1   = df[df["activity_date"] == date(2026, 3, 24)]
d7   = df[df["activity_date"] == date(2026, 3, 18)]
media7 = df[df["activity_date"].apply(lambda x: x < date(2026, 3, 25))]

if hoje.empty:
    print("*** SEM DADOS PARA HOJE (25/03) - possivel atraso no pipeline ps_bi.")
    print("    Ultimo dia disponivel:")
    if not df.empty:
        ultimo = df.iloc[0]
        print(f"    {ultimo['activity_date'].strftime('%d/%m/%Y')}: GGR R$ {ultimo['ggr_total']:,.2f}")
    print("\n    Vou tentar buscar dados parciais via bireports_ec2...\n")
else:
    h = hoje.iloc[0]

    # ── Casino GGR breakdown: real vs bonus ──
    casino_ggr_real = h['casino_realbet'] - h['casino_real_win']
    casino_ggr_bonus = h['casino_bonusbet'] - (h['casino_win_amount_local'] if 'casino_win_amount_local' in h else h['casino_win'] - h['casino_real_win'])

    print("=" * 80)
    print(f"  DESTAQUE - 25/03/2026 (dados parciais, dia em andamento)")
    print("=" * 80)

    print(f"""
  ===== RECEITA =====
  GGR Total .......... R$ {h['ggr_total']:>14,.2f}
  NGR (Net Gaming Rev) R$ {h['ngr']:>14,.2f}

  ===== GGR POR PRODUTO =====
  Casino (total) ..... R$ {h['casino_bet'] - h['casino_win']:>14,.2f}
    Real Cash ........ R$ {casino_ggr_real:>14,.2f}
  Sportsbook ......... R$ {h['sb_bet'] - h['sb_win']:>14,.2f}
    Real Cash ........ R$ {h['sb_realbet'] - h['sb_real_win']:>14,.2f}

  ===== FINANCEIRO =====
  Depositos .......... R$ {h['depositos']:>14,.2f}  ({int(h['depositantes'])} jogadores, {int(h['qtd_depositos'])} txns)
  Saques ............. R$ {h['saques']:>14,.2f}  ({int(h['sacadores'])} jogadores, {int(h['qtd_saques'])} txns)
  Net Deposit ........ R$ {h['net_deposit']:>14,.2f}

  ===== VOLUME DE JOGO =====
  Casino Bet ......... R$ {h['casino_bet']:>14,.2f}
  Casino Win ......... R$ {h['casino_win']:>14,.2f}
  Sportsbook Bet ..... R$ {h['sb_bet']:>14,.2f}
  Sportsbook Win ..... R$ {h['sb_win']:>14,.2f}

  ===== BONUS =====
  Bonus Emitido ...... R$ {h['bonus_issued']:>14,.2f}
  Bonus Granted ...... R$ {h['bonus_granted']:>14,.2f}
  Bonus Convertido ... R$ {h['bonus_turnedreal']:>14,.2f}

  ===== JACKPOT =====
  Jackpot Win ........ R$ {h['jackpot_win']:>14,.2f}
  Jackpot Contrib .... R$ {h['jackpot_contribution']:>14,.2f}

  ===== ENGAJAMENTO =====
  Players Ativos ..... {int(h['players_ativos']):>10,}
  Logins ............. {int(h['login_count']):>10,}
  FTD (1o deposito) .. {int(h['ftd_count']):>10,}
  NRC (novos) ........ {int(h['nrc_count']):>10,}
""")

    # Hold Rate
    if h['casino_bet'] > 0:
        hold_casino = ((h['casino_bet'] - h['casino_win']) / h['casino_bet']) * 100
        print(f"  Hold Rate Casino: {hold_casino:.2f}%")
    if h['sb_bet'] > 0:
        hold_sb = ((h['sb_bet'] - h['sb_win']) / h['sb_bet']) * 100
        print(f"  Hold Rate Sports: {hold_sb:.2f}%")
    if h['depositos'] > 0:
        dep_to_ggr = (h['ggr_total'] / h['depositos']) * 100
        print(f"  GGR/Depositos:    {dep_to_ggr:.2f}%")

    # ── Comparações ────────────────────────────────────────────────────
    print("\n  ----- COMPARATIVOS -----")
    if not d1.empty:
        d = d1.iloc[0]
        var_ggr = ((h['ggr_total'] / d['ggr_total']) - 1) * 100 if d['ggr_total'] != 0 else 0
        var_dep = ((h['depositos'] / d['depositos']) - 1) * 100 if d['depositos'] != 0 else 0
        var_net = ((h['net_deposit'] / d['net_deposit']) - 1) * 100 if d['net_deposit'] != 0 else 0
        var_pla = ((h['players_ativos'] / d['players_ativos']) - 1) * 100 if d['players_ativos'] != 0 else 0
        print(f"  vs D-1 (24/03):  GGR {var_ggr:+.1f}%  |  Dep {var_dep:+.1f}%  |  Net Dep {var_net:+.1f}%  |  Players {var_pla:+.1f}%")

    if not d7.empty:
        d = d7.iloc[0]
        var_ggr = ((h['ggr_total'] / d['ggr_total']) - 1) * 100 if d['ggr_total'] != 0 else 0
        var_dep = ((h['depositos'] / d['depositos']) - 1) * 100 if d['depositos'] != 0 else 0
        print(f"  vs D-7 (18/03):  GGR {var_ggr:+.1f}%  |  Dep {var_dep:+.1f}%")

    if not media7.empty:
        m = media7.mean(numeric_only=True)
        var_ggr = ((h['ggr_total'] / m['ggr_total']) - 1) * 100 if m['ggr_total'] != 0 else 0
        var_dep = ((h['depositos'] / m['depositos']) - 1) * 100 if m['depositos'] != 0 else 0
        print(f"  vs Media 7d:     GGR {var_ggr:+.1f}%  |  Dep {var_dep:+.1f}%")

# ── Tabela resumo dos 8 dias ───────────────────────────────────────────
print("\n" + "=" * 80)
print("  EVOLUCAO DIARIA (18/03 a 25/03)")
print("=" * 80)

resumo = df[['activity_date', 'ggr_total', 'ngr', 'depositos', 'saques',
             'net_deposit', 'players_ativos', 'ftd_count', 'nrc_count',
             'casino_bet', 'sb_bet', 'bonus_issued']].copy()
resumo['activity_date'] = resumo['activity_date'].apply(lambda x: x.strftime('%d/%m'))
resumo.columns = ['Data', 'GGR', 'NGR', 'Depositos', 'Saques', 'Net Dep',
                   'Players', 'FTD', 'NRC', 'Casino Bet', 'SB Bet', 'Bonus']
print(resumo.to_string(index=False))

print("\n" + "-" * 80)
print("  NOTA: Dados de 25/03 sao PARCIAIS (pipeline ps_bi pode ter delay de horas).")
print("  Fonte: ps_bi.fct_player_activity_daily | Excluidos test users (is_test=false)")
print("  Valores em BRL reais | activity_date = truncamento UTC")
print("-" * 80)