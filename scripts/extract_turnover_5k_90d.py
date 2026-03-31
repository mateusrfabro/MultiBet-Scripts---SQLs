"""
Extração: Jogadores com turnover acima de R$ 5.000 nos últimos 90 dias.

Fonte: ps_bi.fct_player_activity_daily + ps_bi.dim_user (Athena)
Período: últimos 90 dias até D-1 (evita snapshot parcial)
Valores: BRL (ps_bi já vem dividido, não centavos)
Filtro: is_test = false (exclui test users)

Saída: reports/jogadores_turnover_5k_90d_FINAL.csv + _legenda.txt
"""

import sys
import logging
from datetime import date, timedelta

import pandas as pd

sys.path.insert(0, ".")
from db.athena import query_athena

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# --- Período: D-1 até 90 dias atrás ---
dt_fim = date.today() - timedelta(days=1)      # D-1 (dia completo mais recente)
dt_inicio = dt_fim - timedelta(days=89)         # 90 dias inclusive
log.info(f"Período: {dt_inicio} a {dt_fim} (90 dias)")

# --- Query principal ---
# Turnover = total apostado (casino + sportsbook, real + bônus)
# Breakdown: real vs bônus, casino vs sportsbook, GGR, NGR, dias ativos
sql = f"""
WITH turnover_90d AS (
    SELECT
        f.player_id,

        -- Turnover total (real + bonus, casino + sportsbook)
        COALESCE(SUM(f.casino_realbet_local), 0)
          + COALESCE(SUM(f.casino_bonusbet_local), 0)
          + COALESCE(SUM(f.sb_realbet_local), 0)
          + COALESCE(SUM(f.sb_bonusbet_local), 0)          AS turnover_total,

        -- Real money apenas
        COALESCE(SUM(f.casino_realbet_local), 0)
          + COALESCE(SUM(f.sb_realbet_local), 0)            AS turnover_real,

        -- Bônus apenas
        COALESCE(SUM(f.casino_bonusbet_local), 0)
          + COALESCE(SUM(f.sb_bonusbet_local), 0)           AS turnover_bonus,

        -- Split por produto
        COALESCE(SUM(f.casino_realbet_local), 0)
          + COALESCE(SUM(f.casino_bonusbet_local), 0)       AS turnover_casino,
        COALESCE(SUM(f.sb_realbet_local), 0)
          + COALESCE(SUM(f.sb_bonusbet_local), 0)           AS turnover_sportsbook,

        -- Receita
        COALESCE(SUM(f.ggr_local), 0)                       AS ggr,
        COALESCE(SUM(f.ngr_local), 0)                       AS ngr,

        -- Atividade
        COUNT(DISTINCT f.activity_date)                      AS dias_ativos,
        COALESCE(SUM(f.login_count), 0)                      AS total_logins,
        MIN(f.activity_date)                                 AS primeira_atividade,
        MAX(f.activity_date)                                 AS ultima_atividade

    FROM ps_bi.fct_player_activity_daily f
    WHERE f.activity_date BETWEEN DATE '{dt_inicio}' AND DATE '{dt_fim}'
    GROUP BY f.player_id
)
SELECT
    t.player_id,
    u.external_id,
    ROUND(t.turnover_total, 2)       AS turnover_total_brl,
    ROUND(t.turnover_real, 2)        AS turnover_real_brl,
    ROUND(t.turnover_bonus, 2)       AS turnover_bonus_brl,
    ROUND(t.turnover_casino, 2)      AS turnover_casino_brl,
    ROUND(t.turnover_sportsbook, 2)  AS turnover_sportsbook_brl,
    ROUND(t.ggr, 2)                  AS ggr_brl,
    ROUND(t.ngr, 2)                  AS ngr_brl,
    t.dias_ativos,
    t.total_logins,
    t.primeira_atividade,
    t.ultima_atividade
FROM turnover_90d t
JOIN ps_bi.dim_user u
  ON u.ecr_id = t.player_id
WHERE u.is_test = false
  AND t.turnover_total >= 5000
ORDER BY t.turnover_total DESC
"""

log.info("Executando query no Athena (ps_bi)...")
df = query_athena(sql, database="ps_bi")
log.info(f"Jogadores encontrados: {len(df)}")

# --- Resumo rápido ---
if len(df) > 0:
    print("\n" + "=" * 60)
    print(f"JOGADORES COM TURNOVER >= R$ 5.000 (últimos 90 dias)")
    print(f"Período: {dt_inicio} a {dt_fim}")
    print("=" * 60)
    print(f"Total de jogadores: {len(df):,}")
    print(f"\nTurnover total (soma):  R$ {df['turnover_total_brl'].sum():,.2f}")
    print(f"Turnover médio:         R$ {df['turnover_total_brl'].mean():,.2f}")
    print(f"Turnover mediano:       R$ {df['turnover_total_brl'].median():,.2f}")
    print(f"GGR total:              R$ {df['ggr_brl'].sum():,.2f}")
    print(f"NGR total:              R$ {df['ngr_brl'].sum():,.2f}")
    print(f"\nTop 10 por turnover:")
    print(df.head(10)[['player_id', 'external_id', 'turnover_total_brl',
                        'turnover_real_brl', 'ggr_brl', 'dias_ativos']].to_string(index=False))

    # --- Distribuição por faixa ---
    bins = [5000, 10000, 25000, 50000, 100000, 500000, float('inf')]
    labels = ['5k-10k', '10k-25k', '25k-50k', '50k-100k', '100k-500k', '500k+']
    df['faixa_turnover'] = pd.cut(df['turnover_total_brl'], bins=bins, labels=labels, right=False)
    print(f"\nDistribuição por faixa de turnover:")
    dist = df.groupby('faixa_turnover', observed=True).agg(
        jogadores=('player_id', 'count'),
        turnover_total=('turnover_total_brl', 'sum'),
        ggr_total=('ggr_brl', 'sum')
    ).reset_index()
    dist['turnover_total'] = dist['turnover_total'].apply(lambda x: f"R$ {x:,.2f}")
    dist['ggr_total'] = dist['ggr_total'].apply(lambda x: f"R$ {x:,.2f}")
    print(dist.to_string(index=False))

    # --- Remove coluna auxiliar antes de exportar ---
    df.drop(columns=['faixa_turnover'], inplace=True)

# --- Export CSV ---
output_csv = "reports/jogadores_turnover_5k_90d_FINAL.csv"
df.to_csv(output_csv, index=False, encoding="utf-8-sig")
log.info(f"CSV salvo: {output_csv}")

# --- Legenda ---
legenda = f"""LEGENDA — jogadores_turnover_5k_90d_FINAL.csv
{'=' * 60}
Gerado em: {date.today()}
Período: {dt_inicio} a {dt_fim} (90 dias, até D-1)
Fonte: ps_bi.fct_player_activity_daily + ps_bi.dim_user (Athena)
Filtro: turnover_total >= R$ 5.000 | is_test = false

DICIONÁRIO DE COLUNAS
---------------------
player_id              — ID interno do jogador (ecr_id, 18 dígitos)
external_id            — ID externo (usado no Smartico CRM como user_ext_id)
turnover_total_brl     — Total apostado no período (casino + sportsbook, real + bônus), em R$
turnover_real_brl      — Apostas com dinheiro real (sem bônus), em R$
turnover_bonus_brl     — Apostas com saldo de bônus, em R$
turnover_casino_brl    — Apostas em casino (slots + live), em R$
turnover_sportsbook_brl — Apostas em sportsbook, em R$
ggr_brl                — Gross Gaming Revenue = apostas - ganhos do jogador, em R$
ngr_brl                — Net Gaming Revenue = GGR - custos de bônus, em R$
dias_ativos            — Quantidade de dias distintos com atividade no período
total_logins           — Total de logins no período
primeira_atividade     — Data da primeira atividade registrada no período
ultima_atividade       — Data da última atividade registrada no período

GLOSSÁRIO
---------
Turnover    = volume total apostado (bet amount)
GGR         = receita bruta da casa (apostas - ganhos do jogador)
NGR         = receita líquida (GGR - custos de bônus e promoções)
D-1         = dia anterior completo (evita dados parciais do dia corrente)
Real money  = apostas com depósito real do jogador
Bônus       = apostas feitas com saldo promocional

NOTAS
-----
- Valores em BRL (reais), já convertidos (ps_bi não usa centavos)
- Test users excluídos (is_test = false no dim_user)
- Timestamps da tabela ps_bi são UTC; activity_date é truncamento por dia
- Ordenado por turnover_total decrescente
"""

output_legenda = "reports/jogadores_turnover_5k_90d_FINAL_legenda.txt"
with open(output_legenda, "w", encoding="utf-8") as f:
    f.write(legenda)
log.info(f"Legenda salva: {output_legenda}")

print(f"\nArquivos gerados:")
print(f"  1. {output_csv}")
print(f"  2. {output_legenda}")
