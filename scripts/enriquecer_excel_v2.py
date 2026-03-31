"""
Enriquece Excel de desbloqueio com:
- valor_abusado_missoes (quanto 'roubou' em missoes)
- saldo_para_casa (GGR - fraude: compensa desbloquear?)
- decisao_v2 com tiers (PRIORITARIO / CONDICIONAL / AVALIAR / MANTER)
- Novas abas organizadas para Castrin e Mauro

Nao roda queries Athena — usa dados ja extraidos.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import numpy as np

REPORTS = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "reports")
INPUT = os.path.join(REPORTS, "analise_desbloqueio_fraudadores_FINAL.xlsx")
OUTPUT = os.path.join(REPORTS, "analise_desbloqueio_fraudadores_FINAL.xlsx")

df = pd.read_excel(INPUT, sheet_name='Todos')
print(f"Carregado: {len(df)} usuarios")

# --- NOVAS COLUNAS ---

# 1. Valor abusado = total_rollback (volume fraudulento em missoes)
df['valor_abusado_missoes'] = df['total_rollback']

# 2. Saldo para casa = GGR que o jogador gerou - valor que abusou
#    Positivo = jogador trouxe mais do que "roubou"
#    Negativo = jogador custou mais do que trouxe
df['saldo_para_casa'] = df['ggr_total'] - df['valor_abusado_missoes']

# 3. Ratio: GGR / fraude (quanto gera por real abusado)
df['ratio_ggr_fraude'] = np.where(
    df['valor_abusado_missoes'] > 0,
    (df['ggr_total'] / df['valor_abusado_missoes']).round(4),
    0
)

# --- DECISAO V2 (mais rigorosa, com tiers) ---

def nova_decisao(row):
    score = row['score_desbloqueio']
    ggr = row['ggr_total'] if pd.notna(row['ggr_total']) else 0
    net_dep = row['net_deposit'] if pd.notna(row['net_deposit']) else 0
    dias_cad = row['dias_desde_cadastro'] if pd.notna(row['dias_desde_cadastro']) else 0

    # PRIORITARIO: score alto + GGR relevante + net deposit positivo + conta antiga
    if score >= 50 and ggr > 100 and net_dep > 500 and dias_cad > 60:
        return 'DESBLOQUEAR - PRIORITARIO'
    # CONDICIONAL: score bom + pelo menos GGR e net_dep positivos
    elif score >= 30 and ggr > 0 and net_dep > 0:
        return 'DESBLOQUEAR - CONDICIONAL'
    # AVALIAR: score intermediario
    elif score >= 10:
        return 'AVALIAR'
    else:
        return 'MANTER BLOQUEADO'

df['decisao_v2'] = df.apply(nova_decisao, axis=1)

# Resumo
print("\n=== DECISAO ORIGINAL ===")
print(df['decisao'].value_counts().to_string())
print("\n=== DECISAO V2 (mais rigorosa) ===")
print(df['decisao_v2'].value_counts().to_string())

for dec in ['DESBLOQUEAR - PRIORITARIO', 'DESBLOQUEAR - CONDICIONAL', 'AVALIAR', 'MANTER BLOQUEADO']:
    sub = df[df['decisao_v2'] == dec]
    if len(sub) > 0:
        print(f"\n{dec} ({len(sub)} users):")
        print(f"  GGR medio: R$ {sub['ggr_total'].mean():,.0f}")
        print(f"  Valor abusado medio: R$ {sub['valor_abusado_missoes'].mean():,.0f}")
        print(f"  Saldo casa medio: R$ {sub['saldo_para_casa'].mean():,.0f}")
        print(f"  Net deposit medio: R$ {sub['net_deposit'].mean():,.0f}")
        print(f"  Idade conta media: {sub['dias_desde_cadastro'].mean():.0f} dias")

# --- SALVAR EXCEL ---

# Organizar colunas
cols_priority = [
    'user_id', 'ecr_id',
    'decisao_v2', 'score_desbloqueio',
    'valor_abusado_missoes', 'ggr_total', 'saldo_para_casa',
    'classificacao_risco', 'score_risco_norm',
    'dias_desde_cadastro', 'teve_ftd',
    'dias_ativos', 'total_logins', 'dias_desde_ultimo_login',
    'total_depositos', 'qtd_depositos', 'total_saques', 'net_deposit',
    'ngr_total',
    'casino_realbet_total', 'casino_real_win_total',
    'sports_realbet_total', 'sports_real_win_total',
    'games_fraud', 'total_buyin', 'total_rollback', 'total_turnover_real',
    'avg_rb_pct', 'fraud_records',
    'first_fraud', 'last_fraud',
    'games_list', 'affiliates', 'country_code',
    'justificativa',
]
cols_exist = [c for c in cols_priority if c in df.columns]
df_out = df[cols_exist].sort_values('score_desbloqueio', ascending=False)

df_prio = df_out[df_out['decisao_v2'] == 'DESBLOQUEAR - PRIORITARIO']
df_cond = df_out[df_out['decisao_v2'] == 'DESBLOQUEAR - CONDICIONAL']
df_aval = df_out[df_out['decisao_v2'] == 'AVALIAR']
df_manter = df_out[df_out['decisao_v2'] == 'MANTER BLOQUEADO']

with pd.ExcelWriter(OUTPUT, engine='openpyxl') as writer:
    df_out.to_excel(writer, sheet_name='Todos', index=False)

    if len(df_prio) > 0:
        df_prio.to_excel(writer, sheet_name='Prioritario', index=False)
    if len(df_cond) > 0:
        df_cond.to_excel(writer, sheet_name='Condicional', index=False)
    if len(df_aval) > 0:
        df_aval.to_excel(writer, sheet_name='Avaliar', index=False)
    if len(df_manter) > 0:
        df_manter.to_excel(writer, sheet_name='Manter Bloqueado', index=False)

    # Aba Resumo
    def fmt(val, prefix="R$ "):
        return f"{prefix}{val:,.0f}" if pd.notna(val) else "N/A"

    resumo_rows = []
    resumo_rows.append(('Total usuarios', len(df_out), ''))
    resumo_rows.append(('Na matriz risco 20/03', (df_out['classificacao_risco'] != 'Sem classificacao (conta pos-20/03)').sum(), ''))
    resumo_rows.append(('', '', ''))
    resumo_rows.append(('DESBLOQUEAR - PRIORITARIO', len(df_prio), 'Score>=50 + GGR>100 + NetDep>500 + Conta>60d'))
    resumo_rows.append(('DESBLOQUEAR - CONDICIONAL', len(df_cond), 'Score>=30 + GGR>0 + NetDep>0'))
    resumo_rows.append(('AVALIAR', len(df_aval), 'Score 10-29'))
    resumo_rows.append(('MANTER BLOQUEADO', len(df_manter), 'Score < 10'))
    resumo_rows.append(('', '', ''))

    for label, sub in [('PRIORITARIO', df_prio), ('CONDICIONAL', df_cond), ('AVALIAR', df_aval), ('MANTER BLOQUEADO', df_manter)]:
        if len(sub) == 0:
            continue
        resumo_rows.append((f'--- {label} ({len(sub)}) ---', '', ''))
        resumo_rows.append(('GGR medio', fmt(sub['ggr_total'].mean()), 'Receita que o jogador gerou pra casa'))
        resumo_rows.append(('GGR total', fmt(sub['ggr_total'].sum()), ''))
        resumo_rows.append(('Valor abusado medio', fmt(sub['valor_abusado_missoes'].mean()), 'Volume de rollbacks fraudulentos'))
        resumo_rows.append(('Valor abusado total', fmt(sub['valor_abusado_missoes'].sum()), ''))
        resumo_rows.append(('Saldo casa medio', fmt(sub['saldo_para_casa'].mean()), 'GGR - fraude (positivo = compensa)'))
        resumo_rows.append(('Net deposit medio', fmt(sub['net_deposit'].mean()), 'Depositos - Saques'))
        resumo_rows.append(('Idade conta media', f"{sub['dias_desde_cadastro'].mean():.0f} dias", ''))
        resumo_rows.append(('Dias ativos medio', f"{sub['dias_ativos'].mean():.0f}", ''))
        resumo_rows.append(('', '', ''))

    pd.DataFrame(resumo_rows, columns=['Metrica', 'Valor', 'Descricao']).to_excel(
        writer, sheet_name='Resumo', index=False)

    # Aba Tabela de Score
    score_rows = [
        ('Idade da conta', '>90 dias', '+20', 'dim_user.registration_date'),
        ('', '31-90 dias', '+10', ''),
        ('', '<30 dias', '-10', ''),
        ('GGR total', '>R$ 500', '+25', 'fct_daily.ggr_base'),
        ('', 'R$ 101-500', '+15', ''),
        ('', 'R$ 1-100', '+5', ''),
        ('', '<-R$ 500', '-15', ''),
        ('Net deposit', '>R$ 500', '+15', 'fct_daily.deposit-cashout'),
        ('', 'R$ 1-500', '+5', ''),
        ('', '<-R$ 500', '-10', ''),
        ('Dias ativos', '>30 dias', '+15', 'fct_daily (COUNT DISTINCT)'),
        ('', '8-30 dias', '+10', ''),
        ('', '1-7 dias', '+3', ''),
        ('', '0 dias', '-5', ''),
        ('Ultimo login', '<7 dias', '+10', 'dim_user.auth_last_login'),
        ('', '7-30 dias', '+5', ''),
        ('Severidade fraude', '100%RB + 0 turnover', '-10', 'CSV fraudadores'),
        ('', 'Buyin >R$ 100K', '-15', ''),
        ('', 'Buyin R$ 50K-100K', '-10', ''),
        ('', 'Buyin R$ 10K-50K', '-5', ''),
        ('', 'Fraude em 4+ jogos', '-10', ''),
        ('', 'Fraude em 2-3 jogos', '-3', ''),
        ('Risco (matriz 20/03)', 'Muito Bom', '+15', 'matriz_risco_multibet_2003'),
        ('', 'Bom', '+10', ''),
        ('', 'Mediano', '0', ''),
        ('', 'Ruim', '-5', ''),
        ('', 'Muito Ruim', '-15', ''),
        ('', 'Sem classificacao', '0', '67% nao estavam na matriz'),
        ('Teve deposito', 'Sim', '+5', 'fct_daily.deposit_success'),
        ('', 'Nao', '-5', ''),
        ('', '', '', ''),
        ('DECISAO V2', 'Score>=50 + GGR>100 + NetDep>500 + 60d+', 'PRIORITARIO', 'Jogador comprovadamente valioso'),
        ('', 'Score>=30 + GGR>0 + NetDep>0', 'CONDICIONAL', 'Desbloquear com monitoramento'),
        ('', 'Score 10-29', 'AVALIAR', 'Caso a caso'),
        ('', 'Score < 10', 'MANTER BLOQUEADO', 'Sem valor ou fraude grave'),
        ('', '', '', ''),
        ('NOVAS COLUNAS', 'valor_abusado_missoes', '', 'Volume rollback fraudulento (BRL)'),
        ('', 'saldo_para_casa', '', 'GGR - valor abusado (positivo = compensa)'),
    ]
    pd.DataFrame(score_rows, columns=['Criterio', 'Condicao', 'Pontos', 'Fonte']).to_excel(
        writer, sheet_name='Tabela de Score', index=False)

print(f"\nExcel salvo: {OUTPUT}")
