"""
Analise de desbloqueio de usuarios flagged por fraude em missoes.
Cruza lista de fraudadores com matriz de risco e dados Athena (cadastro, atividade, GGR/NGR).
Gera relatorio final com recomendacoes de desbloqueio.

Entradas:
- fraudadores_missoes_completo_com_external_id.csv (Downloads)
- matriz_risco_multibet_2003.csv (Downloads)
- Athena: ps_bi.dim_user, ps_bi.fct_player_activity_daily

Saida:
- reports/analise_desbloqueio_fraudadores_FINAL.xlsx
- reports/analise_desbloqueio_fraudadores_FINAL_legenda.txt
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from db.athena import query_athena

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

DOWNLOADS = "C:/Users/NITRO/Downloads"
REPORTS = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "reports")
os.makedirs(REPORTS, exist_ok=True)

TODAY = datetime(2026, 3, 30)


def load_fraud_list():
    """Carrega e agrega lista de fraudadores por usuario unico."""
    df = pd.read_csv(f"{DOWNLOADS}/fraudadores_missoes_completo_com_external_id.csv")
    log.info(f"Fraud list loaded: {len(df)} records, {df['user_id'].nunique()} unique users")

    agg = df.groupby('user_id').agg(
        ecr_id=('ecr_id', 'first'),
        games_fraud=('game_id', 'nunique'),
        games_list=('game_id', lambda x: ', '.join(x.unique())),
        total_buyin=('buyin_reais', 'sum'),
        total_rollback=('rollback_reais', 'sum'),
        total_turnover_real=('turnover_real_reais', 'sum'),
        total_bets=('bets', 'sum'),
        total_rollbacks=('rollbacks', 'sum'),
        avg_rb_pct=('rb_pct', 'mean'),
        first_fraud=('first_play', 'min'),
        last_fraud=('last_play', 'max'),
        fraud_records=('game_id', 'count'),
        affiliates=('affiliate_id', lambda x: ', '.join([str(a) for a in x.unique() if pd.notna(a) and str(a) != '']))
    ).reset_index()

    agg['first_fraud'] = pd.to_datetime(agg['first_fraud'])
    agg['last_fraud'] = pd.to_datetime(agg['last_fraud'])

    return agg


def load_risk_matrix():
    """Carrega matriz de risco de 20/03/2026."""
    df = pd.read_csv(f"{DOWNLOADS}/matriz_risco_multibet_2003.csv")
    log.info(f"Risk matrix loaded: {len(df)} entries")
    return df


def query_user_profiles(external_ids, batch_size=400):
    """Consulta ps_bi.dim_user para dados de cadastro dos usuarios."""
    all_results = []
    ext_ids_str = [str(x) for x in external_ids]

    for i in range(0, len(ext_ids_str), batch_size):
        batch = ext_ids_str[i:i+batch_size]
        ids_clause = ", ".join(batch)  # sem aspas, external_id eh bigint
        batch_num = i // batch_size + 1
        total_batches = (len(ext_ids_str) + batch_size - 1) // batch_size
        log.info(f"Querying dim_user batch {batch_num}/{total_batches} ({len(batch)} users)...")

        sql = f"""
        SELECT
            CAST(external_id AS VARCHAR) AS external_id,
            ecr_id,
            registration_date,
            ftd_date,
            ftd_datetime,
            country_code,
            is_test,
            CAST(affiliate_id AS VARCHAR) AS dim_affiliate_id,
            auth_last_login_time
        FROM ps_bi.dim_user
        WHERE external_id IN ({ids_clause})
          AND is_test = false
        """
        try:
            df = query_athena(sql, database="ps_bi")
            all_results.append(df)
            log.info(f"  -> {len(df)} users found")
        except Exception as e:
            log.error(f"  -> Error in batch {batch_num}: {e}")

    if all_results:
        return pd.concat(all_results, ignore_index=True)
    return pd.DataFrame()


def query_financial_summary(external_ids, batch_size=400):
    """Consulta ps_bi.fct_player_activity_daily para GGR/NGR/LTV agregado."""
    all_results = []
    ext_ids_str = [str(x) for x in external_ids]

    for i in range(0, len(ext_ids_str), batch_size):
        batch = ext_ids_str[i:i+batch_size]
        ids_clause = ", ".join(batch)  # sem aspas, external_id eh bigint
        batch_num = i // batch_size + 1
        total_batches = (len(ext_ids_str) + batch_size - 1) // batch_size
        log.info(f"Querying financial summary batch {batch_num}/{total_batches}...")

        sql = f"""
        SELECT
            CAST(u.external_id AS VARCHAR) AS external_id,
            -- Atividade
            COUNT(DISTINCT f.activity_date) AS dias_ativos,
            MIN(f.activity_date) AS primeiro_dia_ativo,
            MAX(f.activity_date) AS ultimo_dia_ativo,
            -- Depositos (success only)
            COALESCE(SUM(f.deposit_success_base), 0) AS total_depositos,
            COALESCE(SUM(f.deposit_success_count), 0) AS qtd_depositos,
            -- Saques (success only)
            COALESCE(SUM(f.cashout_success_base), 0) AS total_saques,
            COALESCE(SUM(f.cashout_success_count), 0) AS qtd_saques,
            -- GGR e NGR totais (ja em BRL, inclui casino+sports)
            COALESCE(SUM(f.ggr_base), 0) AS ggr_total,
            COALESCE(SUM(f.ngr_base), 0) AS ngr_total,
            -- Casino real bets/wins
            COALESCE(SUM(f.casino_realbet_base), 0) AS casino_realbet_total,
            COALESCE(SUM(f.casino_real_win_base), 0) AS casino_real_win_total,
            -- Sports real bets/wins
            COALESCE(SUM(f.sb_realbet_base), 0) AS sports_realbet_total,
            COALESCE(SUM(f.sb_real_win_base), 0) AS sports_real_win_total,
            -- Net deposit
            COALESCE(SUM(f.deposit_success_base), 0) - COALESCE(SUM(f.cashout_success_base), 0) AS net_deposit,
            -- Logins
            COALESCE(SUM(f.login_count), 0) AS total_logins
        FROM ps_bi.fct_player_activity_daily f
        JOIN ps_bi.dim_user u ON u.ecr_id = f.player_id AND u.is_test = false
        WHERE u.external_id IN ({ids_clause})
        GROUP BY CAST(u.external_id AS VARCHAR)
        """
        try:
            df = query_athena(sql, database="ps_bi")
            all_results.append(df)
            log.info(f"  -> {len(df)} users with financial data")
        except Exception as e:
            log.error(f"  -> Error in batch {batch_num}: {e}")

    if all_results:
        return pd.concat(all_results, ignore_index=True)
    return pd.DataFrame()


def query_last_login(external_ids, batch_size=400):
    """Consulta bireports_ec2.tbl_ecr para ultimo login."""
    all_results = []
    ext_ids_str = [str(x) for x in external_ids]

    for i in range(0, len(ext_ids_str), batch_size):
        batch = ext_ids_str[i:i+batch_size]
        ids_clause = ", ".join(batch)  # sem aspas, c_external_id eh bigint
        batch_num = i // batch_size + 1
        total_batches = (len(ext_ids_str) + batch_size - 1) // batch_size
        log.info(f"Querying last login batch {batch_num}/{total_batches}...")

        sql = f"""
        SELECT
            CAST(c_external_id AS VARCHAR) AS external_id,
            c_last_login_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS last_login_brt,
            c_signup_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS signup_brt
        FROM bireports_ec2.tbl_ecr
        WHERE c_external_id IN ({ids_clause})
          AND c_test_user = false
        """
        try:
            df = query_athena(sql, database="bireports_ec2")
            all_results.append(df)
            log.info(f"  -> {len(df)} users found")
        except Exception as e:
            log.error(f"  -> Error in batch {batch_num}: {e}")

    if all_results:
        return pd.concat(all_results, ignore_index=True)
    return pd.DataFrame()


def classify_unblock_recommendation(row):
    """
    Classifica cada usuario em: DESBLOQUEAR, AVALIAR, MANTER BLOQUEADO.

    Criterios para DESBLOQUEAR:
    - Cadastro antigo (>30 dias)
    - GGR positivo para a casa (jogador perdeu mais do que ganhou)
    - Net deposit positivo (depositou mais do que sacou)
    - Ativo recentemente
    - Fraude de baixa severidade (poucos jogos, baixo volume)

    Criterios para MANTER BLOQUEADO:
    - Conta nova (criada so pra fraudar missoes)
    - GGR negativo (casa perde dinheiro)
    - Fraude em multiplos jogos e alto volume
    - Nunca depositou
    - Classificacao de risco "Muito Ruim"
    """
    score = 0
    reasons = []

    # 1. Idade da conta (max 20 pts)
    if pd.notna(row.get('dias_desde_cadastro')):
        dias = row['dias_desde_cadastro']
        if dias > 90:
            score += 20
            reasons.append(f"Conta antiga ({dias:.0f}d)")
        elif dias > 30:
            score += 10
            reasons.append(f"Conta com {dias:.0f}d")
        else:
            score -= 10
            reasons.append(f"Conta nova ({dias:.0f}d)")
    else:
        score -= 5
        reasons.append("Sem data cadastro")

    # 2. GGR positivo para a casa (max 25 pts)
    total_ggr = row.get('ggr_total', 0) or 0
    if total_ggr > 500:
        score += 25
        reasons.append(f"GGR positivo R$ {total_ggr:,.0f}")
    elif total_ggr > 100:
        score += 15
        reasons.append(f"GGR moderado R$ {total_ggr:,.0f}")
    elif total_ggr > 0:
        score += 5
        reasons.append(f"GGR leve R$ {total_ggr:,.0f}")
    elif total_ggr < -500:
        score -= 15
        reasons.append(f"GGR negativo R$ {total_ggr:,.0f}")
    else:
        reasons.append(f"GGR ~zero R$ {total_ggr:,.0f}")

    # 3. Net deposit positivo (max 15 pts)
    net_dep = row.get('net_deposit', 0) or 0
    if net_dep > 500:
        score += 15
        reasons.append(f"Net deposit R$ {net_dep:,.0f}")
    elif net_dep > 0:
        score += 5
        reasons.append(f"Net deposit leve R$ {net_dep:,.0f}")
    elif net_dep < -500:
        score -= 10
        reasons.append(f"Net deposit negativo R$ {net_dep:,.0f}")

    # 4. Atividade (max 15 pts)
    dias_ativos = row.get('dias_ativos', 0) or 0
    if dias_ativos > 30:
        score += 15
        reasons.append(f"Muito ativo ({dias_ativos} dias)")
    elif dias_ativos > 7:
        score += 10
        reasons.append(f"Ativo ({dias_ativos} dias)")
    elif dias_ativos > 0:
        score += 3
        reasons.append(f"Pouca atividade ({dias_ativos} dias)")
    else:
        score -= 5
        reasons.append("Sem atividade registrada")

    # 5. Recencia - ultimo login (max 10 pts)
    if pd.notna(row.get('dias_desde_ultimo_login')):
        dias_login = row['dias_desde_ultimo_login']
        if dias_login <= 7:
            score += 10
            reasons.append(f"Login recente ({dias_login:.0f}d atras)")
        elif dias_login <= 30:
            score += 5
            reasons.append(f"Login ha {dias_login:.0f}d")
        else:
            reasons.append(f"Inativo ha {dias_login:.0f}d")

    # 6. Severidade da fraude (penalizacao, max -25 pts)
    rb_pct = row.get('avg_rb_pct', 100) or 100
    buyin = row.get('total_buyin', 0) or 0
    games_fraud = row.get('games_fraud', 1) or 1
    turnover_real = row.get('total_turnover_real', 0) or 0

    if rb_pct >= 100 and turnover_real == 0:
        score -= 10
        reasons.append("Fraude pura (100% RB, zero turnover)")
    elif rb_pct >= 99.5:
        score -= 5
        reasons.append(f"RB muito alto ({rb_pct:.1f}%)")

    if buyin > 100000:
        score -= 15
        reasons.append(f"Alto volume fraude R$ {buyin:,.0f}")
    elif buyin > 50000:
        score -= 10
        reasons.append(f"Volume fraude medio R$ {buyin:,.0f}")
    elif buyin > 10000:
        score -= 5
        reasons.append(f"Volume fraude R$ {buyin:,.0f}")

    if games_fraud >= 4:
        score -= 10
        reasons.append(f"Fraude em {games_fraud} jogos")
    elif games_fraud >= 2:
        score -= 3
        reasons.append(f"Fraude em {games_fraud} jogos")

    # 7. Classificacao na matriz de risco (max 15/-15 pts)
    risco = row.get('classificacao_risco', '')
    if risco == 'Muito Bom':
        score += 15
        reasons.append("Risco: Muito Bom")
    elif risco == 'Bom':
        score += 10
        reasons.append("Risco: Bom")
    elif risco == 'Mediano':
        score += 0
        reasons.append("Risco: Mediano")
    elif risco == 'Ruim':
        score -= 5
        reasons.append("Risco: Ruim")
    elif risco == 'Muito Ruim':
        score -= 15
        reasons.append("Risco: Muito Ruim")

    # 8. Teve FTD? (depositou alguma vez)
    total_dep = row.get('total_depositos', 0) or 0
    if total_dep > 0:
        score += 5
        reasons.append(f"Depositou R$ {total_dep:,.0f}")
    else:
        score -= 5
        reasons.append("Nunca depositou")

    # Decisao final
    if score >= 30:
        decision = "DESBLOQUEAR"
    elif score >= 10:
        decision = "AVALIAR"
    else:
        decision = "MANTER BLOQUEADO"

    return pd.Series({
        'score_desbloqueio': score,
        'decisao': decision,
        'justificativa': ' | '.join(reasons)
    })


def main():
    log.info("=" * 60)
    log.info("ANALISE DE DESBLOQUEIO DE FRAUDADORES DE MISSOES")
    log.info("=" * 60)

    # 1. Carregar dados base
    df_fraud = load_fraud_list()
    df_risk = load_risk_matrix()

    # 2. Cruzar com matriz de risco
    df_fraud['user_id_str'] = df_fraud['user_id'].astype(str)
    df_risk['user_ext_id_str'] = df_risk['user_ext_id'].astype(str)

    df_base = df_fraud.merge(
        df_risk[['user_ext_id_str', 'score_bruto', 'score_norm', 'classificacao']].drop_duplicates('user_ext_id_str'),
        left_on='user_id_str',
        right_on='user_ext_id_str',
        how='left'
    )
    df_base.rename(columns={
        'score_bruto': 'score_risco_bruto',
        'score_norm': 'score_risco_norm',
        'classificacao': 'classificacao_risco'
    }, inplace=True)

    found_risk = df_base['classificacao_risco'].notna().sum()
    log.info(f"Fraud users found in risk matrix: {found_risk}/{len(df_base)}")

    # 3. Consultar Athena - perfis de usuario
    external_ids = df_base['user_id'].unique().tolist()
    log.info(f"\nQuerying Athena for {len(external_ids)} users...")

    df_profiles = query_user_profiles(external_ids)
    log.info(f"Profiles found: {len(df_profiles)}")

    # 4. Consultar Athena - dados financeiros
    df_financial = query_financial_summary(external_ids)
    log.info(f"Financial data found: {len(df_financial)}")

    # 5. Ultimo login ja vem do dim_user (auth_last_login_time)

    # 6. Montar dataset consolidado
    if len(df_profiles) > 0:
        df_profiles['external_id'] = df_profiles['external_id'].astype(str)
        df_base = df_base.merge(
            df_profiles[['external_id', 'registration_date', 'ftd_date', 'country_code', 'auth_last_login_time']],
            left_on='user_id_str',
            right_on='external_id',
            how='left'
        )

    if len(df_financial) > 0:
        df_financial['external_id'] = df_financial['external_id'].astype(str)
        df_base = df_base.merge(
            df_financial,
            left_on='user_id_str',
            right_on='external_id',
            how='left',
            suffixes=('', '_fin')
        )

    # 7. Calcular metricas derivadas
    if 'registration_date' in df_base.columns:
        df_base['registration_date'] = pd.to_datetime(df_base['registration_date'], errors='coerce')
        df_base['dias_desde_cadastro'] = (TODAY - df_base['registration_date']).dt.days

    if 'auth_last_login_time' in df_base.columns:
        df_base['auth_last_login_time'] = pd.to_datetime(df_base['auth_last_login_time'], errors='coerce', utc=True).dt.tz_localize(None)
        df_base['dias_desde_ultimo_login'] = (TODAY - df_base['auth_last_login_time']).dt.days

    if 'ftd_date' in df_base.columns:
        df_base['ftd_date'] = pd.to_datetime(df_base['ftd_date'], errors='coerce')
        df_base['dias_desde_ftd'] = (TODAY - df_base['ftd_date']).dt.days
        df_base['teve_ftd'] = df_base['ftd_date'].notna()

    # 8. Aplicar scoring de desbloqueio
    log.info("Applying unblock scoring...")
    scoring = df_base.apply(classify_unblock_recommendation, axis=1)
    df_base = pd.concat([df_base, scoring], axis=1)

    # 9. Resumo
    log.info("\n" + "=" * 60)
    log.info("RESUMO DA ANALISE")
    log.info("=" * 60)
    log.info(f"Total usuarios analisados: {len(df_base)}")
    log.info(f"\nDecisao:")
    for decision, count in df_base['decisao'].value_counts().items():
        log.info(f"  {decision}: {count}")

    desbloquear = df_base[df_base['decisao'] == 'DESBLOQUEAR']
    avaliar = df_base[df_base['decisao'] == 'AVALIAR']
    manter = df_base[df_base['decisao'] == 'MANTER BLOQUEADO']

    if len(desbloquear) > 0:
        log.info(f"\nDESBLOQUEAR ({len(desbloquear)} users):")
        log.info(f"  GGR medio: R$ {desbloquear['ggr_total'].mean():,.0f}" if 'ggr_total' in desbloquear.columns else "  GGR: N/A")
        log.info(f"  Net deposit medio: R$ {desbloquear['net_deposit'].mean():,.0f}" if 'net_deposit' in desbloquear.columns else "  Net deposit: N/A")
        if 'dias_desde_cadastro' in desbloquear.columns:
            log.info(f"  Idade media conta: {desbloquear['dias_desde_cadastro'].mean():,.0f} dias")

    # 10. Selecionar e ordenar colunas para entrega
    cols_entrega = [
        'user_id', 'ecr_id',
        'decisao', 'score_desbloqueio', 'justificativa',
        'classificacao_risco', 'score_risco_bruto', 'score_risco_norm',
        'dias_desde_cadastro', 'teve_ftd', 'dias_desde_ftd',
        'dias_ativos', 'total_logins', 'dias_desde_ultimo_login',
        'total_depositos', 'qtd_depositos', 'total_saques', 'net_deposit',
        'ggr_total', 'ngr_total',
        'casino_realbet_total', 'casino_real_win_total',
        'sports_realbet_total', 'sports_real_win_total',
        'games_fraud', 'total_buyin', 'total_rollback', 'total_turnover_real',
        'avg_rb_pct', 'fraud_records',
        'first_fraud', 'last_fraud',
        'games_list', 'affiliates', 'country_code'
    ]
    # Filtrar colunas que existem
    cols_final = [c for c in cols_entrega if c in df_base.columns]
    df_final = df_base[cols_final].sort_values('score_desbloqueio', ascending=False)

    # Preencher NaN de risco com texto explicativo (para Castrin/Mauro entenderem)
    df_final['classificacao_risco'] = df_final['classificacao_risco'].fillna('Sem classificacao (conta pos-20/03)')
    df_final['score_risco_bruto'] = df_final['score_risco_bruto'].fillna(0)
    df_final['score_risco_norm'] = df_final['score_risco_norm'].fillna(0)

    # 11. Salvar Excel com abas
    output_path = os.path.join(REPORTS, "analise_desbloqueio_fraudadores_FINAL.xlsx")

    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        # Aba: Todos
        df_final.to_excel(writer, sheet_name='Todos', index=False)

        # Aba: Desbloquear
        df_desbloquear = df_final[df_final['decisao'] == 'DESBLOQUEAR']
        if len(df_desbloquear) > 0:
            df_desbloquear.to_excel(writer, sheet_name='Desbloquear', index=False)

        # Aba: Avaliar
        df_avaliar = df_final[df_final['decisao'] == 'AVALIAR']
        if len(df_avaliar) > 0:
            df_avaliar.to_excel(writer, sheet_name='Avaliar', index=False)

        # Aba: Manter Bloqueado
        df_manter = df_final[df_final['decisao'] == 'MANTER BLOQUEADO']
        if len(df_manter) > 0:
            df_manter.to_excel(writer, sheet_name='Manter Bloqueado', index=False)

        # Aba: Resumo
        n_sem_risco = (df_final['classificacao_risco'] == 'Sem classificacao (conta pos-20/03)').sum()
        resumo_data = {
            'Metrica': [
                'Total usuarios analisados',
                'Encontrados na matriz de risco (20/03)',
                'Sem classificacao (conta criada apos 20/03)',
                '',
                'DESBLOQUEAR (recomendado)',
                'AVALIAR (caso a caso)',
                'MANTER BLOQUEADO',
                '',
                '--- DESBLOQUEAR ---',
                'GGR medio',
                'GGR total',
                'NGR medio',
                'Net deposit medio',
                'Net deposit total',
                'Depositos medio',
                'Dias ativos medio',
                'Idade media conta (dias)',
                '',
                '--- AVALIAR ---',
                'GGR medio',
                'Net deposit medio',
                'Dias ativos medio',
                'Idade media conta (dias)',
                '',
                '--- MANTER BLOQUEADO ---',
                'GGR medio',
                'Net deposit medio',
                'Dias ativos medio',
                'Idade media conta (dias)',
            ],
            'Valor': [
                len(df_final),
                found_risk,
                n_sem_risco,
                '',
                len(df_desbloquear),
                len(df_avaliar),
                len(df_manter),
                '',
                '',
                f"R$ {df_desbloquear['ggr_total'].mean():,.0f}" if len(df_desbloquear) > 0 else 'N/A',
                f"R$ {df_desbloquear['ggr_total'].sum():,.0f}" if len(df_desbloquear) > 0 else 'N/A',
                f"R$ {df_desbloquear['ngr_total'].mean():,.0f}" if len(df_desbloquear) > 0 else 'N/A',
                f"R$ {df_desbloquear['net_deposit'].mean():,.0f}" if len(df_desbloquear) > 0 else 'N/A',
                f"R$ {df_desbloquear['net_deposit'].sum():,.0f}" if len(df_desbloquear) > 0 else 'N/A',
                f"R$ {df_desbloquear['total_depositos'].mean():,.0f}" if len(df_desbloquear) > 0 else 'N/A',
                f"{df_desbloquear['dias_ativos'].mean():,.0f}" if len(df_desbloquear) > 0 else 'N/A',
                f"{df_desbloquear['dias_desde_cadastro'].mean():,.0f}" if len(df_desbloquear) > 0 else 'N/A',
                '',
                '',
                f"R$ {df_avaliar['ggr_total'].mean():,.0f}" if len(df_avaliar) > 0 else 'N/A',
                f"R$ {df_avaliar['net_deposit'].mean():,.0f}" if len(df_avaliar) > 0 else 'N/A',
                f"{df_avaliar['dias_ativos'].mean():,.0f}" if len(df_avaliar) > 0 else 'N/A',
                f"{df_avaliar['dias_desde_cadastro'].mean():,.0f}" if len(df_avaliar) > 0 else 'N/A',
                '',
                '',
                f"R$ {df_manter['ggr_total'].mean():,.0f}" if len(df_manter) > 0 else 'N/A',
                f"R$ {df_manter['net_deposit'].mean():,.0f}" if len(df_manter) > 0 else 'N/A',
                f"{df_manter['dias_ativos'].mean():,.0f}" if len(df_manter) > 0 else 'N/A',
                f"{df_manter['dias_desde_cadastro'].mean():,.0f}" if len(df_manter) > 0 else 'N/A',
            ]
        }
        pd.DataFrame(resumo_data).to_excel(writer, sheet_name='Resumo', index=False)

        # Aba: Tabela de Score
        score_table = pd.DataFrame([
            {'Criterio': 'Idade da conta', 'Condicao': '>90 dias', 'Pontos': '+20', 'Fonte': 'dim_user.registration_date'},
            {'Criterio': '', 'Condicao': '31-90 dias', 'Pontos': '+10', 'Fonte': ''},
            {'Criterio': '', 'Condicao': '<30 dias', 'Pontos': '-10', 'Fonte': ''},
            {'Criterio': '', 'Condicao': 'Sem data', 'Pontos': '-5', 'Fonte': ''},
            {'Criterio': 'GGR total', 'Condicao': '>R$ 500', 'Pontos': '+25', 'Fonte': 'fct_daily.ggr_base'},
            {'Criterio': '', 'Condicao': 'R$ 101-500', 'Pontos': '+15', 'Fonte': ''},
            {'Criterio': '', 'Condicao': 'R$ 1-100', 'Pontos': '+5', 'Fonte': ''},
            {'Criterio': '', 'Condicao': '<-R$ 500', 'Pontos': '-15', 'Fonte': ''},
            {'Criterio': 'Net deposit', 'Condicao': '>R$ 500', 'Pontos': '+15', 'Fonte': 'fct_daily.deposit-cashout'},
            {'Criterio': '', 'Condicao': 'R$ 1-500', 'Pontos': '+5', 'Fonte': ''},
            {'Criterio': '', 'Condicao': '<-R$ 500', 'Pontos': '-10', 'Fonte': ''},
            {'Criterio': 'Dias ativos', 'Condicao': '>30 dias', 'Pontos': '+15', 'Fonte': 'fct_daily (COUNT DISTINCT)'},
            {'Criterio': '', 'Condicao': '8-30 dias', 'Pontos': '+10', 'Fonte': ''},
            {'Criterio': '', 'Condicao': '1-7 dias', 'Pontos': '+3', 'Fonte': ''},
            {'Criterio': '', 'Condicao': '0 dias', 'Pontos': '-5', 'Fonte': ''},
            {'Criterio': 'Ultimo login', 'Condicao': '<7 dias', 'Pontos': '+10', 'Fonte': 'dim_user.auth_last_login_time'},
            {'Criterio': '', 'Condicao': '7-30 dias', 'Pontos': '+5', 'Fonte': ''},
            {'Criterio': '', 'Condicao': '>30 dias', 'Pontos': '0', 'Fonte': ''},
            {'Criterio': 'Severidade fraude', 'Condicao': '100%RB + zero turnover', 'Pontos': '-10', 'Fonte': 'CSV fraudadores'},
            {'Criterio': '', 'Condicao': 'RB >= 99.5%', 'Pontos': '-5', 'Fonte': ''},
            {'Criterio': '', 'Condicao': 'Buyin >R$ 100K', 'Pontos': '-15', 'Fonte': ''},
            {'Criterio': '', 'Condicao': 'Buyin R$ 50K-100K', 'Pontos': '-10', 'Fonte': ''},
            {'Criterio': '', 'Condicao': 'Buyin R$ 10K-50K', 'Pontos': '-5', 'Fonte': ''},
            {'Criterio': '', 'Condicao': 'Fraude em 4+ jogos', 'Pontos': '-10', 'Fonte': ''},
            {'Criterio': '', 'Condicao': 'Fraude em 2-3 jogos', 'Pontos': '-3', 'Fonte': ''},
            {'Criterio': 'Risco (matriz 20/03)', 'Condicao': 'Muito Bom', 'Pontos': '+15', 'Fonte': 'matriz_risco_multibet_2003'},
            {'Criterio': '', 'Condicao': 'Bom', 'Pontos': '+10', 'Fonte': ''},
            {'Criterio': '', 'Condicao': 'Mediano', 'Pontos': '0', 'Fonte': ''},
            {'Criterio': '', 'Condicao': 'Ruim', 'Pontos': '-5', 'Fonte': ''},
            {'Criterio': '', 'Condicao': 'Muito Ruim', 'Pontos': '-15', 'Fonte': ''},
            {'Criterio': '', 'Condicao': 'Sem classificacao (conta pos-20/03)', 'Pontos': '0', 'Fonte': '67% dos usuarios'},
            {'Criterio': 'Teve deposito', 'Condicao': 'Sim (dep > 0)', 'Pontos': '+5', 'Fonte': 'fct_daily.deposit_success'},
            {'Criterio': '', 'Condicao': 'Nao', 'Pontos': '-5', 'Fonte': ''},
            {'Criterio': '', 'Condicao': '', 'Pontos': '', 'Fonte': ''},
            {'Criterio': 'FAIXA DE DECISAO', 'Condicao': 'Score >= 30', 'Pontos': 'DESBLOQUEAR', 'Fonte': 'Jogador valioso, fraude leve'},
            {'Criterio': '', 'Condicao': 'Score 10-29', 'Pontos': 'AVALIAR', 'Fonte': 'Caso intermediario'},
            {'Criterio': '', 'Condicao': 'Score < 10', 'Pontos': 'MANTER BLOQUEADO', 'Fonte': 'Sem valor ou fraude grave'},
        ])
        score_table.to_excel(writer, sheet_name='Tabela de Score', index=False)

    log.info(f"\nExcel salvo: {output_path}")

    # 12. Gerar legenda
    legenda_path = os.path.join(REPORTS, "analise_desbloqueio_fraudadores_FINAL_legenda.txt")
    with open(legenda_path, 'w', encoding='utf-8') as f:
        f.write("=" * 70 + "\n")
        f.write("LEGENDA - Analise de Desbloqueio de Fraudadores de Missoes\n")
        f.write(f"Data: {TODAY.strftime('%d/%m/%Y')}\n")
        f.write("=" * 70 + "\n\n")

        f.write("DICIONARIO DE COLUNAS\n")
        f.write("-" * 40 + "\n")
        cols_desc = {
            'user_id': 'ID externo do usuario (external_id)',
            'ecr_id': 'ID interno transacional (18 digitos)',
            'decisao': 'Recomendacao: DESBLOQUEAR / AVALIAR / MANTER BLOQUEADO',
            'score_desbloqueio': 'Score composto (-100 a +100) baseado em multiplos criterios',
            'justificativa': 'Razoes detalhadas para a decisao',
            'classificacao_risco': 'Classificacao na matriz de risco de 20/03 (Muito Ruim a Muito Bom)',
            'score_risco_bruto': 'Score bruto da matriz de risco (negativo = pior)',
            'score_risco_norm': 'Score normalizado da matriz de risco (0-100)',
            'dias_desde_cadastro': 'Dias desde o cadastro ate hoje (30/03/2026)',
            'teve_ftd': 'Se o usuario ja fez primeiro deposito (True/False)',
            'dias_desde_ftd': 'Dias desde o primeiro deposito',
            'dias_ativos': 'Quantidade de dias distintos com atividade',
            'total_logins': 'Total de logins registrados',
            'dias_desde_ultimo_login': 'Dias desde o ultimo login',
            'total_depositos': 'Soma total de depositos em BRL',
            'qtd_depositos': 'Quantidade de depositos realizados',
            'total_saques': 'Soma total de saques em BRL',
            'net_deposit': 'Depositos - Saques (positivo = casa tem saldo)',
            'ggr_total': 'GGR total (casino + sports, realcash) em BRL. Positivo = casa ganha',
            'ngr_total': 'NGR total (GGR - bonus - custos) em BRL. Principal indicador de rentabilidade',
            'casino_realbet_total': 'Total apostas casino realcash em BRL',
            'casino_real_win_total': 'Total ganhos casino realcash em BRL',
            'sports_realbet_total': 'Total apostas sports realcash em BRL',
            'sports_real_win_total': 'Total ganhos sports realcash em BRL',
            'games_fraud': 'Quantidade de jogos diferentes em que cometeu fraude',
            'total_buyin': 'Volume total de buy-in nas apostas fraudulentas (BRL)',
            'total_rollback': 'Volume total de rollbacks fraudulentos (BRL)',
            'total_turnover_real': 'Turnover real (nao-rollback) nas apostas flagged (BRL)',
            'avg_rb_pct': 'Percentual medio de rollback (100% = fraude pura)',
            'fraud_records': 'Numero de registros de fraude (jogo x periodo)',
            'first_fraud': 'Data/hora da primeira atividade fraudulenta',
            'last_fraud': 'Data/hora da ultima atividade fraudulenta',
            'games_list': 'Lista dos jogos em que cometeu fraude',
            'affiliates': 'IDs dos afiliados associados',
            'country_code': 'Codigo do pais do usuario (ex: BR)',
        }
        for col, desc in cols_desc.items():
            f.write(f"  {col}: {desc}\n")

        f.write("\n\nGLOSSARIO\n")
        f.write("-" * 40 + "\n")
        f.write("  GGR = Gross Gaming Revenue = Apostas - Ganhos do jogador. Positivo = casa ganha.\n")
        f.write("  NGR = Net Gaming Revenue = GGR - Bonus - Custos. Principal indicador de rentabilidade.\n")
        f.write("  Net Deposit = Depositos - Saques. Positivo = jogador deixou dinheiro na plataforma.\n")
        f.write("  LTV = Lifetime Value = Valor total que o jogador gerou para a casa ao longo da vida.\n")
        f.write("  FTD = First Time Deposit = Primeiro deposito do jogador.\n")
        f.write("  Rollback = Cancelamento de aposta. 100% rollback = jogador apostou e cancelou tudo.\n")
        f.write("  Missoes = Desafios que premiam jogadores por completar acoes (ex: fazer X apostas).\n")

        f.write("\n\nCRITERIOS DE DESBLOQUEIO\n")
        f.write("-" * 40 + "\n")
        f.write("  O scoring considera 8 dimensoes:\n")
        f.write("  1. Idade da conta: >90d = +20pts, >30d = +10pts, <30d = -10pts\n")
        f.write("  2. GGR total: >R$500 = +25pts, >R$100 = +15pts, <-R$500 = -15pts\n")
        f.write("  3. Net deposit: >R$500 = +15pts, >0 = +5pts, <-R$500 = -10pts\n")
        f.write("  4. Dias ativos: >30 = +15pts, >7 = +10pts, 0 = -5pts\n")
        f.write("  5. Recencia login: <7d = +10pts, <30d = +5pts\n")
        f.write("  6. Severidade fraude: 100%RB + 0 turnover = -10pts, alto volume = -15pts\n")
        f.write("  7. Classificacao risco: Muito Bom = +15pts, Muito Ruim = -15pts\n")
        f.write("  8. Teve deposito: sim = +5pts, nao = -5pts\n\n")
        f.write("  Score >= 30: DESBLOQUEAR (jogador valioso, fraude foi leve)\n")
        f.write("  Score 10-29: AVALIAR (caso a caso, pode ter potencial)\n")
        f.write("  Score < 10: MANTER BLOQUEADO (sem valor ou fraude grave)\n")

        f.write("\n\nFONTE DOS DADOS\n")
        f.write("-" * 40 + "\n")
        f.write("  Fraude: fraudadores_missoes_completo_com_external_id.csv\n")
        f.write("  Risco: matriz_risco_multibet_2003.csv (computada em 20/03/2026)\n")
        f.write("  Cadastro: ps_bi.dim_user (Athena)\n")
        f.write("  Financeiro: ps_bi.fct_player_activity_daily (Athena)\n")
        f.write("  Login: bireports_ec2.tbl_ecr (Athena)\n")
        f.write(f"  Periodo: dados ate D-1 ({(TODAY - timedelta(days=1)).strftime('%d/%m/%Y')})\n")

        f.write("\n\nACAO SUGERIDA\n")
        f.write("-" * 40 + "\n")
        f.write("  DESBLOQUEAR: Jogadores com historico positivo para a casa.\n")
        f.write("    Acao: desbloquear e monitorar por 30 dias.\n")
        f.write("  AVALIAR: Casos intermediarios que podem ter potencial.\n")
        f.write("    Acao: revisar manualmente, considerar desbloqueio com restricoes.\n")
        f.write("  MANTER BLOQUEADO: Contas de baixo valor ou fraude grave.\n")
        f.write("    Acao: manter bloqueado. Muitos sao contas criadas so para abusar.\n")

    log.info(f"Legenda salva: {legenda_path}")
    log.info("\nAnalise concluida!")

    return df_final


if __name__ == "__main__":
    df_result = main()
