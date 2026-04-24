"""
Investigação Sports GGR 11/03/2026 — V2 (com c_custom_json)
"""

import sys, json
sys.path.insert(0, r"C:\Users\NITRO\OneDrive - PGX\Projetos - Super Nova\MultiBet")

import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 220)
pd.set_option('display.max_colwidth', 80)
pd.set_option('display.max_rows', 300)

from db.redshift import query_redshift

# ==============================================================================
# 1. DEEP DIVE: arthuroficial777
# ==============================================================================
print("=" * 100)
print("1. DEEP DIVE: arthuroficial777@gmail.com")
print("=" * 100)

# Dados do jogador
ecr_id = 178391773139721504
ext_id = 386161773139721

# Todas as transações SB de 08/03 a 12/03 com c_custom_json
sql_arthur = f"""
SELECT
    c_txn_id,
    CONVERT_TIMEZONE('UTC', 'America/Sao_Paulo', c_start_time) AS ts_brt,
    c_txn_type,
    CASE c_txn_type
        WHEN 59 THEN 'SB_BUYIN' WHEN 112 THEN 'SB_WIN'
        WHEN 89 THEN 'SB_LOWERING' WHEN 61 THEN 'SB_CANCEL'
        WHEN 64 THEN 'SB_SETTLEMENT'
        ELSE 'OTHER'
    END AS tipo,
    c_op_type,
    c_amount_in_ecr_ccy / 100.0 AS valor_brl,
    c_session_id,
    c_session_id_at_vendor,
    c_custom1,
    c_custom_json
FROM fund.tbl_real_fund_txn
WHERE c_ecr_id = {ecr_id}
  AND c_txn_type IN (59, 61, 64, 89, 112)
  AND c_start_time >= '2026-03-08 03:00:00'
  AND c_start_time < '2026-03-13 03:00:00'
  AND c_txn_status = 'SUCCESS'
ORDER BY c_start_time
"""
df_arthur = query_redshift(sql_arthur)
print(f"\nTotal transações SB: {len(df_arthur)}")
for _, row in df_arthur.iterrows():
    print(f"\n  {row['ts_brt']} | {row['tipo']:12s} | R$ {row['valor_brl']:>12,.2f} | session_vendor={row['c_session_id_at_vendor']}")
    if row['c_custom_json']:
        try:
            cj = json.loads(row['c_custom_json'])
            bets = cj.get('extraInfo', {}).get('betInfo', {}).get('bets', [])
            for b in bets:
                print(f"    betSlipId={cj['extraInfo']['betInfo']['betSlipId']} | stake={b.get('totalStake')} | return={b.get('totalReturn')} | odds={b.get('TotalOdds')} | legs={b.get('LegCount')}")
                for leg in b.get('betDetails', []):
                    print(f"      {leg.get('eventName')} | {leg.get('marketName')}: {leg.get('selectionName')} @ {leg.get('odds')} | {leg.get('LegStatus')} | {leg.get('TournamentName','').strip()}")
        except:
            print(f"    [JSON parse error]")

# ==============================================================================
# 2. CLUSTER 18:33 — Parsing c_custom_json
# ==============================================================================
print("\n" + "=" * 100)
print("2. CLUSTER 18:33 BRT — Detalhes dos eventos via c_custom_json")
print("=" * 100)

# Buscar as top wins do cluster com custom_json
sql_cluster = """
SELECT
    CONVERT_TIMEZONE('UTC', 'America/Sao_Paulo', c_start_time) AS ts_brt,
    c_ecr_id,
    c_amount_in_ecr_ccy / 100.0 AS valor_brl,
    c_session_id_at_vendor,
    c_custom_json
FROM fund.tbl_real_fund_txn
WHERE c_txn_type = 112
  AND c_txn_status = 'SUCCESS'
  AND c_start_time >= '2026-03-11 21:33:00'
  AND c_start_time < '2026-03-11 21:34:00'
ORDER BY c_amount_in_ecr_ccy DESC
LIMIT 50
"""
df_cluster = query_redshift(sql_cluster)
print(f"\nTotal SB_WIN no minuto 18:33: {len(df_cluster)}")

# Extrair eventos de cada aposta
all_events = []
all_bets = []
for _, row in df_cluster.iterrows():
    if row['c_custom_json']:
        try:
            cj = json.loads(row['c_custom_json'])
            bets = cj.get('extraInfo', {}).get('betInfo', {}).get('bets', [])
            for b in bets:
                bet_info = {
                    'valor_win': row['valor_brl'],
                    'stake': b.get('totalStake'),
                    'return': b.get('totalReturn'),
                    'odds': b.get('TotalOdds'),
                    'legs': b.get('LegCount'),
                    'betType': b.get('betType'),
                    'isLive': b.get('isLive')
                }
                all_bets.append(bet_info)
                for leg in b.get('betDetails', []):
                    all_events.append({
                        'eventName': leg.get('eventName', '').strip(),
                        'sport': leg.get('SportTypeName', '').strip(),
                        'tournament': leg.get('TournamentName', '').strip(),
                        'market': leg.get('marketName', '').strip(),
                        'selection': leg.get('selectionName', '').strip(),
                        'odds': leg.get('odds'),
                        'status': leg.get('LegStatus'),
                        'eventId': leg.get('EventId'),
                        'win_amount': row['valor_brl']
                    })
        except:
            pass

if all_events:
    df_events = pd.DataFrame(all_events)
    print("\n--- Eventos mais frequentes nas apostas do cluster ---")
    evt_grp = df_events.groupby(['eventName', 'tournament', 'sport']).agg(
        ocorrencias=('status', 'count'),
        total_win=('win_amount', 'sum')
    ).sort_values('ocorrencias', ascending=False)
    print(evt_grp.to_string())

    print("\n--- Mercados apostados ---")
    mkt_grp = df_events.groupby(['market', 'selection']).agg(
        ocorrencias=('status', 'count')
    ).sort_values('ocorrencias', ascending=False)
    print(mkt_grp.to_string())

    print("\n--- Status dos legs ---")
    print(df_events['status'].value_counts().to_string())

if all_bets:
    df_bets = pd.DataFrame(all_bets)
    print("\n--- Resumo das apostas ---")
    print(f"  Total apostas analisadas: {len(df_bets)}")
    print(f"  Stake médio: R$ {df_bets['stake'].astype(float).mean():,.2f}")
    print(f"  Retorno médio: R$ {df_bets['return'].astype(float).mean():,.2f}")
    print(f"  Odds médias: {df_bets['odds'].astype(float).mean():.4f}")
    print(f"  Nº de legs mais comum: {df_bets['legs'].mode().iloc[0]}")
    print(f"  betType: {df_bets['betType'].value_counts().to_dict()}")

# ==============================================================================
# 3. TODAS as SB_WIN do dia 11/03 — por janela horária
# ==============================================================================
print("\n" + "=" * 100)
print("3. OVERVIEW DO DIA 11/03 — SB_WIN por hora")
print("=" * 100)

sql_hourly = """
SELECT
    EXTRACT(HOUR FROM CONVERT_TIMEZONE('UTC', 'America/Sao_Paulo', c_start_time)) AS hora_brt,
    COUNT(*) AS qtd_wins,
    COUNT(DISTINCT c_ecr_id) AS jogadores,
    SUM(c_amount_in_ecr_ccy)/100.0 AS total_win_brl,
    AVG(c_amount_in_ecr_ccy)/100.0 AS avg_win_brl,
    MAX(c_amount_in_ecr_ccy)/100.0 AS max_win_brl
FROM fund.tbl_real_fund_txn
WHERE c_txn_type = 112
  AND c_txn_status = 'SUCCESS'
  AND c_start_time >= '2026-03-11 03:00:00'
  AND c_start_time < '2026-03-12 03:00:00'
GROUP BY 1
ORDER BY 1
"""
df_hourly = query_redshift(sql_hourly)
print(df_hourly.to_string(index=False))

# ==============================================================================
# 4. Eventos do cluster 18:50 (segundo pico)
# ==============================================================================
print("\n" + "=" * 100)
print("4. CLUSTER 18:50 BRT — Detalhes")
print("=" * 100)

sql_1850 = """
SELECT
    CONVERT_TIMEZONE('UTC', 'America/Sao_Paulo', c_start_time) AS ts_brt,
    c_ecr_id,
    c_amount_in_ecr_ccy / 100.0 AS valor_brl,
    c_custom_json
FROM fund.tbl_real_fund_txn
WHERE c_txn_type = 112
  AND c_txn_status = 'SUCCESS'
  AND c_amount_in_ecr_ccy > 100000
  AND c_start_time >= '2026-03-11 21:50:00'
  AND c_start_time < '2026-03-11 21:51:00'
ORDER BY c_amount_in_ecr_ccy DESC
LIMIT 10
"""
df_1850 = query_redshift(sql_1850)
print(f"\nSB_WIN > R$1.000 às 18:50: {len(df_1850)}")
for _, row in df_1850.iterrows():
    print(f"\n  R$ {row['valor_brl']:,.2f}")
    if row['c_custom_json']:
        try:
            cj = json.loads(row['c_custom_json'])
            bets = cj.get('extraInfo', {}).get('betInfo', {}).get('bets', [])
            for b in bets:
                print(f"    stake={b.get('totalStake')} return={b.get('totalReturn')} odds={b.get('TotalOdds')}")
                for leg in b.get('betDetails', []):
                    print(f"      {leg.get('eventName')} | {leg.get('TournamentName','').strip()} | {leg.get('marketName')}: {leg.get('selectionName')} @ {leg.get('odds')}")
        except:
            pass

# ==============================================================================
# 5. vendor.tbl_sports_book_info — colunas e range
# ==============================================================================
print("\n" + "=" * 100)
print("5. vendor.tbl_sports_book_info — metadata")
print("=" * 100)

sql_sbi = """
SELECT column_name FROM information_schema.columns
WHERE table_schema = 'vendor' AND table_name = 'tbl_sports_book_info'
ORDER BY ordinal_position
"""
df_sbi = query_redshift(sql_sbi)
print(df_sbi.to_string(index=False))

sql_sbi_range = """
SELECT MIN(c_created_date) AS min_dt, MAX(c_created_date) AS max_dt, COUNT(*) AS total
FROM vendor.tbl_sports_book_info
"""
df_sbi_r = query_redshift(sql_sbi_range)
print(f"\nRange: {df_sbi_r.to_string(index=False)}")

print("\n" + "=" * 100)
print("INVESTIGAÇÃO CONCLUÍDA")
print("=" * 100)
