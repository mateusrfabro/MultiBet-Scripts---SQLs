"""
Investigação Sports GGR 11/03/2026
- Deep dive arthuroficial777
- Cluster 18:33-18:50 BRT
- Identificação de evento esportivo
"""

import sys
sys.path.insert(0, r"C:\Users\NITRO\OneDrive - PGX\MultiBet")

import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 220)
pd.set_option('display.max_colwidth', 60)
pd.set_option('display.max_rows', 200)

from db.redshift import query_redshift

# ==============================================================================
# 0. Descobrir colunas reais de tbl_real_fund_txn
# ==============================================================================
print("--- 0. Colunas de fund.tbl_real_fund_txn ---")
sql_fund_cols = """
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'fund' AND table_name = 'tbl_real_fund_txn'
ORDER BY ordinal_position
"""
df_fund_cols = query_redshift(sql_fund_cols)
print(df_fund_cols.to_string(index=False))
fund_col_list = df_fund_cols['column_name'].tolist()
print(f"\nTotal colunas: {len(fund_col_list)}")

# Verificar quais colunas úteis existem
useful = ['c_event_id', 'c_game_id', 'c_session_id', 'c_vendor_id',
          'c_txn_ref', 'c_ref', 'c_description', 'c_channel',
          'c_sub_channel', 'c_external_id', 'c_round_id']
for col in useful:
    exists = col in fund_col_list
    print(f"  {col}: {'SIM' if exists else 'NÃO'}")

# ==============================================================================
# 1. DEEP DIVE: arthuroficial777@gmail.com
# ==============================================================================
print("\n" + "=" * 80)
print("1. DEEP DIVE: arthuroficial777@gmail.com")
print("=" * 80)

# Email está em c_email_id na tbl_ecr
print("\n--- 1a. Buscando IDs do jogador via c_email_id ---")
sql_player = """
SELECT c_ecr_id, c_external_id, c_email_id, c_signup_time
FROM ecr.tbl_ecr
WHERE LOWER(c_email_id) LIKE '%arthuroficial777%'
"""
df_player = query_redshift(sql_player)
print(df_player.to_string(index=False))

if df_player.empty:
    print("Não encontrado. Tentando busca mais ampla...")
    sql_p2 = """
    SELECT c_ecr_id, c_external_id, c_email_id
    FROM ecr.tbl_ecr
    WHERE LOWER(c_email_id) LIKE '%arthuroficial%'
    LIMIT 10
    """
    df_player = query_redshift(sql_p2)
    print(df_player.to_string(index=False))

if not df_player.empty:
    ecr_id = df_player['c_ecr_id'].iloc[0]
    ext_id = df_player['c_external_id'].iloc[0]
    print(f"\n==> ecr_id = {ecr_id}, external_id = {ext_id}")

    # 1b. Construir SELECT dinâmico com colunas que existem
    select_cols = ['c_txn_id']
    select_cols.append("CONVERT_TIMEZONE('UTC', 'America/Sao_Paulo', c_start_time) AS ts_brt")
    select_cols.append('c_txn_type')
    select_cols.append("""CASE c_txn_type
            WHEN 59 THEN 'SB_BUYIN'
            WHEN 112 THEN 'SB_WIN'
            WHEN 89 THEN 'SB_LOWERING_BET'
            WHEN 61 THEN 'SB_BUYIN_CANCEL'
            WHEN 60 THEN 'SB_LEAVE_TABLE'
            WHEN 63 THEN 'SB_PLAYER_BET_CANCEL'
            WHEN 64 THEN 'SB_SETTLEMENT'
            WHEN 113 THEN 'SB_WIN_CANCEL'
            ELSE 'OTHER_SB'
        END AS txn_label""")
    select_cols.append('c_op_type')
    select_cols.append('c_amount_in_ecr_ccy / 100.0 AS valor_brl')
    select_cols.append('c_txn_status')

    # Adicionar colunas opcionais que existem
    for col in ['c_game_id', 'c_session_id', 'c_vendor_id', 'c_channel']:
        if col in fund_col_list:
            select_cols.append(col)

    print("\n--- 1b. Transações sportsbook 08/03 a 12/03 ---")
    sql_txns = f"""
    SELECT {', '.join(select_cols)}
    FROM fund.tbl_real_fund_txn
    WHERE c_ecr_id = {ecr_id}
      AND c_txn_type IN (59, 60, 61, 63, 64, 89, 112, 113)
      AND c_start_time >= '2026-03-08 03:00:00'
      AND c_start_time < '2026-03-13 03:00:00'
      AND c_txn_status = 'SUCCESS'
    ORDER BY c_start_time
    """
    df_txns = query_redshift(sql_txns)
    print(f"\nTotal de transações: {len(df_txns)}")
    print(df_txns.to_string(index=False))

    # 1c. Resumo por dia e tipo
    if not df_txns.empty:
        print("\n--- 1c. Resumo por dia e tipo ---")
        df_txns['dia'] = pd.to_datetime(df_txns['ts_brt']).dt.date
        resumo = df_txns.groupby(['dia', 'txn_label']).agg(
            qtd=('valor_brl', 'count'),
            total_brl=('valor_brl', 'sum')
        ).reset_index()
        print(resumo.to_string(index=False))

        # 1d. Tentar match via session_id
        print("\n--- 1d. Matching WIN 11/03 com BETs via session_id ---")
        wins_11 = df_txns[
            (pd.to_datetime(df_txns['ts_brt']).dt.date == pd.Timestamp('2026-03-11').date()) &
            (df_txns['txn_label'] == 'SB_WIN')
        ]
        if not wins_11.empty:
            print(f"\nWINs em 11/03: {len(wins_11)}")
            for _, win in wins_11.iterrows():
                sid = win.get('c_session_id')
                print(f"\n  WIN R${win['valor_brl']:,.2f} @ {win['ts_brt']} | session={sid}")
                if sid:
                    same_session = df_txns[df_txns['c_session_id'] == sid]
                    if len(same_session) > 0:
                        print(f"    Todas txns na session {sid}:")
                        for _, t in same_session.iterrows():
                            print(f"      {t['ts_brt']} | {t['txn_label']} | R${t['valor_brl']:,.2f}")
        else:
            print("Nenhum SB_WIN em 11/03 para esse jogador.")
            # Verificar se WIN está como SB_SETTLEMENT (tipo 64)
            sett_11 = df_txns[
                (pd.to_datetime(df_txns['ts_brt']).dt.date == pd.Timestamp('2026-03-11').date()) &
                (df_txns['txn_label'] == 'SB_SETTLEMENT')
            ]
            if not sett_11.empty:
                print(f"\nMas encontrou SB_SETTLEMENT em 11/03: {len(sett_11)}")
                print(sett_11.to_string(index=False))
else:
    print("\n*** JOGADOR NÃO ENCONTRADO ***")
    ecr_id = None

# ==============================================================================
# 2. CLUSTER 18:00-19:00 BRT em 11/03
# ==============================================================================
print("\n" + "=" * 80)
print("2. CLUSTER DE SB_WIN > R$1.000 entre 18:00-19:00 BRT em 11/03")
print("=" * 80)

# Construir SELECT dinâmico
sel2 = ["CONVERT_TIMEZONE('UTC', 'America/Sao_Paulo', f.c_start_time) AS ts_brt",
        "DATE_TRUNC('minute', CONVERT_TIMEZONE('UTC', 'America/Sao_Paulo', f.c_start_time)) AS minuto_brt",
        "f.c_ecr_id", "e.c_external_id",
        "f.c_amount_in_ecr_ccy / 100.0 AS valor_brl"]
for col in ['c_game_id', 'c_session_id', 'c_vendor_id']:
    if col in fund_col_list:
        sel2.append(f"f.{col}")

sql_cluster = f"""
SELECT {', '.join(sel2)}
FROM fund.tbl_real_fund_txn f
JOIN ecr.tbl_ecr e ON e.c_ecr_id = f.c_ecr_id
WHERE f.c_txn_type = 112
  AND f.c_txn_status = 'SUCCESS'
  AND f.c_amount_in_ecr_ccy > 100000
  AND f.c_start_time >= '2026-03-11 21:00:00'
  AND f.c_start_time < '2026-03-11 22:00:00'
ORDER BY f.c_start_time
"""
df_cluster = query_redshift(sql_cluster)
print(f"\nTotal de SB_WIN > R$1.000 entre 18:00-19:00 BRT: {len(df_cluster)}")

if not df_cluster.empty:
    print(df_cluster.to_string(index=False))

    print("\n--- 2b. Agrupamento por minuto ---")
    df_cluster['minuto'] = pd.to_datetime(df_cluster['minuto_brt']).dt.strftime('%H:%M')
    por_minuto = df_cluster.groupby('minuto').agg(
        qtd_wins=('valor_brl', 'count'),
        total_brl=('valor_brl', 'sum'),
        jogadores_distintos=('c_ecr_id', 'nunique')
    ).reset_index()
    print(por_minuto.to_string(index=False))

    n_jog = df_cluster['c_ecr_id'].nunique()
    total_pago = df_cluster['valor_brl'].sum()
    print(f"\nJogadores distintos: {n_jog}")
    print(f"Total pago: R$ {total_pago:,.2f}")

    # Agrupamentos por game_id, session_id, vendor_id
    for col in ['c_game_id', 'c_session_id', 'c_vendor_id']:
        if col in df_cluster.columns:
            print(f"\n--- {col} na janela ---")
            col_data = df_cluster[df_cluster[col].notna() & (df_cluster[col].astype(str) != '')]
            if not col_data.empty:
                grp = col_data.groupby(col).agg(
                    qtd=('valor_brl', 'count'),
                    total_brl=('valor_brl', 'sum'),
                    jogadores=('c_ecr_id', 'nunique')
                ).sort_values('total_brl', ascending=False).head(15)
                print(grp.to_string())
            else:
                print(f"  {col} está NULL/vazio para todas.")
else:
    print("Nenhuma SB_WIN > R$1.000 nessa janela.")
    # Sem filtro mínimo
    sql_c2 = """
    SELECT COUNT(*) AS total,
           SUM(c_amount_in_ecr_ccy)/100.0 AS soma_brl,
           MAX(c_amount_in_ecr_ccy)/100.0 AS max_brl
    FROM fund.tbl_real_fund_txn
    WHERE c_txn_type = 112
      AND c_txn_status = 'SUCCESS'
      AND c_start_time >= '2026-03-11 21:00:00'
      AND c_start_time < '2026-03-11 22:00:00'
    """
    df_c2 = query_redshift(sql_c2)
    print(df_c2.to_string(index=False))

# ==============================================================================
# 3. IDENTIFICAR EVENTO/ESPORTE
# ==============================================================================
print("\n" + "=" * 80)
print("3. TENTATIVA DE IDENTIFICAR O EVENTO ESPORTIVO")
print("=" * 80)

# 3a. Amostra de SB_WIN 11/03
print("\n--- 3a. Amostra de colunas com dados em SB_WIN 11/03 ---")
sample_cols = [c for c in fund_col_list if c not in ['c_txn_id', 'c_ecr_id', 'c_amount_in_ecr_ccy', 'c_start_time', 'c_txn_status']]
sql_sample = f"""
SELECT {', '.join(sample_cols)}
FROM fund.tbl_real_fund_txn
WHERE c_txn_type = 112
  AND c_txn_status = 'SUCCESS'
  AND c_start_time >= '2026-03-11 21:00:00'
  AND c_start_time < '2026-03-11 22:00:00'
LIMIT 5
"""
try:
    df_sample = query_redshift(sql_sample)
    print(df_sample.to_string(index=False))
except Exception as ex:
    print(f"Erro: {ex}")
    # Fallback: mostrar só as que sabemos
    sql_sample2 = """
    SELECT c_game_id, c_session_id, c_vendor_id, c_channel, c_sub_channel, c_op_type
    FROM fund.tbl_real_fund_txn
    WHERE c_txn_type = 112 AND c_txn_status = 'SUCCESS'
      AND c_start_time >= '2026-03-11 21:00:00'
      AND c_start_time < '2026-03-11 22:00:00'
    LIMIT 5
    """
    df_sample2 = query_redshift(sql_sample2)
    print(df_sample2.to_string(index=False))

# 3b. Tabelas auxiliares de sportsbook no fund schema
print("\n--- 3b. fund.tbl_event_level_details ---")
try:
    sql_eld_cols = """
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'fund' AND table_name = 'tbl_event_level_details'
    ORDER BY ordinal_position
    """
    df_eld_cols = query_redshift(sql_eld_cols)
    print("Colunas:")
    print(df_eld_cols.to_string(index=False))

    sql_eld_range = """
    SELECT COUNT(*) AS total FROM fund.tbl_event_level_details
    """
    df_eld_r = query_redshift(sql_eld_range)
    print(f"\nTotal registros: {df_eld_r.iloc[0,0]}")

    if df_eld_r.iloc[0,0] > 0:
        sql_eld_data = """
        SELECT * FROM fund.tbl_event_level_details
        ORDER BY 1 DESC LIMIT 10
        """
        df_eld = query_redshift(sql_eld_data)
        print(df_eld.to_string(index=False))
except Exception as ex:
    print(f"Erro: {ex}")

# 3c. vendor.tbl_sports_book_info
print("\n--- 3c. vendor.tbl_sports_book_info ---")
try:
    sql_sbi_cols = """
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'vendor' AND table_name = 'tbl_sports_book_info'
    ORDER BY ordinal_position
    """
    df_sbi_cols = query_redshift(sql_sbi_cols)
    print("Colunas:")
    print(df_sbi_cols.to_string(index=False))

    sql_sbi_range = """
    SELECT MIN(c_created_date) AS min_dt, MAX(c_created_date) AS max_dt, COUNT(*) AS total
    FROM vendor.tbl_sports_book_info
    """
    df_sbi_r = query_redshift(sql_sbi_range)
    print(f"\nRange de dados:")
    print(df_sbi_r.to_string(index=False))

    # Amostra dos dados mais recentes
    sql_sbi_sample = """
    SELECT * FROM vendor.tbl_sports_book_info
    ORDER BY c_created_date DESC LIMIT 5
    """
    df_sbi_s = query_redshift(sql_sbi_sample)
    print(f"\nAmostra recente:")
    print(df_sbi_s.to_string(index=False))
except Exception as ex:
    print(f"Erro: {ex}")

# 3d. Top game_ids/session_ids de SB_WIN grandes em 11/03
print("\n--- 3d. Agrupamento de SB_WINs > R$500 por game_id/session_id ---")
grp_cols = []
for c in ['c_game_id', 'c_session_id', 'c_vendor_id']:
    if c in fund_col_list:
        grp_cols.append(c)

if grp_cols:
    sql_grp = f"""
    SELECT {', '.join(grp_cols)},
           COUNT(*) AS txns,
           COUNT(DISTINCT c_ecr_id) AS jogadores,
           SUM(c_amount_in_ecr_ccy)/100.0 AS total_brl,
           MAX(c_amount_in_ecr_ccy)/100.0 AS max_brl
    FROM fund.tbl_real_fund_txn
    WHERE c_txn_type = 112
      AND c_txn_status = 'SUCCESS'
      AND c_amount_in_ecr_ccy > 50000
      AND c_start_time >= '2026-03-11 03:00:00'
      AND c_start_time < '2026-03-12 03:00:00'
    GROUP BY {', '.join(grp_cols)}
    ORDER BY total_brl DESC
    LIMIT 30
    """
    df_grp = query_redshift(sql_grp)
    print(df_grp.to_string(index=False))

    # Mapear game_ids no catálogo
    if 'c_game_id' in df_grp.columns:
        gids = df_grp['c_game_id'].dropna().unique().tolist()
        if gids:
            gids_str = ",".join([f"'{g}'" for g in gids[:20]])
            print("\n--- 3e. Mapeamento no catálogo de jogos ---")
            sql_cat = f"""
            SELECT c_game_id, c_game_desc, c_vendor_id
            FROM bireports.tbl_vendor_games_mapping_data
            WHERE c_game_id IN ({gids_str})
            """
            try:
                df_cat = query_redshift(sql_cat)
                if not df_cat.empty:
                    print(df_cat.to_string(index=False))
                else:
                    print(f"Nenhum game_id encontrado no catálogo. IDs: {gids[:10]}")
            except Exception as ex:
                print(f"Erro: {ex}")

# 3f. Outras tabelas de sportsbook
print("\n--- 3f. tbl_bo_sportsbook_open_bets_settlement ---")
try:
    sql_obs_cols = """
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'fund' AND table_name = 'tbl_bo_sportsbook_open_bets_settlement'
    ORDER BY ordinal_position
    """
    df_obs_cols = query_redshift(sql_obs_cols)
    print("Colunas:")
    print(df_obs_cols.to_string(index=False))

    sql_obs_range = """
    SELECT COUNT(*) AS total FROM fund.tbl_bo_sportsbook_open_bets_settlement
    """
    df_obs_r = query_redshift(sql_obs_range)
    print(f"Total: {df_obs_r.iloc[0,0]}")
except Exception as ex:
    print(f"Erro: {ex}")

# 3g. tbl_player_level_event_snapshot
print("\n--- 3g. tbl_player_level_event_snapshot ---")
try:
    sql_ples_cols = """
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'fund' AND table_name = 'tbl_player_level_event_snapshot'
    ORDER BY ordinal_position
    """
    df_ples_cols = query_redshift(sql_ples_cols)
    print("Colunas:")
    print(df_ples_cols.to_string(index=False))

    sql_ples_r = """
    SELECT COUNT(*) AS total FROM fund.tbl_player_level_event_snapshot
    """
    df_ples_r = query_redshift(sql_ples_r)
    print(f"Total: {df_ples_r.iloc[0,0]}")

    if df_ples_r.iloc[0,0] > 0:
        sql_ples_data = """
        SELECT * FROM fund.tbl_player_level_event_snapshot
        ORDER BY 1 DESC LIMIT 5
        """
        df_ples = query_redshift(sql_ples_data)
        print(df_ples.to_string(index=False))
except Exception as ex:
    print(f"Erro: {ex}")

# 3h. Verificar vendor.tbl_sports_book_info para o jogador (se encontrado)
if ecr_id:
    print(f"\n--- 3h. vendor.tbl_sports_book_info para ext_id={ext_id} ---")
    try:
        sql_sbi_player = f"""
        SELECT *
        FROM vendor.tbl_sports_book_info
        WHERE c_customer_id = {ext_id}
        ORDER BY c_created_date DESC
        LIMIT 20
        """
        df_sbi_p = query_redshift(sql_sbi_player)
        print(f"Registros: {len(df_sbi_p)}")
        if not df_sbi_p.empty:
            print(df_sbi_p.to_string(index=False))
    except Exception as ex:
        print(f"Erro: {ex}")

print("\n" + "=" * 80)
print("INVESTIGAÇÃO CONCLUÍDA")
print("=" * 80)
