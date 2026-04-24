import sys
sys.path.insert(0, 'c:/Users/NITRO/OneDrive - PGX/Projetos - Super Nova/MultiBet')
from db.bigquery import query_bigquery

# 1. Search for '120326' in client_action field (as substring of the numeric value)
print('=== 1. client_action containing 120326 ===')
try:
    q = """
    SELECT client_action, COUNT(*) as cnt, COUNT(DISTINCT user_id) as users,
           MIN(event_time) as first_event, MAX(event_time) as last_event
    FROM `smartico-bq6.dwh_ext_24105.tr_client_action`
    WHERE CAST(client_action AS STRING) LIKE '%120326%'
    GROUP BY client_action
    ORDER BY cnt DESC
    """
    df = query_bigquery(q)
    print(f'Rows: {len(df)}')
    if len(df) > 0:
        print(df.to_string())
    else:
        print('No matches')
except Exception as e:
    print(f'Error: {e}')

# 2. Search j_engagements activity_details_json for RELAMPAGO
print('\n=== 2. j_engagements - activity_details_json with RELAMPAGO ===')
try:
    q = """
    SELECT engagement_id, user_id, user_ext_id, activity_id, activity_type_id,
           activity_details_json, create_date
    FROM `smartico-bq6.dwh_ext_24105.j_engagements`
    WHERE LOWER(activity_details_json) LIKE '%relampago%'
    LIMIT 20
    """
    df = query_bigquery(q)
    print(f'Rows: {len(df)}')
    if len(df) > 0:
        print(df.to_string())
    else:
        print('No matches')
except Exception as e:
    print(f'Error: {e}')

# 3. Search j_communication for RELAMPAGO (might be a campaign name)
print('\n=== 3. j_communication with RELAMPAGO ===')
try:
    q = 'SELECT * FROM `smartico-bq6.dwh_ext_24105.j_communication` LIMIT 3'
    df = query_bigquery(q)
    print('Columns:', list(df.columns))
    # Now search
    q2 = """
    SELECT *
    FROM `smartico-bq6.dwh_ext_24105.j_communication`
    WHERE LOWER(CAST(communication_details AS STRING)) LIKE '%relampago%'
       OR LOWER(CAST(subject AS STRING)) LIKE '%relampago%'
    LIMIT 20
    """
    try:
        df2 = query_bigquery(q2)
        print(f'Matches: {len(df2)}')
        if len(df2) > 0:
            print(df2.to_string())
    except:
        # Try with whatever string columns exist
        str_cols = [c for c in df.columns if df[c].dtype == 'object']
        print(f'String columns: {str_cols}')
except Exception as e:
    print(f'Error: {e}')

# 4. Search dm_audience for RELAMPAGO (audiences/segments)
print('\n=== 4. dm_audience with RELAMPAGO ===')
try:
    q = 'SELECT * FROM `smartico-bq6.dwh_ext_24105.dm_audience` LIMIT 3'
    df = query_bigquery(q)
    print('Columns:', list(df.columns))
    q2 = 'SELECT * FROM `smartico-bq6.dwh_ext_24105.dm_audience` LIMIT 2000'
    df2 = query_bigquery(q2)
    for col in df2.columns:
        if df2[col].dtype == 'object':
            matches = df2[df2[col].str.contains('RELAMPAGO|relampago|Relampago|Rel.mpago', na=False, case=False)]
            if len(matches) > 0:
                print(f"\nFound in column '{col}':")
                print(matches.to_string())
                break
    else:
        print('No matches in dm_audience')
except Exception as e:
    print(f'Error: {e}')

# 5. Search dm_segment for RELAMPAGO
print('\n=== 5. dm_segment with RELAMPAGO ===')
try:
    q = 'SELECT * FROM `smartico-bq6.dwh_ext_24105.dm_segment` LIMIT 3'
    df = query_bigquery(q)
    print('Columns:', list(df.columns))
    q2 = 'SELECT * FROM `smartico-bq6.dwh_ext_24105.dm_segment` LIMIT 2000'
    df2 = query_bigquery(q2)
    for col in df2.columns:
        if df2[col].dtype == 'object':
            matches = df2[df2[col].str.contains('RELAMPAGO|relampago|Relampago', na=False, case=False)]
            if len(matches) > 0:
                print(f"\nFound in column '{col}':")
                print(matches.to_string())
                break
    else:
        print('No matches in dm_segment')
except Exception as e:
    print(f'Error: {e}')

# 6. Search dm_bonus_template for RELAMPAGO
print('\n=== 6. dm_bonus_template with RELAMPAGO ===')
try:
    q = 'SELECT * FROM `smartico-bq6.dwh_ext_24105.dm_bonus_template` LIMIT 2000'
    df = query_bigquery(q)
    print('Columns:', list(df.columns))
    for col in df.columns:
        if df[col].dtype == 'object':
            matches = df[df[col].str.contains('RELAMPAGO|relampago|Relampago', na=False, case=False)]
            if len(matches) > 0:
                print(f"\nFound in column '{col}':")
                print(matches.to_string())
                break
    else:
        print('No matches in dm_bonus_template')
except Exception as e:
    print(f'Error: {e}')

# 7. Search j_bonuses for RELAMPAGO
print('\n=== 7. j_bonuses with RELAMPAGO ===')
try:
    q = 'SELECT * FROM `smartico-bq6.dwh_ext_24105.j_bonuses` LIMIT 3'
    df = query_bigquery(q)
    print('Columns:', list(df.columns))
except Exception as e:
    print(f'Error: {e}')

# 8. Also: search dm_resource for RELAMPAGO (resources can be push notifications, popups etc)
print('\n=== 8. dm_resource with RELAMPAGO ===')
try:
    q = 'SELECT * FROM `smartico-bq6.dwh_ext_24105.dm_resource` LIMIT 2000'
    df = query_bigquery(q)
    print('Columns:', list(df.columns))
    for col in df.columns:
        if df[col].dtype == 'object':
            matches = df[df[col].str.contains('RELAMPAGO|relampago|Relampago', na=False, case=False)]
            if len(matches) > 0:
                print(f"\nFound in column '{col}':")
                print(matches[['resource_id', col]].head(20).to_string() if 'resource_id' in matches.columns else matches.head(20).to_string())
    print('Done scanning dm_resource')
except Exception as e:
    print(f'Error: {e}')
