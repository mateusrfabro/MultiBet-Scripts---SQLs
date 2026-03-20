import sys
sys.path.insert(0, 'c:/Users/NITRO/OneDrive - PGX/MultiBet')
from db.bigquery import query_bigquery

# 1. Search dm_event_type for RELAMPAGO
print("=== 1. dm_event_type search ===")
try:
    q = """
    SELECT * FROM `smartico-bq6.dwh_ext_24105.dm_event_type`
    WHERE LOWER(event_type_name) LIKE '%relampago%'
       OR LOWER(event_type_uiname) LIKE '%relampago%'
    """
    df = query_bigquery(q)
    print(f"Rows: {len(df)}")
    if len(df) > 0:
        print(df)
    else:
        print("No matches")
except Exception as e:
    print(f"Error: {e}")

# 2. Search dm_automation_rule for RELAMPAGO (could be a CRM automation)
print("\n=== 2. dm_automation_rule search ===")
try:
    q = "SELECT * FROM `smartico-bq6.dwh_ext_24105.dm_automation_rule` LIMIT 5"
    df = query_bigquery(q)
    print("Columns:", list(df.columns))
    # Now search for relampago in string columns
    q2 = "SELECT * FROM `smartico-bq6.dwh_ext_24105.dm_automation_rule` LIMIT 500"
    df2 = query_bigquery(q2)
    for col in df2.columns:
        if df2[col].dtype == 'object':
            matches = df2[df2[col].str.contains('RELAMPAGO|relampago', na=False, case=False)]
            if len(matches) > 0:
                print(f"\nFound in column '{col}':")
                print(matches)
    if not any(df2[col].dtype == 'object' and df2[df2[col].str.contains('RELAMPAGO|relampago', na=False, case=False)].shape[0] > 0 for col in df2.columns):
        print("No matches found in automation_rule")
except Exception as e:
    print(f"Error: {e}")

# 3. Search j_engagements for RELAMPAGO
print("\n=== 3. j_engagements search ===")
try:
    q = "SELECT * FROM `smartico-bq6.dwh_ext_24105.j_engagements` LIMIT 5"
    df = query_bigquery(q)
    print("Columns:", list(df.columns))
except Exception as e:
    print(f"Error: {e}")

# 4. Check dm_deal for RELAMPAGO
print("\n=== 4. dm_deal search ===")
try:
    q = "SELECT * FROM `smartico-bq6.dwh_ext_24105.dm_deal` LIMIT 5"
    df = query_bigquery(q)
    print("Columns:", list(df.columns))
    q2 = "SELECT * FROM `smartico-bq6.dwh_ext_24105.dm_deal` LIMIT 500"
    df2 = query_bigquery(q2)
    found = False
    for col in df2.columns:
        if df2[col].dtype == 'object':
            matches = df2[df2[col].str.contains('RELAMPAGO|relampago', na=False, case=False)]
            if len(matches) > 0:
                print(f"\nFound in column '{col}':")
                print(matches)
                found = True
                break
    if not found:
        print("No matches in dm_deal")
except Exception as e:
    print(f"Error: {e}")

# 5. Check dm_tag for RELAMPAGO
print("\n=== 5. dm_tag search ===")
try:
    q = """
    SELECT * FROM `smartico-bq6.dwh_ext_24105.dm_tag`
    WHERE LOWER(tag_name) LIKE '%relampago%'
       OR LOWER(tag_name) LIKE '%promo%relampago%'
    """
    df = query_bigquery(q)
    print(f"Rows: {len(df)}")
    if len(df) > 0:
        print(df)
    else:
        print("No tag matches")
except Exception as e:
    print(f"Error: {e}")

# 6. Check if PROMO_RELAMPAGO_120326 is literally in tr_client_action as a recent event
# Maybe the action code encodes this - let's see distinct recent actions
print("\n=== 6. Recent client_actions (last 3 days) ===")
try:
    q = """
    SELECT client_action, COUNT(*) as cnt, COUNT(DISTINCT user_id) as users
    FROM `smartico-bq6.dwh_ext_24105.tr_client_action`
    WHERE event_time >= '2026-03-10'
    GROUP BY client_action
    ORDER BY cnt DESC
    LIMIT 30
    """
    df = query_bigquery(q)
    print(df.to_string())
except Exception as e:
    print(f"Error: {e}")

# 7. Search dm_activity_type more broadly - get all rows
print("\n=== 7. All dm_activity_type ===")
try:
    q = "SELECT activity_type_id, activity_name FROM `smartico-bq6.dwh_ext_24105.dm_activity_type` ORDER BY activity_type_id"
    df = query_bigquery(q)
    print(f"Total: {len(df)}")
    # Search
    matches = df[df['activity_name'].str.contains('RELAMPAGO|relampago|PROMO|promo', na=False, case=False)]
    if len(matches) > 0:
        print("Matches:")
        print(matches.to_string())
    else:
        print("No RELAMPAGO/PROMO matches in activity_name")
        # Show last 20 to see naming convention
        print("\nLast 20 activities:")
        print(df.tail(20).to_string())
except Exception as e:
    print(f"Error: {e}")
