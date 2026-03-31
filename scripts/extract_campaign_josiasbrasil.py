"""
Extrator de resultados da campanha utm_campaign=josiasbrasil — DIA ATUAL (2026-03-26).

AVISO: dados do dia corrente podem ser parciais (dia nao fechou).

Fluxo:
1. Descobre jogadores atribuidos a essa campanha (silver.dmu_campaigns + ecr_ec2.tbl_ecr_banner + ps_bi.dim_user)
2. Puxa KPIs: REG, FTD, Depositos, Saques, GGR, NGR
3. Validacao cruzada BigQuery (Smartico)

Uso:
    python scripts/extract_campaign_josiasbrasil.py
"""
import sys
import os
sys.path.insert(0, r"c:\Users\NITRO\OneDrive - PGX\MultiBet")
os.environ["PYTHONIOENCODING"] = "utf-8"

from db.athena import query_athena
from db.bigquery import query_bigquery
import traceback

DATA = "2026-03-26"
UTM_CAMPAIGN = "josiasbrasil"


# =====================================================================
# PASSO 1 -- Descobrir jogadores e affiliate_id(s) da campanha
# =====================================================================

def query_discovery_dmu_campaigns():
    """Busca em silver.dmu_campaigns por utm_campaign.
    Colunas reais: ecr_id, affiliate_id, affiliate, utm_campaign, utm_medium, utm_source, btag, c_reference_url."""
    return f"""
    SELECT
        ecr_id,
        affiliate_id,
        affiliate,
        utm_source,
        utm_medium,
        utm_campaign
    FROM silver.dmu_campaigns
    WHERE LOWER(utm_campaign) = '{UTM_CAMPAIGN}'
    LIMIT 500
    """


def query_discovery_reference_url():
    """Busca em ecr_ec2.tbl_ecr_banner por c_reference_url contendo a campanha."""
    return f"""
    SELECT
        c_ecr_id,
        c_affiliate_id,
        c_affiliate_name,
        c_tracker_id,
        c_banner_id,
        c_reference_url,
        c_created_time
    FROM ecr_ec2.tbl_ecr_banner
    WHERE LOWER(c_reference_url) LIKE '%utm_campaign={UTM_CAMPAIGN}%'
       OR LOWER(c_reference_url) LIKE '%{UTM_CAMPAIGN}%'
    LIMIT 500
    """


def query_discovery_dim_user():
    """Busca em ps_bi.dim_user por utm_campaign, tracker ou affiliate que contenha o nome.
    Colunas reais: ecr_id, external_id, affiliate_id, affiliate, tracker_id, utm_campaign, utm_source, signup_datetime, etc."""
    return f"""
    SELECT
        ecr_id,
        external_id,
        affiliate_id,
        affiliate,
        tracker_id,
        utm_campaign,
        utm_source,
        signup_datetime,
        has_ftd,
        ftd_date,
        ftd_amount_inhouse,
        is_test
    FROM ps_bi.dim_user
    WHERE LOWER(COALESCE(utm_campaign, '')) = '{UTM_CAMPAIGN}'
       OR LOWER(COALESCE(utm_campaign, '')) LIKE '%{UTM_CAMPAIGN}%'
       OR LOWER(COALESCE(CAST(tracker_id AS VARCHAR), '')) LIKE '%{UTM_CAMPAIGN}%'
       OR LOWER(COALESCE(CAST(affiliate AS VARCHAR), '')) LIKE '%{UTM_CAMPAIGN}%'
    LIMIT 500
    """


# =====================================================================
# PASSO 2 -- KPIs para os ecr_ids encontrados
# =====================================================================

def query_reg_hoje(ecr_ids_clause):
    """REG: registros novos HOJE (BRT)."""
    return f"""
    SELECT COUNT(*) AS reg
    FROM bireports_ec2.tbl_ecr
    WHERE CAST(c_sign_up_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '{DATA}'
      AND c_ecr_id IN ({ecr_ids_clause})
      AND c_test_user = false
    """


def query_ftd_hoje(ecr_ids_clause):
    """FTD: primeiros depositos HOJE (BRT)."""
    return f"""
    SELECT COUNT(*) AS ftd, COALESCE(SUM(ftd_amount_inhouse), 0) AS ftd_dep
    FROM ps_bi.dim_user
    WHERE CAST(ftd_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '{DATA}'
      AND ecr_id IN ({ecr_ids_clause})
      AND is_test = false
    """


def query_financeiro_hoje(ecr_ids_clause):
    """Financeiro: depositos, saques, GGR (realcash), bonus, NGR.
    Fonte: bireports_ec2.tbl_ecr_wise_daily_bi_summary (centavos /100).
    GGR usa sub-fund isolation (somente realcash)."""
    return f"""
    SELECT
        COALESCE(SUM(c_deposit_success_amount), 0) / 100.0 AS dep_amount,
        COALESCE(SUM(c_co_success_amount), 0) / 100.0 AS saques,
        COALESCE(SUM(c_casino_realcash_bet_amount - c_casino_realcash_win_amount), 0) / 100.0 AS ggr_cassino,
        COALESCE(SUM(c_sb_realcash_bet_amount - c_sb_realcash_win_amount), 0) / 100.0 AS ggr_sport,
        COALESCE(SUM(c_bonus_issued_amount), 0) / 100.0 AS bonus_cost
    FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary
    WHERE c_created_date = DATE '{DATA}'
      AND c_ecr_id IN ({ecr_ids_clause})
    """


# =====================================================================
# PASSO 3 -- Validacao cruzada BigQuery
# =====================================================================

def query_bq_discovery():
    """Busca no BigQuery (Smartico) por jogadores da campanha.
    core_affiliate_str e NULL (feedback), usar core_affiliate_id."""
    return f"""
    SELECT
        user_ext_id,
        core_registration_date,
        core_affiliate_id,
        core_first_deposit_date
    FROM `smartico-bq6.dwh_ext_24105.j_user`
    WHERE LOWER(COALESCE(CAST(core_campaign AS STRING), '')) LIKE '%{UTM_CAMPAIGN}%'
    LIMIT 500
    """


def query_bq_reg_hoje(ext_ids_clause):
    """Validacao cruzada REG no BigQuery."""
    return f"""
    SELECT COUNT(DISTINCT user_ext_id) AS reg_bq
    FROM `smartico-bq6.dwh_ext_24105.j_user`
    WHERE DATE(core_registration_date, "America/Sao_Paulo") = '{DATA}'
      AND user_ext_id IN ({ext_ids_clause})
    """


# =====================================================================
# EXECUCAO
# =====================================================================

def fmt(valor):
    """Formata valor monetario no padrao BR."""
    if valor < 0:
        return f"-R$ {abs(valor):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def run():
    print(f"\n{'='*70}")
    print(f"CAMPANHA: utm_campaign={UTM_CAMPAIGN}")
    print(f"DATA: {DATA} (AVISO: dia corrente, dados podem ser parciais)")
    print(f"{'='*70}")

    # ---- DISCOVERY ----
    print("\n[1/4] Descobrindo jogadores da campanha...")

    ecr_ids = set()
    external_ids = set()
    affiliate_ids = set()

    # Fonte 1: silver.dmu_campaigns
    try:
        print("  > Buscando em silver.dmu_campaigns...")
        df_dmu = query_athena(query_discovery_dmu_campaigns(), database="silver")
        if len(df_dmu) > 0:
            ecr_ids.update(df_dmu["ecr_id"].dropna().astype(str).tolist())
            affiliate_ids.update(df_dmu["affiliate_id"].dropna().astype(str).tolist())
            print(f"    Encontrados: {len(df_dmu)} registros | affiliates: {affiliate_ids}")
            print(f"    UTM: source={df_dmu['utm_source'].unique().tolist()}, medium={df_dmu['utm_medium'].unique().tolist()}")
        else:
            print("    Nenhum registro encontrado em dmu_campaigns.")
    except Exception as e:
        print(f"    ERRO dmu_campaigns: {e}")

    # Fonte 2: ecr_ec2.tbl_ecr_banner (URL)
    try:
        print("  > Buscando em ecr_ec2.tbl_ecr_banner (reference_url)...")
        df_banner = query_athena(query_discovery_reference_url(), database="ecr_ec2")
        if len(df_banner) > 0:
            ecr_ids.update(df_banner["c_ecr_id"].dropna().astype(str).tolist())
            affiliate_ids.update(df_banner["c_affiliate_id"].dropna().astype(str).tolist())
            print(f"    Encontrados: {len(df_banner)} registros | affiliates: {affiliate_ids}")
            for _, row in df_banner.head(3).iterrows():
                print(f"    URL sample: {str(row.get('c_reference_url', ''))[:120]}...")
        else:
            print("    Nenhum registro encontrado em tbl_ecr_banner.")
    except Exception as e:
        print(f"    ERRO tbl_ecr_banner: {e}")

    # Fonte 3: ps_bi.dim_user (utm_campaign, tracker, affiliate name)
    try:
        print("  > Buscando em ps_bi.dim_user (utm_campaign/tracker/affiliate)...")
        df_dim = query_athena(query_discovery_dim_user(), database="ps_bi")
        if len(df_dim) > 0:
            ecr_ids.update(df_dim["ecr_id"].dropna().astype(str).tolist())
            external_ids.update(df_dim["external_id"].dropna().astype(str).tolist())
            affiliate_ids.update(df_dim["affiliate_id"].dropna().astype(str).tolist())
            print(f"    Encontrados: {len(df_dim)} registros")
            print(f"    utm_campaign values: {df_dim['utm_campaign'].unique().tolist()}")
            print(f"    Affiliates: {df_dim['affiliate_id'].unique().tolist()}")
        else:
            print("    Nenhum registro encontrado em dim_user.")
    except Exception as e:
        print(f"    ERRO dim_user: {e}")

    # Resume discovery
    print(f"\n  TOTAL jogadores encontrados: {len(ecr_ids)}")
    print(f"  Affiliate IDs associados: {affiliate_ids}")

    if len(ecr_ids) == 0:
        print("\n[!] NENHUM jogador encontrado para esta campanha.")
        print("  Possibilidades:")
        print("  - utm_campaign pode nao estar mapeado nas tabelas disponiveis")
        print("  - Campanha pode usar tracker_id em vez de utm_campaign")
        print("  - Verificar se o nome esta correto (case sensitive?)")

        # Busca ampla dmu_campaigns
        print("\n  Tentando busca ampla por '%josias%' em dmu_campaigns...")
        try:
            df_broad = query_athena("""
                SELECT DISTINCT utm_campaign, affiliate_id, affiliate, COUNT(*) as cnt
                FROM silver.dmu_campaigns
                WHERE LOWER(COALESCE(utm_campaign, '')) LIKE '%josias%'
                   OR LOWER(COALESCE(affiliate, '')) LIKE '%josias%'
                GROUP BY utm_campaign, affiliate_id, affiliate
                ORDER BY cnt DESC
                LIMIT 20
            """, database="silver")
            if len(df_broad) > 0:
                print("    Resultados busca ampla dmu_campaigns:")
                print(df_broad.to_string(index=False))
            else:
                print("    Nenhum resultado na busca ampla dmu_campaigns.")
        except Exception as e:
            print(f"    ERRO busca ampla dmu: {e}")

        # Busca ampla dim_user
        print("\n  Tentando busca ampla por '%josias%' em ps_bi.dim_user...")
        try:
            df_broad_du = query_athena("""
                SELECT DISTINCT utm_campaign, affiliate_id, affiliate, tracker_id, COUNT(*) as cnt
                FROM ps_bi.dim_user
                WHERE LOWER(COALESCE(utm_campaign, '')) LIKE '%josias%'
                   OR LOWER(COALESCE(CAST(affiliate AS VARCHAR), '')) LIKE '%josias%'
                   OR LOWER(COALESCE(CAST(tracker_id AS VARCHAR), '')) LIKE '%josias%'
                GROUP BY utm_campaign, affiliate_id, affiliate, tracker_id
                ORDER BY cnt DESC
                LIMIT 20
            """, database="ps_bi")
            if len(df_broad_du) > 0:
                print("    Resultados busca ampla dim_user:")
                print(df_broad_du.to_string(index=False))
            else:
                print("    Nenhum resultado na busca ampla dim_user.")
        except Exception as e:
            print(f"    ERRO busca ampla dim_user: {e}")

        # Busca ampla tbl_ecr_banner
        print("\n  Tentando busca ampla por '%josias%' em tbl_ecr_banner...")
        try:
            df_broad2 = query_athena("""
                SELECT c_affiliate_id, c_affiliate_name, c_tracker_id, COUNT(*) as cnt,
                       ARBITRARY(c_reference_url) as sample_url
                FROM ecr_ec2.tbl_ecr_banner
                WHERE LOWER(COALESCE(c_reference_url, '')) LIKE '%josias%'
                   OR LOWER(COALESCE(c_affiliate_name, '')) LIKE '%josias%'
                GROUP BY c_affiliate_id, c_affiliate_name, c_tracker_id
                ORDER BY cnt DESC
                LIMIT 20
            """, database="ecr_ec2")
            if len(df_broad2) > 0:
                print("    Resultados busca ampla tbl_ecr_banner:")
                print(df_broad2.to_string(index=False))
            else:
                print("    Nenhum resultado na busca ampla tbl_ecr_banner.")
        except Exception as e:
            print(f"    ERRO busca ampla banner: {e}")

        # Busca ampla BigQuery
        print("\n  Tentando busca ampla por '%josias%' no BigQuery...")
        try:
            df_bq_broad = query_bigquery(f"""
                SELECT DISTINCT
                    CAST(core_affiliate_id AS STRING) as affiliate_id,
                    core_campaign,
                    COUNT(*) as cnt
                FROM `smartico-bq6.dwh_ext_24105.j_user`
                WHERE LOWER(COALESCE(CAST(core_campaign AS STRING), '')) LIKE '%josias%'
                GROUP BY 1, 2
                ORDER BY cnt DESC
                LIMIT 10
            """)
            if len(df_bq_broad) > 0:
                print("    Resultados busca ampla BigQuery:")
                print(df_bq_broad.to_string(index=False))
            else:
                print("    Nenhum resultado na busca ampla BigQuery.")
        except Exception as e:
            print(f"    ERRO busca ampla BigQuery: {e}")

        return

    # ---- KPIs ----
    ecr_list = ", ".join(ecr_ids)

    print(f"\n[2/4] Puxando REG e FTD de hoje ({DATA})...")
    try:
        df_reg = query_athena(query_reg_hoje(ecr_list), database="bireports_ec2")
        reg = int(df_reg["reg"].iloc[0])
        print(f"  REG hoje: {reg}")
    except Exception as e:
        reg = "ERRO"
        print(f"  ERRO REG: {e}")

    try:
        df_ftd = query_athena(query_ftd_hoje(ecr_list), database="ps_bi")
        ftd = int(df_ftd["ftd"].iloc[0])
        ftd_dep = float(df_ftd["ftd_dep"].iloc[0])
        print(f"  FTD hoje: {ftd} | FTD Deposit: {fmt(ftd_dep)}")
    except Exception as e:
        ftd, ftd_dep = "ERRO", 0
        print(f"  ERRO FTD: {e}")

    print(f"\n[3/4] Puxando financeiro de hoje ({DATA})...")
    try:
        df_fin = query_athena(query_financeiro_hoje(ecr_list), database="bireports_ec2")
        dep = float(df_fin["dep_amount"].iloc[0])
        saq = float(df_fin["saques"].iloc[0])
        ggr_c = float(df_fin["ggr_cassino"].iloc[0])
        ggr_s = float(df_fin["ggr_sport"].iloc[0])
        bonus = float(df_fin["bonus_cost"].iloc[0])
        ngr = ggr_c + ggr_s - bonus
    except Exception as e:
        dep = saq = ggr_c = ggr_s = bonus = ngr = 0
        print(f"  ERRO financeiro: {e}")

    # ---- VALIDACAO CRUZADA BIGQUERY ----
    print(f"\n[4/4] Validacao cruzada BigQuery...")
    reg_bq = "N/A"
    match_status = "N/A"
    try:
        df_bq = query_bigquery(query_bq_discovery())
        if len(df_bq) > 0:
            external_ids.update(df_bq["user_ext_id"].dropna().astype(str).tolist())
            print(f"  BigQuery: {len(df_bq)} jogadores encontrados na campanha")

            # REG hoje no BQ
            if external_ids:
                ext_list = ", ".join(f"'{x}'" for x in external_ids)
                df_bq_reg = query_bigquery(query_bq_reg_hoje(ext_list))
                reg_bq = int(df_bq_reg["reg_bq"].iloc[0])
                if isinstance(reg, int):
                    match_status = "OK" if abs(reg - reg_bq) <= 3 else f"DIVERGE ({reg} vs {reg_bq})"
                print(f"  REG BigQuery hoje: {reg_bq} | Status: {match_status}")
        else:
            print("  Nenhum jogador encontrado no BigQuery para esta campanha.")
    except Exception as e:
        print(f"  ERRO BigQuery: {e}")

    # ---- REPORT FINAL ----
    print(f"\n{'='*70}")
    print(f"REPORT -- utm_campaign={UTM_CAMPAIGN} -- {DATA}")
    print(f"{'='*70}")
    print(f"[!] DADOS PARCIAIS (dia corrente, nao fechou)")
    print(f"")
    print(f"Jogadores na campanha (historico): {len(ecr_ids)}")
    print(f"Affiliate IDs:                     {affiliate_ids}")
    print(f"")
    print(f"--- KPIs HOJE ---")
    print(f"REG (registros):    {reg}")
    print(f"FTD (1o deposito):  {ftd}")
    if isinstance(ftd_dep, (int, float)):
        print(f"FTD Deposit:        {fmt(ftd_dep)}")
    if isinstance(dep, (int, float)):
        print(f"Depositos total:    {fmt(dep)}")
        print(f"Saques:             {fmt(saq)}")
        print(f"GGR Cassino:        {fmt(ggr_c)}")
        print(f"GGR Sportsbook:     {fmt(ggr_s)}")
        print(f"Bonus Cost:         {fmt(bonus)}")
        print(f"NGR:                {fmt(ngr)}")
        print(f"Net Deposit:        {fmt(dep - saq)}")
    print(f"")
    print(f"--- VALIDACAO CRUZADA ---")
    print(f"REG Athena:   {reg}")
    print(f"REG BigQuery: {reg_bq}")
    print(f"Status:       {match_status}")
    print(f"{'='*70}")


if __name__ == "__main__":
    run()
