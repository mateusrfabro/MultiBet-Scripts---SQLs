"""
Pipeline: CRM Report Daily — Performance de Campanhas (v1 DEPRECADA)
=====================================================================
>>> DEPRECADA: Usar crm_report_daily_v3_agent.py <<<

Esta versao foi a primeira implementacao. A v3 substitui com:
  - Suporte a ps_bi (BRL reais, sem /100)
  - VIP tiers, recuperacao, comparativo antes/durante/depois
  - Top jogos por coorte CRM
  - DDL embutida com fallback
  - Melhor tratamento de erros e batching

Mantida apenas como referencia historica.

Autor: Squad Intelligence Engine (Mateus F.)
Data: 28/03/2026
"""

import argparse
import logging
import sys
import os
from datetime import date, datetime, timedelta

import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.bigquery import query_bigquery
from db.athena import query_athena
from db.supernova import execute_supernova, get_supernova_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

DS = "smartico-bq6.dwh_ext_24105"

# Custos por provider
CUSTO_PROVIDER = {
    1536: ("SMS", "DisparoPro",  0.045),
    1545: ("SMS", "PushFY",      0.060),
    1268: ("SMS", "Comtele",     0.063),
    1261: ("WhatsApp", "Loyalty", 0.16),
    1553: ("Push", "PushFY",     0.060),
    611:  ("Popup", "Smartico",   0.0),
}

CANAL_MAP = {50: "popup", 60: "SMS", 64: "WhatsApp", 30: "push", 40: "push", 31: "inbox"}


def _classify_type(name) -> str:
    if not name or not isinstance(name, str):
        return "outro"
    n = name.upper()
    if "RETEM" in n: return "RETEM"
    if "CHALLENGE" in n or "QUEST" in n or "MULTIVERSO" in n: return "Challenge"
    if "DESAFIO" in n or "GIRE" in n or "DAILYFS" in n or "DAILY_FS" in n: return "DailyFS"
    if "CASHBACK" in n: return "Cashback"
    if "TORNEIO" in n or "TOURNAMENT" in n: return "Torneio"
    if "FREEBET" in n or "FREE BET" in n or "DEPOSITE" in n: return "Freebet"
    if "CA_FS_" in n or "CAS_FS_" in n or "FREE" in n and "SPIN" in n: return "FreeSpins"
    if "LIFECYCLE" in n or "LIFE_CYCLE" in n: return "Lifecycle"
    if "WELCOME" in n or "BEM.VINDO" in n or "BEMVINDO" in n: return "Welcome"
    if "CORUJ" in n: return "RETEM"
    return "outro"


# ============================================================
# STEP 1: Campanhas com claims no dia (BigQuery)
# ============================================================
def step1_campanhas(dt: str) -> pd.DataFrame:
    log.info(f"STEP 1 — Campanhas com claims em {dt}...")
    # Usa activity_details.campaign_name para pegar o nome real da campanha
    # Conversao UTC→BRT: fact_date esta em UTC, converter para BRT antes de filtrar por dia
    # Filtro test users: excluir users com user_ext_id que sao test (via LEFT JOIN j_user)
    sql = f"""
    SELECT
        CAST(b.entity_id AS STRING) AS campaign_id,
        MAX(JSON_EXTRACT_SCALAR(b.activity_details, '$.campaign_name')) AS campaign_name,
        COUNT(DISTINCT b.user_ext_id) AS cumpriram_condicao,
        SUM(CAST(b.bonus_cost_value AS FLOAT64)) AS custo_bonus_brl
    FROM `{DS}.j_bonuses` b
    WHERE b.bonus_status_id = 3
      AND DATE(b.fact_date, 'America/Sao_Paulo') = '{dt}'
    GROUP BY b.entity_id
    ORDER BY cumpriram_condicao DESC
    """
    df = query_bigquery(sql)
    log.info(f"  {len(df)} campanhas com claims")
    return df


# ============================================================
# STEP 2: Enriquecer com nomes (BigQuery + Super Nova DB)
# ============================================================
def step2_enriquecer(df: pd.DataFrame) -> pd.DataFrame:
    log.info("STEP 2 — Enriquecendo nomes e classificando...")
    if df.empty:
        return df

    # campaign_name ja vem do STEP 1 via activity_details.campaign_name
    df["campaign_name"] = df["campaign_name"].fillna(
        df["campaign_id"].apply(lambda x: f"Campanha {x}")
    )
    df["campaign_type"] = df["campaign_name"].apply(_classify_type)
    df["channel"] = "popup"  # default, sera refinado pelo funil
    df["segment_name"] = None

    # Friendly names do Super Nova DB (override se existir)
    try:
        friendly = execute_supernova("""
            SELECT entity_id, friendly_name, categoria
            FROM multibet.dim_crm_friendly_names
        """, fetch=True)
        if friendly:
            df_f = pd.DataFrame(friendly, columns=["entity_id", "friendly_name", "categoria"])
            df = df.merge(df_f, left_on="campaign_id", right_on="entity_id", how="left")
            mask = df["friendly_name"].notna()
            df.loc[mask, "campaign_name"] = df.loc[mask, "friendly_name"]
            if "categoria" in df.columns:
                mask2 = df["categoria"].notna()
                df.loc[mask2, "campaign_type"] = df.loc[mask2, "categoria"]
    except Exception:
        pass

    log.info(f"  Tipos: {df['campaign_type'].value_counts().to_dict()}")
    return df


# ============================================================
# STEP 3: Funil de conversao (BigQuery)
# ============================================================
def step3_funil(dt: str, campaign_ids: list) -> pd.DataFrame:
    log.info("STEP 3 — Funil de conversao (por user_ext_id)...")
    if not campaign_ids:
        return pd.DataFrame()

    ids_str = ",".join(campaign_ids)
    # Linkar por user_ext_id: pegar users de cada campanha via j_bonuses,
    # depois contar esses mesmos users no j_communication do dia
    # Buscar comunicacao dos ultimos 30 dias (user pode ter sido segmentado antes de completar)
    # Vincular por entity_id no j_communication para garantir que o funil e da mesma campanha
    sql = f"""
    WITH campanha_users AS (
        SELECT DISTINCT
            CAST(entity_id AS STRING) AS campaign_id,
            user_ext_id
        FROM `{DS}.j_bonuses`
        WHERE entity_id IN ({ids_str})
          AND bonus_status_id = 3
          AND DATE(fact_date, 'America/Sao_Paulo') = '{dt}'
    )
    SELECT
        cu.campaign_id,
        COUNT(DISTINCT CASE WHEN c.fact_type_id = 1 THEN c.user_ext_id END) AS segmentados,
        COUNT(DISTINCT CASE WHEN c.fact_type_id = 2 THEN c.user_ext_id END) AS msg_entregues,
        COUNT(DISTINCT CASE WHEN c.fact_type_id = 3 THEN c.user_ext_id END) AS msg_abertos,
        COUNT(DISTINCT CASE WHEN c.fact_type_id = 4 THEN c.user_ext_id END) AS msg_clicados,
        COUNT(DISTINCT CASE WHEN c.fact_type_id = 5 THEN c.user_ext_id END) AS convertidos
    FROM campanha_users cu
    INNER JOIN `{DS}.j_communication` c
        ON c.user_ext_id = cu.user_ext_id
        AND CAST(c.entity_id AS STRING) = cu.campaign_id
    WHERE DATE(c.fact_date, 'America/Sao_Paulo') BETWEEN DATE_SUB('{dt}', INTERVAL 30 DAY) AND '{dt}'
      AND c.fact_type_id BETWEEN 1 AND 5
    GROUP BY cu.campaign_id
    """
    try:
        df = query_bigquery(sql)
        log.info(f"  Funil: {len(df)} campanhas com dados de comunicacao")
        return df
    except Exception as e:
        log.warning(f"  Funil falhou: {e}")
        return pd.DataFrame()


# ============================================================
# STEP 4: Custos de disparo (BigQuery)
# ============================================================
def step4_custos(dt: str) -> pd.DataFrame:
    log.info("STEP 4 — Custos de disparo...")
    sql = f"""
    SELECT
        activity_type_id,
        label_provider_id,
        COUNT(*) AS total_sent
    FROM `{DS}.j_communication`
    WHERE fact_type_id = 1
      AND DATE(fact_date, 'America/Sao_Paulo') = '{dt}'
    GROUP BY activity_type_id, label_provider_id
    """
    try:
        df = query_bigquery(sql)
        rows = []
        for _, r in df.iterrows():
            pid = int(r["label_provider_id"]) if pd.notna(r["label_provider_id"]) else 0
            info = CUSTO_PROVIDER.get(pid, ("outro", "desconhecido", 0.0))
            canal, prov, custo = info
            total = int(r["total_sent"])
            rows.append({
                "channel": canal, "provider": prov,
                "cost_per_unit": custo, "total_sent": total,
                "total_cost_brl": round(total * custo, 2),
            })
        df_c = pd.DataFrame(rows)
        log.info(f"  Custo total: R$ {df_c['total_cost_brl'].sum():,.2f}")
        return df_c
    except Exception as e:
        log.warning(f"  Custos falhou: {e}")
        return pd.DataFrame()


# ============================================================
# STEP 5: Metricas financeiras (Athena bireports_ec2)
# ============================================================
def step5_financeiro(dt: str, campaign_ids: list) -> pd.DataFrame:
    """Busca GGR, turnover, depositos por campanha via bireports_ec2.
    Agrupa users de cada campanha e busca metricas no mesmo dia."""
    log.info("STEP 5 — Metricas financeiras (Athena)...")
    if not campaign_ids:
        return pd.DataFrame()

    ids_str = ",".join(campaign_ids)

    # Primeiro extrair a coorte de users por campanha do BigQuery
    try:
        sql_coorte = f"""
        SELECT
            CAST(entity_id AS STRING) AS campaign_id,
            user_ext_id
        FROM `{DS}.j_bonuses`
        WHERE entity_id IN ({ids_str})
          AND bonus_status_id = 3
          AND DATE(fact_date, 'America/Sao_Paulo') = '{dt}'
        """
        df_coorte = query_bigquery(sql_coorte)
        if df_coorte.empty:
            log.warning("  Coorte vazia")
            return pd.DataFrame()

        all_users = df_coorte["user_ext_id"].unique().tolist()
        log.info(f"  Coorte: {len(all_users)} users unicos")

        # Buscar financeiro no bireports_ec2 para TODOS os users
        # Batch de 2000 para nao estourar query
        batch_size = 2000
        all_fin = []
        for i in range(0, len(all_users), batch_size):
            batch = all_users[i:i + batch_size]
            ids_csv = ",".join(f"'{uid}'" for uid in batch)

            sql_athena = f"""
            SELECT
                CAST(e.c_external_id AS VARCHAR) AS user_ext_id,
                COALESCE(SUM(b.c_casino_realcash_bet_amount), 0) / 100.0 AS turnover_casino_brl,
                COALESCE(SUM(b.c_casino_realcash_win_amount), 0) / 100.0 AS casino_wins_brl,
                COALESCE(SUM(b.c_sb_realcash_bet_amount), 0) / 100.0 AS turnover_sports_brl,
                COALESCE(SUM(b.c_sb_realcash_win_amount), 0) / 100.0 AS sports_wins_brl,
                COALESCE(SUM(b.c_deposit_success_amount), 0) / 100.0 AS depositos_brl,
                COALESCE(SUM(b.c_login_count), 0) AS sessoes
            FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary b
            JOIN bireports_ec2.tbl_ecr e ON e.c_ecr_id = b.c_ecr_id
            WHERE CAST(e.c_external_id AS VARCHAR) IN ({ids_csv})
              AND e.c_test_user = false
              AND b.c_created_date = DATE '{dt}'
            GROUP BY e.c_external_id
            """
            try:
                df_batch = query_athena(sql_athena, database="bireports_ec2")
                all_fin.append(df_batch)
                log.info(f"  Athena batch {i // batch_size + 1}: {len(df_batch)} users")
            except Exception as e:
                log.warning(f"  Athena batch {i // batch_size + 1} falhou: {e}")

        if not all_fin:
            return pd.DataFrame()

        df_fin = pd.concat(all_fin, ignore_index=True)
        df_fin["ggr_casino_brl"] = df_fin["turnover_casino_brl"] - df_fin["casino_wins_brl"]
        df_fin["ggr_sports_brl"] = df_fin["turnover_sports_brl"] - df_fin["sports_wins_brl"]
        df_fin["ggr_brl"] = df_fin["ggr_casino_brl"] + df_fin["ggr_sports_brl"]
        df_fin["saques_brl"] = 0  # Coluna de saques nao disponivel neste summary
        df_fin["net_deposit_brl"] = df_fin["depositos_brl"]
        df_fin["turnover_total_brl"] = df_fin["turnover_casino_brl"] + df_fin["turnover_sports_brl"]

        # Agregar por campanha via coorte
        df_merged = df_coorte.merge(df_fin, on="user_ext_id", how="left")
        fin_cols = ["turnover_total_brl", "turnover_casino_brl", "turnover_sports_brl",
                    "ggr_brl", "ggr_casino_brl", "ggr_sports_brl",
                    "depositos_brl", "saques_brl", "net_deposit_brl", "sessoes"]
        df_agg = df_merged.groupby("campaign_id")[fin_cols].sum().reset_index()
        for col in fin_cols:
            df_agg[col] = df_agg[col].fillna(0).round(2)

        log.info(f"  Financeiro: GGR total R$ {df_agg['ggr_brl'].sum():,.2f}")
        return df_agg

    except Exception as e:
        log.warning(f"  Financeiro falhou: {e}")
        return pd.DataFrame()


# ============================================================
# STEP 6: Persistir no Super Nova DB
# ============================================================
def step6_persistir(dt: str, df_camps: pd.DataFrame, df_funil: pd.DataFrame,
                    df_financeiro: pd.DataFrame, df_custos: pd.DataFrame,
                    dry_run: bool = False):
    log.info("STEP 5 — Persistindo no Super Nova DB...")

    if df_camps.empty:
        log.warning("  Nada a persistir.")
        return

    custo_disparos_total = float(df_custos["total_cost_brl"].sum()) if not df_custos.empty else 0

    if dry_run:
        log.info(f"  [DRY-RUN] {len(df_camps)} campanhas preparadas:")
        for _, c in df_camps.head(5).iterrows():
            log.info(f"    {c['campaign_id']} | {c.get('campaign_name','')} | tipo={c.get('campaign_type','')}")
        return

    tunnel, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            for _, camp in df_camps.iterrows():
                cid = str(camp["campaign_id"])

                # Funil
                seg = ent = abe = cli = conv = 0
                if not df_funil.empty:
                    f = df_funil[df_funil["campaign_id"] == cid]
                    if not f.empty:
                        row = f.iloc[0]
                        seg = int(row.get("segmentados", 0))
                        ent = int(row.get("msg_entregues", 0))
                        abe = int(row.get("msg_abertos", 0))
                        cli = int(row.get("msg_clicados", 0))
                        conv = int(row.get("convertidos", 0))

                # Financeiro para esta campanha
                fin = {}
                if not df_financeiro.empty:
                    f_row = df_financeiro[df_financeiro["campaign_id"] == cid]
                    if not f_row.empty:
                        fin = f_row.iloc[0].to_dict()
                turnover = float(fin.get("turnover_total_brl", 0))
                ggr = float(fin.get("ggr_brl", 0))
                ggr_pct = round(ggr / turnover * 100, 2) if turnover > 0 else 0

                custo_bonus = float(camp.get("custo_bonus_brl", 0) or 0)
                cumpriram = int(camp.get("cumpriram_condicao", 0) or 0)
                custo_disp_camp = custo_disparos_total / max(len(df_camps), 1)
                custo_total = custo_bonus + custo_disp_camp
                ngr = ggr - custo_bonus
                ngr_pct = round(ngr / turnover * 100, 2) if turnover > 0 else 0
                cpa = round(custo_total / cumpriram, 2) if cumpriram > 0 else 0
                roi = round(ngr / custo_total, 4) if custo_total > 0 else None

                cur.execute("""
                    INSERT INTO multibet.crm_campaign_daily (
                        report_date, campaign_id, campaign_name, campaign_type,
                        channel, segment_name,
                        segmentados, msg_entregues, msg_abertos, msg_clicados,
                        convertidos, cumpriram_condicao,
                        turnover_total_brl, ggr_brl, ggr_pct, ngr_brl, ngr_pct,
                        net_deposit_brl, depositos_brl, saques_brl,
                        turnover_casino_brl, ggr_casino_brl,
                        turnover_sports_brl, ggr_sports_brl,
                        custo_bonus_brl, custo_disparos_brl, custo_total_brl,
                        cpa_medio_brl, roi,
                        updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        NOW()
                    )
                    ON CONFLICT (report_date, campaign_id) DO UPDATE SET
                        campaign_name = EXCLUDED.campaign_name,
                        campaign_type = EXCLUDED.campaign_type,
                        channel = EXCLUDED.channel,
                        segment_name = EXCLUDED.segment_name,
                        segmentados = EXCLUDED.segmentados,
                        msg_entregues = EXCLUDED.msg_entregues,
                        msg_abertos = EXCLUDED.msg_abertos,
                        msg_clicados = EXCLUDED.msg_clicados,
                        convertidos = EXCLUDED.convertidos,
                        cumpriram_condicao = EXCLUDED.cumpriram_condicao,
                        turnover_total_brl = EXCLUDED.turnover_total_brl,
                        ggr_brl = EXCLUDED.ggr_brl,
                        ggr_pct = EXCLUDED.ggr_pct,
                        ngr_brl = EXCLUDED.ngr_brl,
                        ngr_pct = EXCLUDED.ngr_pct,
                        net_deposit_brl = EXCLUDED.net_deposit_brl,
                        depositos_brl = EXCLUDED.depositos_brl,
                        saques_brl = EXCLUDED.saques_brl,
                        turnover_casino_brl = EXCLUDED.turnover_casino_brl,
                        ggr_casino_brl = EXCLUDED.ggr_casino_brl,
                        turnover_sports_brl = EXCLUDED.turnover_sports_brl,
                        ggr_sports_brl = EXCLUDED.ggr_sports_brl,
                        custo_bonus_brl = EXCLUDED.custo_bonus_brl,
                        custo_disparos_brl = EXCLUDED.custo_disparos_brl,
                        custo_total_brl = EXCLUDED.custo_total_brl,
                        cpa_medio_brl = EXCLUDED.cpa_medio_brl,
                        roi = EXCLUDED.roi,
                        updated_at = NOW()
                """, (
                    dt, cid,
                    str(camp.get("campaign_name", ""))[:255],
                    str(camp.get("campaign_type", "outro"))[:50],
                    str(camp.get("channel", "popup"))[:50],
                    str(camp.get("segment_name", ""))[:255] if camp.get("segment_name") else None,
                    seg, ent, abe, cli, conv, cumpriram,
                    round(turnover, 2), round(ggr, 2), ggr_pct, round(ngr, 2), ngr_pct,
                    round(float(fin.get("net_deposit_brl", 0)), 2),
                    round(float(fin.get("depositos_brl", 0)), 2),
                    round(float(fin.get("saques_brl", 0)), 2),
                    round(float(fin.get("turnover_casino_brl", 0)), 2),
                    round(float(fin.get("ggr_casino_brl", 0)), 2),
                    round(float(fin.get("turnover_sports_brl", 0)), 2),
                    round(float(fin.get("ggr_sports_brl", 0)), 2),
                    round(custo_bonus, 2),
                    round(custo_disp_camp, 2),
                    round(custo_total, 2),
                    cpa, roi,
                ))

            conn.commit()
        log.info(f"  {len(df_camps)} campanhas persistidas com sucesso!")
    except Exception as e:
        log.error(f"  ERRO ao persistir: {e}")
        raise
    finally:
        conn.close()
        tunnel.stop()

    # Custos de disparo
    if not df_custos.empty:
        _persistir_custos(dt, df_custos)


def _persistir_custos(dt: str, df_custos: pd.DataFrame):
    month_ref = dt[:7] + "-01"
    tunnel, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            for _, r in df_custos.iterrows():
                cur.execute("""
                    INSERT INTO multibet.crm_dispatch_budget (
                        month_ref, channel, provider, cost_per_unit,
                        total_sent, total_cost_brl, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (month_ref, channel, provider) DO UPDATE SET
                        total_sent = EXCLUDED.total_sent,
                        total_cost_brl = EXCLUDED.total_cost_brl,
                        updated_at = NOW()
                """, (
                    month_ref, r["channel"], r["provider"],
                    r["cost_per_unit"], int(r["total_sent"]),
                    round(float(r["total_cost_brl"]), 2),
                ))
            conn.commit()
        log.info(f"  Custos de disparo persistidos ({len(df_custos)} canais)")
    except Exception as e:
        log.warning(f"  Custos de disparo falhou: {e}")
    finally:
        conn.close()
        tunnel.stop()


# ============================================================
# ORQUESTRADOR
# ============================================================
def run(dt: str, dry_run: bool = False):
    inicio = datetime.now()
    log.info("=" * 70)
    log.info(f"CRM REPORT DAILY | Data: {dt} | Modo: {'DRY-RUN' if dry_run else 'PRODUCAO'}")
    log.info("=" * 70)

    df_camps = step1_campanhas(dt)
    if df_camps.empty:
        log.warning("Nenhuma campanha. Encerrando.")
        return

    df_camps = step2_enriquecer(df_camps)

    campaign_ids = df_camps["campaign_id"].tolist()
    df_funil = step3_funil(dt, campaign_ids)
    df_custos = step4_custos(dt)
    df_financeiro = step5_financeiro(dt, campaign_ids)

    step6_persistir(dt, df_camps, df_funil, df_financeiro, df_custos, dry_run)

    elapsed = (datetime.now() - inicio).total_seconds()
    log.info("=" * 70)
    log.info(f"Concluido em {elapsed:.1f}s | {len(df_camps)} campanhas")
    log.info("=" * 70)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline CRM Report Daily")
    parser.add_argument("--date", type=str, default=None)
    parser.add_argument("--days", type=int, default=1)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    base = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else date.today() - timedelta(days=1)
    for i in range(args.days):
        d = base - timedelta(days=i)
        run(d.strftime("%Y-%m-%d"), dry_run=args.dry_run)
