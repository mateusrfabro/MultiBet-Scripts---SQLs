"""
Pipeline: multibet.fact_crm_daily_performance — Opção B (Isolamento de Coorte)

Fluxo:
  1. BigQuery  → Extração de Coorte (j_bonuses, bonus_status_id=3)
  2. BigQuery  → Funil de comunicação (j_communication) para a coorte
  3. BigQuery  → Custo de bônus (bonus_cost_value) por campanha
  4. Redshift  → Métricas financeiras (GGR, Depósitos, Sessões) via INNER JOIN com coorte
  5. Python    → Deduplicação Last Click (última campanha do dia por usuário)
  6. Python    → Cálculo de BTR e ROI
  7. Super Nova DB → Persistência em blocos JSONB (funil, financeiro, comparativo)

Regras:
  - Duplo filtro entity_id + label_bonus_template_id (isolation)
  - Custo fixo R$0,16 por disparo SMS/WhatsApp (activity_type_id IN (60,61))
  - Last Click: sobreposição no mesmo dia → credita só a última campanha
  - Valores Redshift em centavos → /100.0 para BRL
  - Timestamps Redshift em UTC → CONVERT_TIMEZONE para BRT

Uso:
    python pipelines/fact_crm_daily_performance.py [--dias 30] [--dry-run]
"""

import sys
import json
import logging
import argparse
from datetime import datetime, timedelta
from decimal import Decimal

import pandas as pd

sys.path.insert(0, "c:/Users/NITRO/OneDrive - PGX/MultiBet")

from db.bigquery import query_bigquery
from db.redshift import query_redshift
from db.supernova import execute_supernova, get_supernova_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

DATASET = "`smartico-bq6.dwh_ext_24105`"

# Canais com custo de disparo (SMS=60, WhatsApp=61)
CANAIS_COM_CUSTO = (60, 61)
CUSTO_POR_DISPARO_BRL = 0.16

# Funnel fact_type_id (Smartico j_communication)
FUNNEL_MAP = {
    1: "enviados",
    2: "entregues",
    3: "abertos",
    4: "clicados",
    5: "convertidos",
}


# ============================================================================
# STEP 1 — Extração de Coorte (BigQuery)
# ============================================================================
def extrair_coorte(dias: int) -> pd.DataFrame:
    """
    Para cada entity_id + label_bonus_template_id, lista os user_ext_id
    que tiveram bonus_status_id = 3 (Claimed) nos últimos N dias.

    Retorna: DataFrame com entity_id, label_bonus_template_id, user_ext_id,
             fact_date (do claim), bonus_cost_value, engagement_uid
    """
    sql = f"""
    SELECT
        b.entity_id,
        b.label_bonus_template_id,
        b.user_ext_id,
        b.fact_date                         AS claim_date,
        CAST(b.bonus_cost_value AS FLOAT64) AS bonus_cost_value,
        b.engagement_uid,
        t.public_name                       AS nome_template
    FROM {DATASET}.j_bonuses b
    LEFT JOIN {DATASET}.dm_bonus_template t
        ON b.label_bonus_template_id = t.label_bonus_template_id
    WHERE b.bonus_status_id = 3
      AND b.fact_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {dias} DAY)
    """
    log.info(f"Step 1 — Extraindo coorte (bonus_status_id=3, últimos {dias} dias)...")
    df = query_bigquery(sql)
    log.info(f"  Coorte: {len(df)} claims, {df['user_ext_id'].nunique()} jogadores, "
             f"{df['entity_id'].nunique()} entity_ids")
    return df


# ============================================================================
# STEP 2 — Funil de Comunicação (BigQuery)
# ============================================================================
def extrair_funil(dias: int) -> pd.DataFrame:
    """
    Extrai o funil de comunicação (j_communication) por engagement_uid,
    contando fact_type_id 1-5 e identificando disparos com custo.

    Retorna: DataFrame com engagement_uid, user_ext_id, activity_type_id,
             fact_type_id, fact_date, e uma flag de custo
    """
    sql = f"""
    SELECT
        c.engagement_uid,
        c.user_ext_id,
        c.activity_type_id,
        c.fact_type_id,
        c.fact_date,
        c.root_engagement_id
    FROM {DATASET}.j_communication c
    WHERE c.fact_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {dias} DAY)
      AND c.fact_type_id IN (1, 2, 3, 4, 5)
    """
    log.info(f"Step 2 — Extraindo funil de comunicação (últimos {dias} dias)...")
    df = query_bigquery(sql)
    log.info(f"  Funil: {len(df)} registros, {df['user_ext_id'].nunique()} jogadores")
    return df


# ============================================================================
# STEP 3 — Métricas Financeiras (Redshift) com Isolamento de Coorte
# ============================================================================
def extrair_metricas_redshift(user_ext_ids: list, data_inicio: str, data_fim: str) -> pd.DataFrame:
    """
    Injeta os user_ext_ids na query do Redshift via VALUES temporário
    (Redshift não suporta CREATE TEMP TABLE em read-only, então usamos
    subquery com VALUES ou IN clause em batches).

    Calcula por jogador:
      - GGR = Casino Bets - Casino Wins (c_txn_type 27 - 45)
      - Depósitos = soma de depósitos bem-sucedidos (c_txn_type=1, SUCCESS)
      - Sessões = COUNT DISTINCT c_session_id
    """
    if not user_ext_ids:
        log.warning("  Nenhum user_ext_id para consultar no Redshift.")
        return pd.DataFrame()

    # Converte para lista de IDs limpa (external_id é bigint no Redshift)
    ids_limpos = [str(uid).strip() for uid in user_ext_ids if uid and str(uid).strip()]
    if not ids_limpos:
        return pd.DataFrame()

    log.info(f"Step 3 — Consultando Redshift para {len(ids_limpos)} jogadores...")

    # Batch de IDs (Redshift tem limite de ~10K no IN clause)
    BATCH_SIZE = 5000
    all_results = []

    for i in range(0, len(ids_limpos), BATCH_SIZE):
        batch = ids_limpos[i:i + BATCH_SIZE]
        ids_csv = ",".join(batch)

        sql = f"""
        WITH coorte AS (
            SELECT e.c_ecr_id, e.c_external_id
            FROM ecr.tbl_ecr e
            WHERE e.c_external_id IN ({ids_csv})
        )
        SELECT
            c.c_external_id                                     AS user_ext_id,

            -- GGR = Bets - Wins (em centavos)
            COALESCE(SUM(CASE WHEN t.c_txn_type = 27
                THEN t.c_amount_in_ecr_ccy END), 0)            AS casino_bets_cents,
            COALESCE(SUM(CASE WHEN t.c_txn_type = 45
                THEN t.c_amount_in_ecr_ccy END), 0)            AS casino_wins_cents,

            -- Depósitos (c_txn_type=1, SUCCESS)
            COALESCE(SUM(CASE WHEN t.c_txn_type = 1 AND t.c_txn_status = 'SUCCESS'
                THEN t.c_amount_in_ecr_ccy END), 0)            AS depositos_cents,

            -- Sessões distintas
            COUNT(DISTINCT t.c_session_id)                      AS sessoes

        FROM coorte c
        INNER JOIN fund.tbl_real_fund_txn t
            ON c.c_ecr_id = t.c_ecr_id
        WHERE t.c_txn_status = 'SUCCESS'
          AND CONVERT_TIMEZONE('UTC', 'America/Sao_Paulo', t.c_start_time)
              BETWEEN '{data_inicio}' AND '{data_fim}'
          AND t.c_txn_type IN (1, 27, 45)
        GROUP BY c.c_external_id
        """

        df_batch = query_redshift(sql)
        all_results.append(df_batch)
        log.info(f"  Batch {i // BATCH_SIZE + 1}: {len(df_batch)} jogadores com dados")

    if not all_results:
        return pd.DataFrame()

    df = pd.concat(all_results, ignore_index=True)

    # Converte centavos → BRL
    df["casino_bets_brl"] = df["casino_bets_cents"].astype(float) / 100.0
    df["casino_wins_brl"] = df["casino_wins_cents"].astype(float) / 100.0
    df["depositos_brl"] = df["depositos_cents"].astype(float) / 100.0
    df["ggr_brl"] = df["casino_bets_brl"] - df["casino_wins_brl"]

    log.info(f"  Total: {len(df)} jogadores | GGR total: R$ {df['ggr_brl'].sum():,.2f}")
    return df


# ============================================================================
# STEP 4 — Deduplicação Last Click
# ============================================================================
def deduplicar_last_click(df_coorte: pd.DataFrame, df_funil: pd.DataFrame) -> pd.DataFrame:
    """
    Para cada user_ext_id + dia, se houve sobreposição de campanhas,
    credita o valor financeiro apenas à ÚLTIMA campanha que interagiu
    com o usuário (baseado no claim_date mais recente).

    Retorna: df_coorte com coluna 'is_last_click' (True/False)
    """
    log.info("Step 4 — Aplicando deduplicação Last Click...")

    if df_coorte.empty:
        return df_coorte

    df = df_coorte.copy()

    # Data do claim (sem hora) para agrupar por dia
    df["claim_day"] = pd.to_datetime(df["claim_date"]).dt.date

    # Rank por user_ext_id + dia: o claim mais recente ganha
    df["rank"] = (
        df.sort_values("claim_date", ascending=False)
        .groupby(["user_ext_id", "claim_day"])
        .cumcount()
    )
    df["is_last_click"] = df["rank"] == 0

    total = len(df)
    last_clicks = df["is_last_click"].sum()
    duplicados = total - last_clicks
    log.info(f"  Total claims: {total} | Last Click: {last_clicks} | "
             f"Descartados por sobreposição: {duplicados}")

    df.drop(columns=["rank"], inplace=True)
    return df


# ============================================================================
# STEP 5 — Montagem dos blocos JSONB e cálculo de BTR/ROI
# ============================================================================
def montar_blocos_jsonb(
    df_coorte: pd.DataFrame,
    df_funil: pd.DataFrame,
    df_financeiro: pd.DataFrame,
) -> list[dict]:
    """
    Agrupa por campanha (entity_id + label_bonus_template_id) e monta
    os blocos JSONB: funil, financeiro, comparativo.

    Retorna lista de dicts prontos para INSERT.
    """
    log.info("Step 5 — Montando blocos JSONB por campanha...")

    if df_coorte.empty:
        log.warning("  Coorte vazia, nada a processar.")
        return []

    # Filtra apenas Last Click para atribuição financeira
    df_lc = df_coorte[df_coorte["is_last_click"]].copy()

    # Merge com financeiro (user_ext_id → métricas)
    df_lc["user_ext_id"] = df_lc["user_ext_id"].astype(str)
    if not df_financeiro.empty:
        df_financeiro["user_ext_id"] = df_financeiro["user_ext_id"].astype(str)
        df_lc = df_lc.merge(df_financeiro, on="user_ext_id", how="left")
    else:
        df_lc["ggr_brl"] = 0.0
        df_lc["depositos_brl"] = 0.0
        df_lc["sessoes"] = 0
        df_lc["casino_bets_brl"] = 0.0
        df_lc["casino_wins_brl"] = 0.0

    # Funil: contar fact_type_id por engagement_uid → campanha
    # Link: df_coorte.engagement_uid → df_funil.engagement_uid
    df_funil_camp = pd.DataFrame()
    if not df_funil.empty:
        # Filtra funil apenas para engagements da coorte
        eng_uids = set(df_coorte["engagement_uid"].dropna().unique())
        df_funil_filtrado = df_funil[df_funil["engagement_uid"].isin(eng_uids)].copy()

        # Contagem de disparos SMS/WhatsApp (fact_type_id=1 = Enviado)
        disparos_custo = df_funil_filtrado[
            (df_funil_filtrado["activity_type_id"].isin(CANAIS_COM_CUSTO))
            & (df_funil_filtrado["fact_type_id"] == 1)
        ]

        # Merge engagement_uid → entity_id via coorte
        eng_to_camp = df_coorte[["engagement_uid", "entity_id", "label_bonus_template_id"]].drop_duplicates()
        df_funil_filtrado = df_funil_filtrado.merge(eng_to_camp, on="engagement_uid", how="inner")
        disparos_custo = disparos_custo.merge(eng_to_camp, on="engagement_uid", how="inner")
    else:
        df_funil_filtrado = pd.DataFrame()
        disparos_custo = pd.DataFrame()

    # Agrupa por campanha
    campanhas = df_lc.groupby(["entity_id", "label_bonus_template_id", "nome_template"])
    resultados = []

    for (entity_id, template_id, nome_template), grupo in campanhas:
        n_jogadores = len(grupo)
        claim_min = grupo["claim_date"].min()
        claim_max = grupo["claim_date"].max()

        # --- BLOCO FUNIL ---
        funil_data = {}
        if not df_funil_filtrado.empty:
            funil_camp = df_funil_filtrado[
                (df_funil_filtrado["entity_id"] == entity_id)
                & (df_funil_filtrado["label_bonus_template_id"] == template_id)
            ]
            for ft_id, nome_etapa in FUNNEL_MAP.items():
                funil_data[nome_etapa] = int(
                    funil_camp[funil_camp["fact_type_id"] == ft_id]["user_ext_id"].nunique()
                )
        else:
            for nome_etapa in FUNNEL_MAP.values():
                funil_data[nome_etapa] = 0

        funil_data["jogadores_impactados"] = n_jogadores
        funil_data["bonus_claimed"] = n_jogadores  # todos neste grupo são status=3

        # --- BLOCO FINANCEIRO ---
        # BTR = custo de bônus (soma de bonus_cost_value dos claims)
        btr_total = float(grupo["bonus_cost_value"].fillna(0).sum())

        # Custo de disparos SMS/WhatsApp
        n_disparos = 0
        if not disparos_custo.empty:
            n_disparos = int(disparos_custo[
                (disparos_custo["entity_id"] == entity_id)
                & (disparos_custo["label_bonus_template_id"] == template_id)
            ].shape[0])
        custo_disparos = n_disparos * CUSTO_POR_DISPARO_BRL

        # Métricas do Redshift (somente Last Click)
        ggr_total = float(grupo["ggr_brl"].fillna(0).sum())
        depositos_total = float(grupo["depositos_brl"].fillna(0).sum())
        sessoes_total = int(grupo["sessoes"].fillna(0).sum())
        bets_total = float(grupo["casino_bets_brl"].fillna(0).sum())
        wins_total = float(grupo["casino_wins_brl"].fillna(0).sum())

        # Custo total = BTR + disparos
        custo_total = btr_total + custo_disparos

        # ROI = (GGR - Custo Total) / Custo Total (se custo > 0)
        roi = ((ggr_total - custo_total) / custo_total) if custo_total > 0 else None

        # New NGR = GGR - BTR (sem disparos, é receita líquida do jogo)
        new_ngr = ggr_total - btr_total

        financeiro_data = {
            "ggr_brl": round(ggr_total, 2),
            "casino_bets_brl": round(bets_total, 2),
            "casino_wins_brl": round(wins_total, 2),
            "depositos_brl": round(depositos_total, 2),
            "sessoes": sessoes_total,
            "btr_brl": round(btr_total, 2),
            "custo_disparos_brl": round(custo_disparos, 2),
            "n_disparos_sms_whatsapp": n_disparos,
            "custo_por_disparo_brl": CUSTO_POR_DISPARO_BRL,
            "custo_total_brl": round(custo_total, 2),
            "new_ngr_brl": round(new_ngr, 2),
            "roi": round(roi, 4) if roi is not None else None,
        }

        # --- BLOCO COMPARATIVO ---
        # Métricas per-capita para comparação entre campanhas
        arpu = ggr_total / n_jogadores if n_jogadores > 0 else 0
        deposito_medio = depositos_total / n_jogadores if n_jogadores > 0 else 0
        sessao_media = sessoes_total / n_jogadores if n_jogadores > 0 else 0
        cpa_efetivo = custo_total / n_jogadores if n_jogadores > 0 else 0

        comparativo_data = {
            "arpu_brl": round(arpu, 2),
            "deposito_medio_brl": round(deposito_medio, 2),
            "sessao_media": round(sessao_media, 2),
            "cpa_efetivo_brl": round(cpa_efetivo, 2),
            "jogadores_unicos": n_jogadores,
            "taxa_conversao_funil": round(
                funil_data.get("convertidos", 0) / funil_data["enviados"], 4
            ) if funil_data.get("enviados", 0) > 0 else None,
        }

        resultados.append({
            "campanha_id": f"{entity_id}_{template_id}",
            "campanha_name": str(nome_template) if nome_template else f"entity_{entity_id}",
            "campanha_start": str(pd.Timestamp(claim_min).date()) if pd.notna(claim_min) else None,
            "campanha_end": str(pd.Timestamp(claim_max).date()) if pd.notna(claim_max) else None,
            "funil": funil_data,
            "financeiro": financeiro_data,
            "comparativo": comparativo_data,
        })

    log.info(f"  {len(resultados)} campanhas montadas.")
    return resultados


# ============================================================================
# STEP 6 — Persistência no Super Nova DB
# ============================================================================
def persistir_supernova(resultados: list[dict], period: str, period_start: str, period_end: str, dry_run: bool = False):
    """
    Upsert dos resultados na tabela multibet.fact_crm_daily_performance.

    Usa ON CONFLICT (campanha_id, period) para evitar duplicatas.
    Blocos JSONB são substituídos integralmente (não merge parcial).
    """
    if not resultados:
        log.warning("Step 6 — Nenhum resultado para persistir.")
        return

    log.info(f"Step 6 — Persistindo {len(resultados)} campanhas no Super Nova DB...")

    if dry_run:
        log.info("  [DRY-RUN] Mostrando 3 primeiros resultados:")
        for r in resultados[:3]:
            log.info(f"    {r['campanha_id']} | {r['campanha_name']}")
            log.info(f"    funil: {json.dumps(r['funil'], ensure_ascii=False)}")
            log.info(f"    financeiro: {json.dumps(r['financeiro'], ensure_ascii=False)}")
            log.info(f"    comparativo: {json.dumps(r['comparativo'], ensure_ascii=False)}")
        return

    # Garante que existe constraint UNIQUE para upsert
    _garantir_unique_constraint()

    tunnel, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            for r in resultados:
                cur.execute("""
                    INSERT INTO multibet.fact_crm_daily_performance
                        (campanha_id, campanha_name, campanha_start, campanha_end,
                         period, period_start, period_end,
                         funil, financeiro, comparativo,
                         created_at, updated_at)
                    VALUES
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (campanha_id, period)
                    DO UPDATE SET
                        campanha_name  = EXCLUDED.campanha_name,
                        campanha_start = EXCLUDED.campanha_start,
                        campanha_end   = EXCLUDED.campanha_end,
                        period_start   = EXCLUDED.period_start,
                        period_end     = EXCLUDED.period_end,
                        funil          = EXCLUDED.funil,
                        financeiro     = EXCLUDED.financeiro,
                        comparativo    = EXCLUDED.comparativo,
                        updated_at     = NOW()
                """, (
                    r["campanha_id"],
                    r["campanha_name"],
                    r["campanha_start"],
                    r["campanha_end"],
                    period,
                    period_start,
                    period_end,
                    json.dumps(r["funil"], ensure_ascii=False),
                    json.dumps(r["financeiro"], ensure_ascii=False),
                    json.dumps(r["comparativo"], ensure_ascii=False),
                ))
            conn.commit()
        log.info(f"  {len(resultados)} campanhas persistidas com sucesso.")
    finally:
        conn.close()
        tunnel.stop()


def _garantir_unique_constraint():
    """Cria UNIQUE constraint se não existir (para ON CONFLICT)."""
    try:
        execute_supernova("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conname = 'uq_fact_crm_campanha_period'
                ) THEN
                    ALTER TABLE multibet.fact_crm_daily_performance
                    ADD CONSTRAINT uq_fact_crm_campanha_period
                    UNIQUE (campanha_id, period);
                END IF;
            END $$;
        """)
    except Exception as e:
        log.warning(f"  Constraint já existe ou erro: {e}")


# ============================================================================
# ORQUESTRADOR
# ============================================================================
def run(dias: int = 30, dry_run: bool = False):
    """Pipeline completa."""
    inicio = datetime.now()
    log.info("=" * 70)
    log.info("Pipeline: fact_crm_daily_performance — Opção B (Isolamento de Coorte)")
    log.info(f"Período: últimos {dias} dias | Dry Run: {dry_run}")
    log.info("=" * 70)

    data_fim = datetime.now().strftime("%Y-%m-%d 23:59:59")
    data_inicio = (datetime.now() - timedelta(days=dias)).strftime("%Y-%m-%d 00:00:00")
    period = f"L{dias}D"  # ex: "L30D" = Last 30 Days
    period_start = (datetime.now() - timedelta(days=dias)).strftime("%Y-%m-%d")
    period_end = datetime.now().strftime("%Y-%m-%d")

    # Step 1 — Coorte
    df_coorte = extrair_coorte(dias)
    if df_coorte.empty:
        log.warning("Coorte vazia. Encerrando pipeline.")
        return

    # Step 2 — Funil de comunicação
    df_funil = extrair_funil(dias)

    # Step 3 — Métricas financeiras (Redshift)
    user_ids = df_coorte["user_ext_id"].dropna().unique().tolist()
    df_financeiro = extrair_metricas_redshift(user_ids, data_inicio, data_fim)

    # Step 4 — Deduplicação Last Click
    df_coorte = deduplicar_last_click(df_coorte, df_funil)

    # Step 5 — Montagem JSONB + BTR/ROI
    resultados = montar_blocos_jsonb(df_coorte, df_funil, df_financeiro)

    # Step 6 — Persistência
    persistir_supernova(resultados, period, period_start, period_end, dry_run)

    elapsed = (datetime.now() - inicio).total_seconds()
    log.info("=" * 70)
    log.info(f"Pipeline finalizada em {elapsed:.1f}s | "
             f"{len(resultados)} campanhas processadas")
    log.info("=" * 70)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline fact_crm_daily_performance")
    parser.add_argument("--dias", type=int, default=30, help="Período em dias (default: 30)")
    parser.add_argument("--dry-run", action="store_true", help="Não persiste, só mostra os resultados")
    args = parser.parse_args()

    run(dias=args.dias, dry_run=args.dry_run)
