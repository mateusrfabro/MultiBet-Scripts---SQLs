"""
DE-PARA de Affiliates/Trackers -- MultiBet
==========================================
Tabela DIMENSAO enxuta: mapeia affiliate_id -> fonte de trafego.
Sem metricas, sem sinais brutos -- apenas o mapeamento.

Fontes Athena:
- ecr_ec2.tbl_ecr_banner (c_reference_url com click IDs e UTMs)

Destino:
- Super Nova DB: multibet.dim_affiliate_source

Correcoes validadas com arquiteto (18/03/2026):
- regexp_like (Trino) para classificacao de click IDs
- NULL -> 'Direct/Organic' para evitar quebras no front-end
- Coluna real: c_reference_url (nao c_url)
- Coluna real: c_created_time (nao c_created_date)
- ps_bi.ftd_amount_inhouse JA esta em BRL (NAO dividir /100)
"""

import sys
sys.path.insert(0, "c:/Users/NITRO/OneDrive - PGX/MultiBet")

from db.athena import query_athena
from db.supernova import execute_supernova, get_supernova_connection
import psycopg2.extras
import pandas as pd
from datetime import date
import os
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ======================================================================
# SQL Athena: mapeamento affiliate_id -> fonte de trafego
# Apenas campos de dimensao, sem metricas
# ======================================================================
SQL_DE_PARA = """
WITH
banner_sinais AS (
    SELECT
        CAST(c_affiliate_id AS VARCHAR) AS affiliate_id,

        -- Contagem de sinais por tipo de click ID
        COUNT_IF(regexp_like(lower(c_reference_url), 'gclid='))             AS cnt_google_ads,
        COUNT_IF(regexp_like(lower(c_reference_url), 'fbclid='))            AS cnt_facebook,
        COUNT_IF(regexp_like(lower(c_reference_url), 'ttclid='))            AS cnt_tiktok,
        COUNT_IF(regexp_like(lower(c_reference_url), 'kclid|kwai'))         AS cnt_kwai,
        COUNT_IF(regexp_like(lower(c_reference_url), 'instagram')
                 OR regexp_like(lower(c_reference_url), 'utm_source=ig'))   AS cnt_instagram,
        COUNT_IF(regexp_like(lower(c_reference_url), 'source_id='))         AS cnt_afiliado_direto,

        -- Extrair exemplos de UTM e nomes
        MAX(REGEXP_EXTRACT(c_reference_url, 'utm_campaign=([^&]+)', 1))     AS utm_campaign,
        MAX(REGEXP_EXTRACT(c_reference_url, 'utm_source=([^&]+)', 1))       AS utm_source,
        MAX(REGEXP_EXTRACT(c_reference_url, 'utm_medium=([^&]+)', 1))       AS utm_medium,
        MAX(REGEXP_EXTRACT(c_reference_url, 'source_id=([^&]+)', 1))        AS source_id,
        MAX(NULLIF(c_affiliate_name, ''))                                   AS affiliate_name

    FROM ecr_ec2.tbl_ecr_banner
    WHERE c_affiliate_id IS NOT NULL
      AND CAST(c_affiliate_id AS VARCHAR) NOT IN ('', '0')
    GROUP BY 1
)

SELECT
    affiliate_id,
    COALESCE(affiliate_name, '')    AS affiliate_name,
    COALESCE(source_id, '')         AS source_id,

    -- Classificacao: fonte com mais sinais
    CASE
        WHEN cnt_google_ads >= cnt_facebook
             AND cnt_google_ads >= cnt_tiktok
             AND cnt_google_ads >= cnt_kwai
             AND cnt_google_ads >= cnt_instagram
             AND cnt_google_ads >= cnt_afiliado_direto
             AND cnt_google_ads > 0 THEN 'Google Ads'
        WHEN cnt_facebook >= cnt_tiktok
             AND cnt_facebook >= cnt_kwai
             AND cnt_facebook >= cnt_instagram
             AND cnt_facebook >= cnt_afiliado_direto
             AND cnt_facebook > 0 THEN 'Facebook/Meta'
        WHEN cnt_tiktok >= cnt_kwai
             AND cnt_tiktok >= cnt_instagram
             AND cnt_tiktok >= cnt_afiliado_direto
             AND cnt_tiktok > 0 THEN 'TikTok'
        WHEN cnt_kwai >= cnt_instagram
             AND cnt_kwai >= cnt_afiliado_direto
             AND cnt_kwai > 0 THEN 'Kwai'
        WHEN cnt_instagram >= cnt_afiliado_direto
             AND cnt_instagram > 0 THEN 'Instagram'
        WHEN cnt_afiliado_direto > 0 THEN 'Afiliado Direto'
        ELSE 'Direct/Organic'
    END AS fonte_trafego,

    COALESCE(utm_source, '')        AS utm_source,
    COALESCE(utm_medium, '')        AS utm_medium,
    COALESCE(utm_campaign, '')      AS utm_campaign

FROM banner_sinais
ORDER BY affiliate_id
"""


# ======================================================================
# DDL Super Nova DB
# ======================================================================
DDL_STATEMENTS = [
    "DROP TABLE IF EXISTS multibet.dim_affiliate_source",
    """
    CREATE TABLE multibet.dim_affiliate_source (
        affiliate_id        VARCHAR(50)     NOT NULL,
        affiliate_name      VARCHAR(200),
        source_id           VARCHAR(200),
        fonte_trafego       VARCHAR(50)     NOT NULL DEFAULT 'Direct/Organic',
        utm_source          VARCHAR(200),
        utm_medium          VARCHAR(200),
        utm_campaign        VARCHAR(200),
        updated_at          TIMESTAMP       NOT NULL DEFAULT NOW(),
        PRIMARY KEY (affiliate_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_das_fonte ON multibet.dim_affiliate_source (fonte_trafego)",
]

INSERT_SQL = """
    INSERT INTO multibet.dim_affiliate_source (
        affiliate_id, affiliate_name, source_id, fonte_trafego,
        utm_source, utm_medium, utm_campaign, updated_at
    ) VALUES (
        %s, %s, %s, %s,
        %s, %s, %s, NOW()
    )
"""


def main():
    log.info("Iniciando pipeline DE-PARA de affiliates...")

    # 1. Extrair dados do Athena
    log.info("Consultando Athena (ecr_ec2.tbl_ecr_banner)...")
    df = query_athena(SQL_DE_PARA, database="ecr_ec2")
    log.info(f"Total de affiliates mapeados: {len(df)}")

    # 2. Recriar tabela no Super Nova DB
    log.info("Recriando tabela no Super Nova DB...")
    for ddl in DDL_STATEMENTS:
        execute_supernova(ddl, fetch=False)

    # 3. Batch INSERT via conexao unica
    tunnel, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            log.info(f"Inserindo {len(df)} registros (batch)...")
            rows = []
            for _, row in df.iterrows():
                rows.append((
                    str(row["affiliate_id"])[:50],
                    str(row["affiliate_name"])[:200] if row["affiliate_name"] else None,
                    str(row["source_id"])[:200] if row["source_id"] else None,
                    str(row["fonte_trafego"])[:50],
                    str(row["utm_source"])[:200] if row["utm_source"] else None,
                    str(row["utm_medium"])[:200] if row["utm_medium"] else None,
                    str(row["utm_campaign"])[:200] if row["utm_campaign"] else None,
                ))
            psycopg2.extras.execute_batch(cur, INSERT_SQL, rows, page_size=500)
            conn.commit()
            log.info(f"Batch INSERT concluido: {len(rows)} registros")
    finally:
        conn.close()
        tunnel.stop()

    # 4. Validacao pos-carga
    count_result = execute_supernova(
        "SELECT COUNT(*) FROM multibet.dim_affiliate_source", fetch=True
    )
    count_db = count_result[0][0] if count_result else 0
    log.info(f"Validacao: {count_db} registros no DB (esperado: {len(df)})")
    if count_db != len(df):
        log.warning(f"DIVERGENCIA! Athena={len(df)}, SuperNova={count_db}")

    # 5. Excel backup
    out_dir = (
        "c:/Users/NITRO/OneDrive - PGX/MultiBet/"
        "Solicitacoes Pontuais/Solicitacao de Produtos/"
        "Cruzamento de Dados e Analise/out"
    )
    os.makedirs(out_dir, exist_ok=True)
    hoje = date.today().strftime("%Y-%m-%d")
    out_file = f"{out_dir}/de_para_affiliates_FINAL_{hoje}.xlsx"

    with pd.ExcelWriter(out_file, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="DE-PARA Affiliates", index=False)
    log.info(f"Excel backup: {out_file}")

    # 6. Resumo
    print("\n=== RESUMO POR FONTE DE TRAFEGO ===")
    resumo = df.groupby("fonte_trafego").agg(
        qtd_affiliates=("affiliate_id", "count")
    ).reset_index().sort_values("qtd_affiliates", ascending=False)
    print(resumo.to_string(index=False))

    return df


if __name__ == "__main__":
    main()
