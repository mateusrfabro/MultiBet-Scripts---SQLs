"""
Solicitação: FTDs de março/2026 do afiliado 297657
Solicitante: Gestor de tráfego
Objetivo: Listar jogadores que fizeram FTD em março (até hoje)
          vindos do affiliate_id 297657 ou tracker com esse ID.
"""

import sys
sys.path.insert(0, "c:/Users/NITRO/OneDrive - PGX/MultiBet")

from db.redshift import query_redshift
import os
from datetime import date

sql = """
-- FTDs de março/2026 vinculados ao afiliado 297657
WITH
ftd_marco AS (
    SELECT
        c_ecr_id AS player_id,
        CONVERT_TIMEZONE('UTC', 'America/Sao_Paulo', c_conversion_time) AS data_ftd
    FROM ecr.tbl_ecr_conversion_info
    WHERE CONVERT_TIMEZONE('UTC', 'America/Sao_Paulo', c_conversion_time) >= '2026-03-01'
      AND CONVERT_TIMEZONE('UTC', 'America/Sao_Paulo', c_conversion_time) < '2026-03-16'
),
dados_ecr AS (
    SELECT
        c_ecr_id   AS player_id,
        c_email_id AS email,
        c_affiliate_id AS affiliate_id,
        c_tracker_id   AS tracker_id
    FROM ecr.tbl_ecr
),
pii AS (
    SELECT
        c_ecr_id AS player_id,
        TRIM(c_fname || ' ' || c_lname) AS nome_completo,
        '+' || REPLACE(COALESCE(c_phone_isd_code::VARCHAR, ''), '+', '')
            || COALESCE(c_mobile_number::VARCHAR, '') AS telefone
    FROM ecr.tbl_ecr_profile
)
SELECT
    p.nome_completo AS "Nome",
    f.player_id     AS "ID Jogador",
    p.telefone      AS "Telefone",
    e.email         AS "Email"
FROM ftd_marco f
INNER JOIN dados_ecr e ON e.player_id = f.player_id
INNER JOIN pii p       ON p.player_id = f.player_id
WHERE e.affiliate_id = '297657'
   OR REGEXP_INSTR(CAST(e.tracker_id AS VARCHAR), '(^|)297657(|$)') > 0
ORDER BY p.nome_completo;
"""

print("Consultando Redshift...")
df = query_redshift(sql)
print(f"Resultado: {len(df)} jogadores encontrados.")

out_dir = "c:/Users/NITRO/OneDrive - PGX/MultiBet/Solicitações Pontuais/Solicitação de Produtos/Cruzamento de Dados e Análise/out"
os.makedirs(out_dir, exist_ok=True)

hoje = date.today().strftime("%Y-%m-%d")
out_file = f"{out_dir}/ftd_marco_affiliate_297657_{hoje}.xlsx"
df.to_excel(out_file, index=False, sheet_name="FTDs Março")
print(f"Arquivo salvo em: {out_file}")