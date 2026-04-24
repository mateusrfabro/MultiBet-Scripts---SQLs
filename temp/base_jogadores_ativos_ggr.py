"""
Base de Jogadores Ativos 90d + GGR Negativo Inativo 7d (v4)
Fonte: bireports_ec2 (tbl_ecr_wise_daily_bi_summary + tbl_ecr)
Saida: Excel com 2 abas (Base 1 + Base 2)

Regras:
- So contas reais (c_ecr_status = 'real'), sem test users
- ROUND(x,2) para evitar lixo de ponto flutuante
- motivo_bloqueio consolidado com todas as flags validadas:
  AUTOEXCLUSAO, COOL_OFF, CONTA_FECHADA_RG, DEPOSITO_BLOQUEADO,
  CONTA_FRAUD, CONTA_SUSPENDED, CONTA_CLOSED, SEM_OPTIN_MARKETING, OK

Validacoes (19/03/2026):
- c_rg_self_exclusion: so 'active' bloqueia (inactive = expirou, confirmado IA Pragmatic)
- c_rg_cool_off: idem, so 'active'
- c_category: fraud/suspended/closed devem bloquear
- c_send_promotional_emails: false = nao pode disparar (regra de ouro)
- c_deposit_allowed: false = bloqueio operacional (SIGAP, fraude, chargeback)
"""
import sys
sys.path.insert(0, "c:/Users/NITRO/OneDrive - PGX/Projetos - Super Nova/MultiBet")

from db.athena import query_athena
import pandas as pd

SQL = """
WITH params AS (
    SELECT
        CAST(at_timezone(current_timestamp, 'America/Sao_Paulo') AS DATE) as today,
        CAST(at_timezone(current_timestamp, 'America/Sao_Paulo') - INTERVAL '90' DAY AS DATE) as start_90d,
        CAST(at_timezone(current_timestamp, 'America/Sao_Paulo') - INTERVAL '7' DAY AS DATE) as threshold_7d
),
daily_metrics AS (
    SELECT
        s.c_ecr_id,
        e.c_external_id,
        e.c_rg_closed,
        e.c_rg_cool_off,
        e.c_rg_self_exclusion,
        e.c_deposit_allowed,
        e.c_category,
        e.c_send_promotional_emails,
        s.c_created_date,
        (s.c_deposit_success_amount / 100.0) as deposit_real,
        (s.c_casino_realcash_bet_amount / 100.0) as casino_bet_real,
        (s.c_sb_realcash_bet_amount / 100.0) as sb_bet_real,
        ((s.c_casino_realcash_bet_amount + s.c_sb_realcash_bet_amount) -
         (s.c_casino_realcash_win_amount + s.c_sb_realcash_win_amount)) / 100.0 as ggr_real
    FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary s
    JOIN bireports_ec2.tbl_ecr e ON s.c_ecr_id = e.c_ecr_id
    CROSS JOIN params p
    WHERE s.c_created_date BETWEEN p.start_90d AND p.today
      AND e.c_test_user = false
      AND e.c_ecr_status = 'real'
),
player_aggregation AS (
    SELECT
        c_ecr_id,
        c_external_id,
        c_rg_closed,
        c_rg_cool_off,
        c_rg_self_exclusion,
        c_deposit_allowed,
        c_category,
        c_send_promotional_emails,
        ROUND(SUM(ggr_real), 2) as ggr_real_brl,
        ROUND(SUM(deposit_real), 2) as total_deposito_brl,
        ROUND(SUM(casino_bet_real + sb_bet_real), 2) as total_turnover_brl,
        MAX(CASE WHEN deposit_real > 0 OR (casino_bet_real + sb_bet_real) > 0
            THEN c_created_date END) as ultima_atividade
    FROM daily_metrics
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
)
SELECT
    a.c_external_id as user_ext_id,
    a.c_ecr_id,
    a.ggr_real_brl,
    a.ultima_atividade,
    date_diff('day', a.ultima_atividade, p.today) as dias_inativo,
    CASE
        WHEN a.ggr_real_brl < 0 AND a.ultima_atividade <= p.threshold_7d
        THEN true ELSE false
    END as flag_ggr_neg_inativo,
    -- Motivo de bloqueio (ordem de prioridade: regulatorio > operacional > marketing)
    CASE
        WHEN a.c_rg_self_exclusion = 'active'                   THEN 'AUTOEXCLUSAO'
        WHEN a.c_rg_cool_off = 'active'                         THEN 'COOL_OFF'
        WHEN a.c_rg_closed = true                                THEN 'CONTA_FECHADA_RG'
        WHEN a.c_deposit_allowed = false                         THEN 'DEPOSITO_BLOQUEADO'
        WHEN a.c_category IN ('fraud','suspended','closed')      THEN 'CONTA_' || UPPER(a.c_category)
        WHEN a.c_send_promotional_emails = false                 THEN 'SEM_OPTIN_MARKETING'
        ELSE 'OK'
    END as motivo_bloqueio
FROM player_aggregation a
CROSS JOIN params p
WHERE (a.total_deposito_brl > 0 OR a.total_turnover_brl > 0)
ORDER BY a.ggr_real_brl ASC
"""

print("Executando query no Athena (v4 - flags completas)...")
df = query_athena(SQL, database="bireports_ec2")

# Limpar residuos de ponto flutuante
df["ggr_real_brl"] = df["ggr_real_brl"].round(2)
df.loc[df["ggr_real_brl"].abs() < 0.01, "ggr_real_brl"] = 0.0

base2 = df[df["flag_ggr_neg_inativo"] == True].copy()

print(f"\n=== RESULTADO FINAL (v4) ===")
print(f"Base 1 (Ativos 90d, so real): {len(df)}")
print(f"Base 2 (GGR Neg + Inativo 7d): {len(base2)}")

# Motivos de bloqueio
print(f"\n--- Motivo bloqueio Base 1 ---")
print(df["motivo_bloqueio"].value_counts().to_string())

print(f"\n--- Motivo bloqueio Base 2 ---")
print(base2["motivo_bloqueio"].value_counts().to_string())

# Resumo
print(f"\n--- Resumo ---")
print(f"  Base 1 GGR: R$ {df['ggr_real_brl'].sum():,.2f}")
print(f"  Base 2 GGR: R$ {base2['ggr_real_brl'].sum():,.2f}")
print(f"  Base 2 Media dias inativo: {base2['dias_inativo'].mean():.1f}")

disparaveis = base2[base2["motivo_bloqueio"] == "OK"]
print(f"  Base 2 OK para campanha: {len(disparaveis)} de {len(base2)} ({len(disparaveis)/len(base2)*100:.1f}%)")

# Top 10
print(f"\n--- Top 10 Base 2 ---")
print(base2.head(10).to_string())

# Salvar Excel
output_path = "c:/Users/NITRO/OneDrive - PGX/Projetos - Super Nova/MultiBet/output/base_jogadores_ativos_ggr_v4_FINAL.xlsx"
cols = ["user_ext_id", "c_ecr_id", "ggr_real_brl",
        "ultima_atividade", "dias_inativo",
        "flag_ggr_neg_inativo", "motivo_bloqueio"]

with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
    df[cols].to_excel(writer, sheet_name="Base1_Ativos_90d", index=False)
    base2[cols].to_excel(writer, sheet_name="Base2_GGR_Neg_Inativo", index=False)

print(f"\nExcel salvo: {output_path}")
print(f"  Aba 1: {len(df)} jogadores | Aba 2: {len(base2)} jogadores")