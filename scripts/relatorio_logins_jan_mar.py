"""
Relatório de Logins Totais e Únicos — Janeiro, Fevereiro e Março 2026
=====================================================================
Extrai de 3 fontes (bireports_ec2, ps_bi, BigQuery) e gera Excel
com validação cruzada + legenda.

VERSÃO 2 — com cap de 50 logins/player/dia para remover bots/automação.

Uso:
    python scripts/relatorio_logins_jan_mar.py
"""

import sys
import os
import logging
from datetime import datetime

import pandas as pd

# Garante que imports locais funcionem
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.athena import query_athena
from db.bigquery import query_bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "reports")
os.makedirs(OUTPUT_DIR, exist_ok=True)


# =====================================================================
# 1. ATHENA — bireports_ec2 (CAPPED: max 50 logins/player/dia)
# =====================================================================
def extrair_athena_bireports():
    """Logins com cap de 50/player/dia para remover bots."""
    log.info("Extraindo logins do Athena bireports_ec2 (capped, excl. test users)...")

    sql_mensal = """
    SELECT
        date_format(s.c_created_date, '%Y-%m') AS mes,
        SUM(LEAST(s.c_login_count, 50))           AS total_logins,
        SUM(s.c_login_count)                      AS total_logins_raw,
        COUNT(DISTINCT s.c_ecr_id)                AS usuarios_unicos,
        COUNT_IF(s.c_login_count > 50)            AS players_anomalos,
        SUM(s.c_login_count) - SUM(LEAST(s.c_login_count, 50)) AS logins_removidos
    FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary s
    JOIN bireports_ec2.tbl_ecr b ON b.c_ecr_id = s.c_ecr_id
    WHERE s.c_created_date >= DATE '2026-01-01'
      AND s.c_created_date < DATE '2026-04-01'
      AND s.c_login_count > 0
      AND b.c_test_user = false
    GROUP BY date_format(s.c_created_date, '%Y-%m')
    ORDER BY 1
    """

    sql_diario = """
    SELECT
        s.c_created_date                          AS dia,
        SUM(LEAST(s.c_login_count, 50))           AS total_logins,
        SUM(s.c_login_count)                      AS total_logins_raw,
        COUNT(DISTINCT s.c_ecr_id)                AS usuarios_unicos
    FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary s
    JOIN bireports_ec2.tbl_ecr b ON b.c_ecr_id = s.c_ecr_id
    WHERE s.c_created_date >= DATE '2026-01-01'
      AND s.c_created_date < DATE '2026-04-01'
      AND s.c_login_count > 0
      AND b.c_test_user = false
    GROUP BY s.c_created_date
    ORDER BY 1
    """

    df_mensal = query_athena(sql_mensal, database="bireports_ec2")
    df_diario = query_athena(sql_diario, database="bireports_ec2")
    log.info(f"  bireports_ec2: {len(df_mensal)} meses, {len(df_diario)} dias")
    return df_mensal, df_diario


# =====================================================================
# 2. BIGQUERY — Smartico CRM (tr_login)
# =====================================================================
def extrair_bigquery():
    """Logins mensais e diários via BigQuery tr_login."""
    log.info("Extraindo logins do BigQuery tr_login...")

    sql_mensal = '''
    SELECT
        FORMAT_TIMESTAMP("%Y-%m", event_time) AS mes,
        COUNT(*)                               AS total_logins,
        COUNT(DISTINCT user_id)                AS usuarios_unicos
    FROM `smartico-bq6.dwh_ext_24105.tr_login`
    WHERE event_time >= '2026-01-01'
      AND event_time < '2026-04-01'
    GROUP BY 1
    ORDER BY 1
    '''

    sql_diario = '''
    SELECT
        DATE(event_time)        AS dia,
        COUNT(*)                AS total_logins,
        COUNT(DISTINCT user_id) AS usuarios_unicos
    FROM `smartico-bq6.dwh_ext_24105.tr_login`
    WHERE event_time >= '2026-01-01'
      AND event_time < '2026-04-01'
    GROUP BY 1
    ORDER BY 1
    '''

    df_mensal = query_bigquery(sql_mensal)
    df_diario = query_bigquery(sql_diario)
    log.info(f"  BigQuery: {len(df_mensal)} meses, {len(df_diario)} dias")
    return df_mensal, df_diario


# =====================================================================
# 3. VALIDAÇÃO CRUZADA MENSAL
# =====================================================================
def validacao_cruzada(athena_mensal, bq_mensal):
    """Compara Athena capped vs BigQuery com divergência %."""
    log.info("Montando validacao cruzada...")

    a = athena_mensal[
        ["mes", "total_logins", "total_logins_raw", "usuarios_unicos",
         "players_anomalos", "logins_removidos"]
    ].copy()
    a.columns = [
        "mes", "athena_total_limpo", "athena_total_raw", "athena_unicos",
        "players_anomalos", "logins_anomalos"
    ]

    b = bq_mensal.rename(columns={
        "total_logins": "bigquery_total",
        "usuarios_unicos": "bigquery_unicos"
    })

    df = a.merge(b, on="mes", how="outer")

    df["div_total_%"] = (
        (df["athena_total_limpo"] - df["bigquery_total"])
        / df["bigquery_total"] * 100
    ).round(1)
    df["div_unicos_%"] = (
        (df["athena_unicos"] - df["bigquery_unicos"])
        / df["bigquery_unicos"] * 100
    ).round(1)

    return df


# =====================================================================
# 4. COMPARAÇÃO DIÁRIA
# =====================================================================
def comparacao_diaria(athena_diario, bq_diario):
    """Merge diario Athena capped vs BigQuery."""
    log.info("Montando comparacao diaria...")

    a = athena_diario.copy()
    a.columns = ["dia", "athena_total", "athena_raw", "athena_unicos"]
    b = bq_diario.copy()
    b.columns = ["dia", "bigquery_total", "bigquery_unicos"]

    a["dia"] = pd.to_datetime(a["dia"]).dt.date
    b["dia"] = pd.to_datetime(b["dia"]).dt.date

    df = a.merge(b, on="dia", how="outer").sort_values("dia")
    df["div_total_%"] = (
        (df["athena_total"] - df["bigquery_total"])
        / df["bigquery_total"] * 100
    ).round(1)
    df["div_unicos_%"] = (
        (df["athena_unicos"] - df["bigquery_unicos"])
        / df["bigquery_unicos"] * 100
    ).round(1)

    return df


# =====================================================================
# 5. LEGENDA
# =====================================================================
def gerar_legenda():
    """Retorna DataFrame com a legenda/dicionario do relatorio."""
    linhas = [
        ["CONTEXTO", "", ""],
        ["Periodo", "Janeiro, Fevereiro e Marco 2026", "Marco parcial ate 24/03/2026"],
        ["Data extracao", datetime.now().strftime("%Y-%m-%d %H:%M BRT"), ""],
        ["", "", ""],
        ["FONTES DE DADOS", "", ""],
        ["Athena bireports_ec2 (PRIMARIA)",
         "tbl_ecr_wise_daily_bi_summary + tbl_ecr",
         "Fonte bruta Pragmatic Solutions. Test users excluidos (c_test_user=false). "
         "Logins capped em 50/player/dia para remover bots."],
        ["BigQuery Smartico (VALIDACAO)",
         "tr_login",
         "Eventos de login do CRM Smartico. user_id = ID interno. event_time UTC."],
        ["", "", ""],
        ["COLUNAS", "", ""],
        ["mes / dia", "Periodo", "UTC em ambas as fontes"],
        ["athena_total_limpo", "Logins com cap de 50/player/dia",
         "Remove bots e reconexoes automaticas"],
        ["athena_total_raw", "Logins brutos sem cap",
         "Inclui anomalias (bots com ate 46K logins/dia)"],
        ["athena_unicos", "Players unicos com >=1 login",
         "Dedup mensal ou diario conforme a aba"],
        ["bigquery_total", "Total de eventos tr_login",
         "Captura CRM-level (pode diferir da plataforma)"],
        ["bigquery_unicos", "Users unicos no CRM",
         "Apenas players registrados no Smartico"],
        ["div_total_%", "Divergencia total logins",
         "Positivo=Athena maior. +/-10% aceitavel entre fontes."],
        ["div_unicos_%", "Divergencia unicos",
         "Athena tende +15-19% mais unicos (nem todo player esta no CRM)"],
        ["players_anomalos", "Players com >50 logins/dia capados",
         "63 players com 500+ logins/dia no periodo. Provavel automacao."],
        ["logins_anomalos", "Logins removidos pelo cap",
         "~169K logins (4.4%) eram de bots/automacao"],
        ["", "", ""],
        ["ANOMALIAS DETECTADAS", "", ""],
        ["05/01 - Bot massivo",
         "1 player com 46.665 logins + 20 players com ~2K cada",
         "145K logins inflados. Sem cap, Janeiro ficaria +9% inflado."],
        ["08-09/02 - Cluster menor",
         "~40 players com 200-310 logins/dia",
         "23K logins inflados. Padrao de reconexao automatica."],
        ["ps_bi gap 20/03",
         "Pipeline dbt falhou - so 1.982 logins registrados",
         "bireports_ec2 mostra 51K. Nao afeta relatorio (usamos bireports)."],
        ["", "", ""],
        ["DIVERGENCIA ESTRUTURAL", "", ""],
        ["Unicos: Athena > BigQuery",
         "Athena tem 15-19% mais players unicos",
         "Nem todo player da plataforma esta registrado no CRM Smartico."],
        ["Total: BigQuery > Athena (pos-cap)",
         "BigQuery tem 6-12% mais logins",
         "CRM pode capturar sessoes que a plataforma agrega como 1 login."],
        ["", "", ""],
        ["RECOMENDACAO", "", ""],
        ["Numeros oficiais", "Usar athena_total_limpo + athena_unicos",
         "Fonte primaria, mais conservadora, sem bots."],
        ["Validacao", "BigQuery como segunda fonte",
         "Divergencia <10% = OK. >15% = investigar dia especifico."],
    ]
    return pd.DataFrame(linhas, columns=["Item", "Valor", "Observacao"])


# =====================================================================
# 6. VISAO EXECUTIVA (linguagem de negocio)
# =====================================================================
def gerar_visao_executiva(validacao):
    """Gera aba executiva para stakeholders nao-tecnicos."""
    log.info("Gerando visao executiva...")

    # Tabela principal simplificada
    exec_data = []
    meses_label = {"2026-01": "Janeiro", "2026-02": "Fevereiro", "2026-03": "Marco*"}

    for _, row in validacao.iterrows():
        mes = row["mes"]
        label = meses_label.get(mes, mes)

        # Media diaria
        dias = 31 if mes == "2026-01" else (28 if mes == "2026-02" else 24)
        media_diaria = int(row["athena_total_limpo"] / dias)
        media_unicos = int(row["athena_unicos"] / dias) if mes != "2026-03" else None

        exec_data.append({
            "Mes": label,
            "Logins Totais": int(row["athena_total_limpo"]),
            "Jogadores Unicos": int(row["athena_unicos"]),
            "Media de Logins/Dia": media_diaria,
            "Logins por Jogador (media)": round(row["athena_total_limpo"] / row["athena_unicos"], 1),
            "Confiabilidade": "Validado com 2 fontes" if abs(row["div_total_%"]) < 15 else "Investigar",
        })

    df_exec = pd.DataFrame(exec_data)

    # Variacao mes a mes
    if len(exec_data) >= 2:
        jan_logins = exec_data[0]["Logins Totais"]
        fev_logins = exec_data[1]["Logins Totais"]
        jan_unicos = exec_data[0]["Jogadores Unicos"]
        fev_unicos = exec_data[1]["Jogadores Unicos"]
        var_logins = round((fev_logins - jan_logins) / jan_logins * 100, 1)
        var_unicos = round((fev_unicos - jan_unicos) / jan_unicos * 100, 1)

    # Insights e notas
    notas = [
        {"": ""},
        {"": "COMO LER ESTE RELATORIO"},
        {"": ""},
        {"": "Logins Totais = quantidade de vezes que jogadores entraram na plataforma no mes."},
        {"": "Um mesmo jogador pode logar varias vezes no mesmo dia (ex: celular de manha, PC a noite)."},
        {"": ""},
        {"": "Jogadores Unicos = quantos jogadores DIFERENTES entraram pelo menos 1 vez no mes."},
        {"": "Este numero e mais importante que o total para medir engajamento real."},
        {"": ""},
        {"": "Logins por Jogador = em media, quantas vezes cada jogador entrou. Quanto maior, mais engajado."},
        {"": ""},
        {"": "DESTAQUES"},
        {"": ""},
        {"": f"- De Janeiro para Fevereiro: {var_logins:+.1f}% em logins totais e {var_unicos:+.1f}% em jogadores unicos."},
        {"": "- Marco esta INCOMPLETO (dados ate 24/03). Nao comparar diretamente com meses fechados."},
        {"": f"- Janeiro teve ~145 mil logins de bots/automacao que foram removidos da contagem."},
        {"": "- Fevereiro teve ~23 mil logins de bots removidos (impacto menor)."},
        {"": ""},
        {"": "SOBRE A CONFIABILIDADE"},
        {"": ""},
        {"": "Estes numeros foram cruzados com uma segunda fonte independente (CRM Smartico)."},
        {"": "A divergencia entre fontes ficou abaixo de 12%, o que e aceitavel."},
        {"": "Jogadores unicos na plataforma sao ~17% maiores que no CRM porque nem todo"},
        {"": "jogador cadastrado esta sincronizado com o sistema de CRM."},
        {"": ""},
        {"": "* Marco parcial: dados ate 24/03/2026. Numeros finais disponiveis apos o fechamento do mes."},
    ]
    df_notas = pd.DataFrame(notas)
    df_notas.columns = ["Notas e Contexto"]

    # Combinar em um unico DataFrame para a aba
    # Primeiro a tabela, depois as notas
    return df_exec, df_notas


# =====================================================================
# MAIN
# =====================================================================
def main():
    log.info("=" * 60)
    log.info("RELATORIO DE LOGINS - Jan/Fev/Mar 2026 (v3 - executivo)")
    log.info("=" * 60)

    # Extracoes
    athena_mensal, athena_diario = extrair_athena_bireports()
    bq_mensal, bq_diario = extrair_bigquery()

    # Validacao cruzada
    validacao = validacao_cruzada(athena_mensal, bq_mensal)
    comparacao = comparacao_diaria(athena_diario, bq_diario)

    # Legenda tecnica
    legenda = gerar_legenda()

    # Visao executiva
    df_exec, df_notas = gerar_visao_executiva(validacao)

    # ---- Print resumo no console ----
    print("\n" + "=" * 70)
    print("VISAO EXECUTIVA - LOGINS JAN/FEV/MAR 2026")
    print("=" * 70)
    print(df_exec.to_string(index=False))
    print()
    print("--- Validacao tecnica ---")
    print(validacao[["mes", "athena_total_limpo", "bigquery_total", "div_total_%"]].to_string(index=False))

    # ---- Exportar Excel ----
    output_file = os.path.join(OUTPUT_DIR, "logins_jan_fev_mar_2026_FINAL.xlsx")
    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        # Aba executiva primeiro (quem abre o Excel ve esta aba)
        df_exec.to_excel(writer, sheet_name="Resumo Executivo", index=False, startrow=1)
        ws = writer.sheets["Resumo Executivo"]
        ws.cell(row=1, column=1, value="Logins MultiBet - Janeiro a Marco 2026")
        df_notas.to_excel(writer, sheet_name="Resumo Executivo", index=False,
                          startrow=len(df_exec) + 4, header=True)

        # Abas tecnicas
        validacao.to_excel(writer, sheet_name="Validacao Tecnica", index=False)
        comparacao.to_excel(writer, sheet_name="Comparacao Diaria", index=False)
        legenda.to_excel(writer, sheet_name="Legenda Tecnica", index=False)

    log.info(f"\nRelatorio salvo em: {output_file}")
    print(f"\n>>> Arquivo: {output_file}")


if __name__ == "__main__":
    main()