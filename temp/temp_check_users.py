"""
Análise de campanha modelo RETEM — comparativo ANTES / DURANTE / DEPOIS.

Fontes:
  - Redshift (bireports.tbl_ecr_wise_daily_bi_summary): depósitos, GGR, APD, sessões
  - BigQuery (Smartico j_bonuses, bonus_status_id=3): BTR real (bônus creditados)

Regras:
  - DURING é dinâmico: acumula até CURRENT_DATE-1 ou campanha_end (o que vier antes)
  - BTR vem do Smartico (bônus efetivamente creditados, não apenas ofertados)
  - NGR_incremental = NGR_during - NGR_before

Uso:
    python temp_check_users.py
    from temp_check_users import analise_retem
    df = analise_retem('2026-02-01', '2026-02-07')
"""

import logging
from datetime import date, datetime, timedelta
from decimal import Decimal

import pandas as pd

from db.redshift import query_redshift
from db.bigquery import query_bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def _parse_date(d: str) -> date:
    return datetime.strptime(d, "%Y-%m-%d").date()


def analise_retem(
    campanha_start: str,
    campanha_end: str,
    dias_pos_campanha: int = 3,
) -> pd.DataFrame:
    """
    Comparativo ANTES / DURANTE / DEPOIS para campanha RETEM.

    Métricas:
      - total_users, depositos_brl, depositos_qtd
      - ggr_brl (Redshift: apostas - wins)
      - btr_brl (Smartico: bonus_cost_value com bonus_status_id = 3)
      - ngr_brl = GGR - BTR - RCA
      - ngr_incremental = NGR_during - NGR_before
      - avg_play_days (APD)
      - total_sessions (logins)

    Args:
        campanha_start:    'YYYY-MM-DD'
        campanha_end:      'YYYY-MM-DD'
        dias_pos_campanha: dias pós-campanha (padrão: 3)
    """

    # --- Datas calculadas ---
    dt_start = _parse_date(campanha_start)
    dt_end = _parse_date(campanha_end)
    ontem = date.today() - timedelta(days=1)

    # DURING dinâmico: acumula até ontem ou campanha_end (o que vier antes)
    dt_during_end = min(dt_end, ontem)

    # Se a campanha ainda não começou, não faz sentido rodar
    if dt_start > ontem:
        log.warning("Campanha ainda não iniciou (start > ontem). Abortando.")
        return pd.DataFrame()

    # AFTER só existe se a campanha já encerrou
    campanha_encerrada = dt_end < ontem
    dt_after_start = dt_end + timedelta(days=1)
    dt_after_end = dt_end + timedelta(days=dias_pos_campanha)
    # AFTER dinâmico: acumula até ontem se pós-campanha ainda em curso
    dt_after_end = min(dt_after_end, ontem)

    # Baseline M-1: mesmo intervalo no mês anterior
    # Usa DATEADD no SQL pra garantir tratamento correto de meses com dias diferentes
    baseline_start = campanha_start
    baseline_end = campanha_end

    log.info(f"DURING dinâmico: {dt_start} a {dt_during_end} (ontem={ontem})")
    if campanha_encerrada:
        log.info(f"AFTER: {dt_after_start} a {dt_after_end}")
    else:
        log.info("Campanha em andamento — AFTER não será calculado")

    # ==================================================================
    # PARTE 1: Redshift — depósitos, GGR, RCA, APD, sessões
    # ==================================================================
    sql_redshift = f"""
    SELECT * FROM (
        SELECT
            CASE
                WHEN c_created_date
                     BETWEEN DATEADD(month, -1, '{campanha_start}'::DATE)
                         AND DATEADD(month, -1, '{campanha_end}'::DATE)
                    THEN 'BEFORE'
                WHEN c_created_date
                     BETWEEN '{campanha_start}'::DATE
                         AND '{dt_during_end}'::DATE
                    THEN 'DURING'
                {"" if not campanha_encerrada else f"""
                WHEN c_created_date
                     BETWEEN '{dt_after_start}'::DATE
                         AND '{dt_after_end}'::DATE
                    THEN 'AFTER'
                """}
            END AS period,

            COUNT(DISTINCT c_ecr_id) AS total_users,

            -- Depósitos (centavos / 100 = BRL)
            SUM(c_deposit_success_amount) / 100.0 AS depositos_brl,
            SUM(c_deposit_success_count)          AS depositos_qtd,

            -- GGR = apostas - wins (todos os produtos)
            SUM(
                (COALESCE(c_casino_bet_amount, 0) - COALESCE(c_casino_win_amount, 0))
              + (COALESCE(c_sb_bet_amount, 0)     - COALESCE(c_sb_win_amount, 0))
              + (COALESCE(c_bt_bet_amount, 0)     - COALESCE(c_bt_win_amount, 0))
              + (COALESCE(c_bingo_bet_amount, 0)  - COALESCE(c_bingo_win_amount, 0))
            ) / 100.0 AS ggr_brl,

            -- RCA = royalties + jackpot contribution
            SUM(COALESCE(c_royalty_amount, 0) + COALESCE(c_jackpot_contribution_amount, 0))
            / 100.0 AS rca_brl,

            -- APD
            ROUND(
                SUM(CASE WHEN (
                    COALESCE(c_casino_bet_amount, 0) + COALESCE(c_sb_bet_amount, 0)
                  + COALESCE(c_bt_bet_amount, 0) + COALESCE(c_bingo_bet_amount, 0)
                ) > 0 THEN 1 ELSE 0 END) * 1.0
                / NULLIF(COUNT(DISTINCT c_ecr_id), 0)
            , 2) AS avg_play_days,

            -- Sessões
            SUM(c_login_count) AS total_sessions

        FROM bireports.tbl_ecr_wise_daily_bi_summary
        WHERE c_created_date
              BETWEEN DATEADD(month, -1, '{campanha_start}'::DATE)
                  AND '{dt_after_end if campanha_encerrada else dt_during_end}'::DATE
        GROUP BY 1
    ) t
    WHERE period IS NOT NULL
    ORDER BY CASE period WHEN 'BEFORE' THEN 1 WHEN 'DURING' THEN 2 WHEN 'AFTER' THEN 3 END
    """

    log.info("Consultando Redshift (depositos, GGR, RCA, APD, sessoes)...")
    df_rs = query_redshift(sql_redshift)

    # ==================================================================
    # PARTE 2: BigQuery/Smartico — BTR real (bonus_status_id = 3)
    # ==================================================================
    # Montar intervalos para cada período no BigQuery
    # Smartico usa user_ext_id, mas para BTR agregado por período não precisamos de join
    # fact_date = data do evento de bônus

    # Baseline M-1: precisamos calcular as datas no Python
    from dateutil.relativedelta import relativedelta
    bl_start = _parse_date(campanha_start) - relativedelta(months=1)
    bl_end = _parse_date(campanha_end) - relativedelta(months=1)

    periods_bq = [
        ("BEFORE", bl_start, bl_end),
        ("DURING", dt_start, dt_during_end),
    ]
    if campanha_encerrada:
        periods_bq.append(("AFTER", dt_after_start, dt_after_end))

    # Montar CASE no BigQuery
    case_lines = []
    for label, d1, d2 in periods_bq:
        case_lines.append(
            f"WHEN DATE(fact_date) BETWEEN '{d1}' AND '{d2}' THEN '{label}'"
        )
    case_sql = "\n            ".join(case_lines)

    # Menor data e maior data pra filtro
    all_dates = [d for _, d1, d2 in periods_bq for d in (d1, d2)]
    min_dt = min(all_dates)
    max_dt = max(all_dates)

    sql_bq = f"""
    SELECT
        period,
        SUM(bonus_cost_value) AS btr_brl
    FROM (
        SELECT
            CASE
                {case_sql}
            END AS period,
            bonus_cost_value
        FROM `smartico-bq6.dwh_ext_24105.j_bonuses`
        WHERE bonus_status_id = 3
          AND DATE(fact_date) BETWEEN '{min_dt}' AND '{max_dt}'
    )
    WHERE period IS NOT NULL
    GROUP BY 1
    """

    log.info("Consultando BigQuery/Smartico (BTR real, bonus_status_id=3)...")
    df_bq = query_bigquery(sql_bq)

    # ==================================================================
    # MERGE: Redshift + BigQuery
    # ==================================================================
    # Converter tipos Decimal pra float
    for col in df_rs.select_dtypes(include=["object", "number"]).columns:
        if col != "period":
            df_rs[col] = df_rs[col].apply(lambda x: float(x) if isinstance(x, Decimal) else x)

    df = df_rs.copy()

    # Merge BTR do Smartico
    btr_map = dict(zip(df_bq["period"], df_bq["btr_brl"].astype(float)))
    df["btr_brl"] = df["period"].map(btr_map).fillna(0.0)

    # NGR = GGR - BTR - RCA
    df["ngr_brl"] = df["ggr_brl"] - df["btr_brl"] - df["rca_brl"]

    # NGR incremental = NGR_during - NGR_before
    ngr_before = df.loc[df["period"] == "BEFORE", "ngr_brl"].values
    ngr_during = df.loc[df["period"] == "DURING", "ngr_brl"].values
    ngr_inc = (ngr_during[0] - ngr_before[0]) if len(ngr_before) > 0 and len(ngr_during) > 0 else None

    # Guardar como atributo para exibição
    df.attrs["ngr_incremental"] = ngr_inc

    log.info("Merge Redshift + BigQuery concluído")
    return df


def formatar_brl(valor) -> str:
    """Formata float/Decimal para R$ brasileiro."""
    try:
        v = float(valor)
        sinal = "-" if v < 0 else ""
        return f"{sinal}R$ {abs(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except (TypeError, ValueError):
        return str(valor)


if __name__ == "__main__":
    df = analise_retem("2026-02-01", "2026-02-07")

    if df.empty:
        print("Sem dados para o período informado.")
        raise SystemExit(0)

    print(f"\n{'='*80}")
    print("  ANALISE RETEM - Comparativo ANTES / DURANTE / DEPOIS")
    print("  Campanha: 01/02/2026 a 07/02/2026 | Baseline M-1: 01/01 a 07/01/2026")
    print(f"  BTR fonte: Smartico (j_bonuses, bonus_status_id=3 — efetivamente creditados)")
    print(f"{'='*80}\n")

    labels = {
        "BEFORE": "ANTES (baseline M-1: 01/01 a 07/01)",
        "DURING": "DURANTE (campanha)",
        "AFTER":  "DEPOIS (D+1 a D+3)",
    }

    for _, row in df.iterrows():
        p = row["period"]
        print(f"  --- {labels.get(p, p)} ---")
        print(f"  Usuarios unicos:  {int(row['total_users']):>10,}".replace(",", "."))
        print(f"  Depositos:        {formatar_brl(row['depositos_brl']):>18s}  ({int(row['depositos_qtd']):,} txns)".replace(",", "."))
        print(f"  GGR:              {formatar_brl(row['ggr_brl']):>18s}")
        print(f"  BTR (Smartico):   {formatar_brl(row['btr_brl']):>18s}")
        print(f"  NGR:              {formatar_brl(row['ngr_brl']):>18s}")
        print(f"  APD:              {float(row['avg_play_days']):>10.2f} dias")
        print(f"  Sessoes (logins): {int(row['total_sessions']):>10,}".replace(",", "."))
        print()

    # NGR Incremental
    ngr_inc = df.attrs.get("ngr_incremental")
    if ngr_inc is not None:
        print(f"  {'-'*50}")
        print(f"  NGR Incremental (DURING - BEFORE): {formatar_brl(ngr_inc)}")
        if ngr_inc > 0:
            ngr_before = df.loc[df["period"] == "BEFORE", "ngr_brl"].values[0]
            pct = (ngr_inc / abs(ngr_before) * 100) if ngr_before != 0 else 0
            print(f"  Variacao: +{pct:.1f}% sobre o baseline")
        print()
