"""
Relatorio: Tempo de Resgate de Bonus (Time-to-BTR) — Jan a 17/Abr/2026.

Objetivo: clusterizar bonus emitidos por tempo ate o resgate (BTR - bonus virou
real cash), consolidado por mes de emissao.

Definicoes (validadas com CRM + Risco + Estatistica + validacao empirica 17/04):
- Resgate = bonus atingiu wagering e foi convertido em real cash.
  Status: c_bonus_status = 'BONUS_ISSUED_OFFER' na bonus_ec2.
  Validado empiricamente: 1.961 BONUS_ISSUED_OFFER criados em 20/03 vs
  1.878 txns type 20 CR na sub_fund no mesmo dia (match 96%).
- Tempo contado: c_updated_time (transicao p/ inactive = BTR efetivo)
  menos c_created_time (emissao).
  Campos descartados na investigacao:
    * c_issue_date (do summary): igual a created na maioria, NAO e BTR
    * c_win_issue_timestamp: zerado (1970-01-01) na base MultiBet
    * c_claimed_date: sempre NULL
- Cohort: mes de c_created_time (emissao do bonus).
- Buckets: 0-1d, 1-3d, 3-7d, 7-14d, 14-30d, 30d+, ativo, expirado, dropped.

Fontes:
- bonus_ec2.tbl_ecr_bonus_details        (ativos — c_bonus_status = 'BONUS_OFFER')
- bonus_ec2.tbl_ecr_bonus_details_inactive (inativos — resgatou/expirou/dropped)
- bonus_ec2.tbl_bonus_summary_details    (valor em c_actual_issued_amount, c_issue_date)
- ps_bi.dim_user                         (is_test para filtrar QA)

Filtros:
- is_test = false (exclui contas de teste)
- Janeiro/2026 a 17/Abril/2026

Segmentacoes:
- Total geral (mes x bucket)
- Freebet vs cash bonus (c_is_freebet)
- Top 10 campanhas (c_bonus_id)

Saidas:
- reports/tempo_resgate_bonus_FINAL.html
- reports/tempo_resgate_bonus_raw_<timestamp>.csv  (base granular por bonus)

Nota metodologica (estatistica):
- Abril/2026 e cohort parcial (janela de observacao <30d) — marcado no relatorio.
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from db.athena import query_athena

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DATA_INICIO = "2026-01-01"
DATA_FIM    = "2026-04-18"   # exclusivo — captura ate 17/04 23:59:59 BRT
HOJE_STR    = "2026-04-17"

REPORTS_DIR = Path(__file__).resolve().parents[1] / "reports"
REPORTS_DIR.mkdir(exist_ok=True)


# ---------------------------------------------------------------------------
# 1) EXTRACAO
# ---------------------------------------------------------------------------
SQL_BASE = f"""
WITH bonus_unificado AS (
    -- Bonus INATIVOS tem prioridade: representam o estado FINAL do bonus.
    -- Se um mesmo c_ecr_bonus_id aparece em ambas (race condition rara — ~0.004%),
    -- o inativo vence. Logica: pegamos ativos que NAO estao no inativo.
    SELECT
        b.c_ecr_bonus_id,
        b.c_ecr_id,
        b.c_bonus_id,
        b.c_bonus_status,
        b.c_is_freebet,
        b.c_created_time,
        b.c_bonus_expired_date,
        b.c_updated_time AS c_updated_time_inactive,
        'INATIVO' AS fonte_tabela
    FROM bonus_ec2.tbl_ecr_bonus_details_inactive b
    WHERE b.c_created_time >= TIMESTAMP '{DATA_INICIO}'
      AND b.c_created_time <  TIMESTAMP '{DATA_FIM}'

    UNION ALL

    SELECT
        b.c_ecr_bonus_id,
        b.c_ecr_id,
        b.c_bonus_id,
        b.c_bonus_status,
        b.c_is_freebet,
        b.c_created_time,
        b.c_bonus_expired_date,
        CAST(NULL AS TIMESTAMP(3)) AS c_updated_time_inactive,
        'ATIVO' AS fonte_tabela
    FROM bonus_ec2.tbl_ecr_bonus_details b
    WHERE b.c_created_time >= TIMESTAMP '{DATA_INICIO}'
      AND b.c_created_time <  TIMESTAMP '{DATA_FIM}'
      AND b.c_ecr_bonus_id NOT IN (
          SELECT c_ecr_bonus_id
          FROM bonus_ec2.tbl_ecr_bonus_details_inactive
          WHERE c_created_time >= TIMESTAMP '{DATA_INICIO}'
            AND c_created_time <  TIMESTAMP '{DATA_FIM}'
      )
),
com_valor AS (
    SELECT
        u.c_ecr_bonus_id,
        u.c_ecr_id,
        u.c_bonus_id,
        u.c_bonus_status,
        u.c_is_freebet,
        u.fonte_tabela,
        -- Timestamps em BRT
        (u.c_created_time            AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS emissao_brt,
        -- Resgate BTR: c_updated_time do INATIVO e o momento da transicao p/ BONUS_ISSUED_OFFER.
        -- Para bonus ainda ativos, updated nao representa BTR → NULL.
        CASE
            WHEN u.fonte_tabela = 'INATIVO' AND u.c_bonus_status = 'BONUS_ISSUED_OFFER'
            THEN u.c_updated_time_inactive AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'
        END AS resgate_brt,
        (u.c_bonus_expired_date AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS expira_brt,
        -- Valor emitido (apenas se resgatou)
        COALESCE(s.c_actual_issued_amount, 0) / 100.0 AS valor_emitido_brl
    FROM bonus_unificado u
    LEFT JOIN bonus_ec2.tbl_bonus_summary_details s
      ON u.c_ecr_bonus_id = s.c_ecr_bonus_id
)
SELECT
    c.c_ecr_bonus_id,
    c.c_ecr_id,
    c.c_bonus_id,
    c.c_bonus_status,
    c.c_is_freebet,
    c.fonte_tabela,
    c.emissao_brt,
    c.resgate_brt,
    c.expira_brt,
    c.valor_emitido_brl,
    -- tempo em horas e dias entre emissao e resgate
    CASE
        WHEN c.c_bonus_status = 'BONUS_ISSUED_OFFER' AND c.resgate_brt IS NOT NULL
        THEN date_diff('second', c.emissao_brt, c.resgate_brt) / 3600.0
    END AS horas_ate_resgate,
    CASE
        WHEN c.c_bonus_status = 'BONUS_ISSUED_OFFER' AND c.resgate_brt IS NOT NULL
        THEN date_diff('second', c.emissao_brt, c.resgate_brt) / 86400.0
    END AS dias_ate_resgate,
    du.is_test
FROM com_valor c
LEFT JOIN ps_bi.dim_user du
  ON c.c_ecr_id = du.ecr_id
WHERE COALESCE(du.is_test, false) = false
"""


# ---------------------------------------------------------------------------
# 2) TRATAMENTO
# ---------------------------------------------------------------------------
BUCKETS_ORDEM = [
    "<1min", "1-5min", "5-60min", "1-6h", "6-24h",
    "1-3d", "3-7d", "7-14d", "14-30d", "30d+",
    "ATIVO", "EXPIROU", "DROPPED"
]

def classificar_bucket(row) -> str:
    """
    Buckets hibridos: sub-diario para onde estao 99.6% dos resgates
    (validado empiricamente: 37% instantaneo, 44% 1-5min, 14% 5-60min),
    diario para os casos longos (wagering real), mais estados finais.
    """
    status = row["c_bonus_status"]
    if status == "BONUS_ISSUED_OFFER":
        h = row["horas_ate_resgate"]
        if pd.isna(h):
            return "EXPIROU"  # status resgatou sem timestamp de atualizacao — raro
        segundos = h * 3600
        if segundos <  60:        return "<1min"
        if segundos <  300:       return "1-5min"
        if segundos <  3600:      return "5-60min"
        if segundos <  21600:     return "1-6h"
        if segundos <  86400:     return "6-24h"
        if segundos <  259200:    return "1-3d"
        if segundos <  604800:    return "3-7d"
        if segundos < 1209600:    return "7-14d"
        if segundos < 2592000:    return "14-30d"
        return "30d+"
    if status == "BONUS_OFFER":
        return "ATIVO"
    if status == "EXPIRED":
        return "EXPIROU"
    if status == "DROPPED":
        return "DROPPED"
    return "DROPPED"


def extrair() -> pd.DataFrame:
    log.info("Extraindo dados do Athena (periodo: %s → %s)...", DATA_INICIO, DATA_FIM)
    df = query_athena(SQL_BASE)
    log.info("  %s bonus brutos carregados.", f"{len(df):,}")
    return df


def enriquecer(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["emissao_brt"] = pd.to_datetime(df["emissao_brt"], utc=False)
    df["resgate_brt"] = pd.to_datetime(df["resgate_brt"], utc=False)
    df["mes_emissao"] = df["emissao_brt"].dt.strftime("%Y-%m")
    df["bucket"] = df.apply(classificar_bucket, axis=1)
    df["resgatou"] = df["c_bonus_status"] == "BONUS_ISSUED_OFFER"
    df["tipo_bonus"] = df["c_is_freebet"].map(
        {True: "Freebet/Freespin", False: "Cash Bonus"}
    ).fillna("Cash Bonus")
    return df


# ---------------------------------------------------------------------------
# 3) AGREGACOES
# ---------------------------------------------------------------------------
def matriz_mes_bucket(df: pd.DataFrame) -> pd.DataFrame:
    tabela = pd.crosstab(df["mes_emissao"], df["bucket"]).reindex(columns=BUCKETS_ORDEM, fill_value=0)
    tabela["Total"] = tabela.sum(axis=1)
    return tabela


def matriz_mes_bucket_pct(df: pd.DataFrame) -> pd.DataFrame:
    tabela = matriz_mes_bucket(df)
    pct = tabela.drop(columns="Total").div(tabela["Total"], axis=0) * 100
    pct = pct.round(1)
    pct["Total"] = tabela["Total"]
    return pct


def resumo_conversao(df: pd.DataFrame) -> pd.DataFrame:
    g = df.groupby("mes_emissao").agg(
        emitidos=("c_ecr_bonus_id", "count"),
        resgatados=("resgatou", "sum"),
        valor_resgatado_brl=("valor_emitido_brl", "sum"),
        mediana_horas_ate_resgate=(
            "horas_ate_resgate",
            lambda s: s.dropna().median() if s.notna().any() else None,
        ),
        mediana_dias_ate_resgate=(
            "dias_ate_resgate",
            lambda s: s.dropna().median() if s.notna().any() else None,
        ),
    )
    g["taxa_resgate_pct"] = (g["resgatados"] / g["emitidos"] * 100).round(1)
    g["ticket_medio_brl"] = (g["valor_resgatado_brl"] / g["resgatados"]).round(2)
    g["mediana_horas_ate_resgate"] = g["mediana_horas_ate_resgate"].round(2)
    g["mediana_dias_ate_resgate"] = g["mediana_dias_ate_resgate"].round(2)
    return g


def matriz_por_tipo(df: pd.DataFrame) -> pd.DataFrame:
    pv = pd.crosstab(
        [df["mes_emissao"], df["tipo_bonus"]],
        df["bucket"],
    ).reindex(columns=BUCKETS_ORDEM, fill_value=0)
    pv["Total"] = pv.sum(axis=1)
    return pv


def top_campanhas(df: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    top = (
        df.groupby("c_bonus_id")
          .agg(
              emitidos=("c_ecr_bonus_id", "count"),
              resgatados=("resgatou", "sum"),
              valor_brl=("valor_emitido_brl", "sum"),
              mediana_horas=("horas_ate_resgate", lambda s: s.dropna().median()),
          )
          .sort_values("emitidos", ascending=False)
          .head(n)
    )
    top["taxa_resgate_pct"] = (top["resgatados"] / top["emitidos"] * 100).round(1)
    top["mediana_horas"] = top["mediana_horas"].round(1)
    return top


def flags_risco(df: pd.DataFrame) -> dict:
    """Flags agregadas de risco sob otica antifraude."""
    resgatados = df[df["resgatou"]]
    total_r = len(resgatados)
    if total_r == 0:
        return {"pct_btr_sub_1h": 0, "pct_btr_sub_24h": 0, "serial_fast_redeemers": 0}

    sub_1h  = (resgatados["horas_ate_resgate"] < 1).sum()
    sub_24h = (resgatados["horas_ate_resgate"] < 24).sum()

    # serial fast redeemers: jogadores com >=3 bonus resgatados em <24h
    sf = (
        resgatados[resgatados["horas_ate_resgate"] < 24]
          .groupby("c_ecr_id").size()
    )
    serial = int((sf >= 3).sum())

    return {
        "pct_btr_sub_1h":  round(sub_1h  / total_r * 100, 1),
        "pct_btr_sub_24h": round(sub_24h / total_r * 100, 1),
        "serial_fast_redeemers": serial,
        "total_resgates": total_r,
    }


# ---------------------------------------------------------------------------
# 4) RENDER HTML
# ---------------------------------------------------------------------------
def render_html(
    df: pd.DataFrame,
    resumo: pd.DataFrame,
    matriz_qtd: pd.DataFrame,
    matriz_pct: pd.DataFrame,
    por_tipo: pd.DataFrame,
    top_camp: pd.DataFrame,
    flags: dict,
) -> str:

    meses_completos = [m for m in matriz_qtd.index if m != "2026-04"]
    total_bonus = len(df)
    total_resgatados = int(df["resgatou"].sum())
    taxa_global = total_resgatados / total_bonus * 100 if total_bonus else 0
    valor_total = df["valor_emitido_brl"].sum()

    mediana_geral_h = df[df["resgatou"]]["horas_ate_resgate"].dropna().median()
    mediana_geral_d = df[df["resgatou"]]["dias_ate_resgate"].dropna().median()

    def fmt_int(n):  return f"{int(n):,}".replace(",", ".")
    def fmt_brl(v):  return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    def fmt_pct(p):  return f"{p:.1f}%"
    def fmt_tempo_mediana(horas):
        if pd.isna(horas):
            return "—"
        segundos = horas * 3600
        if segundos <  60:    return f"~{segundos:.0f}s"
        if segundos <  3600:  return f"~{segundos/60:.1f} min"
        if segundos <  86400: return f"~{horas:.1f}h"
        return f"~{horas/24:.1f}d"

    # ---------- Tabelas HTML ----------
    def tabela_matriz_html(mat: pd.DataFrame, is_pct: bool = False) -> str:
        cls = "tabela-dados"
        rows = []
        for idx, row in mat.iterrows():
            label = idx if not isinstance(idx, tuple) else " / ".join(str(x) for x in idx)
            is_parcial = "2026-04" in str(label)
            flag = " <span class='tag-parcial'>parcial</span>" if is_parcial else ""
            cells = []
            for col in mat.columns:
                v = row[col]
                if col == "Total":
                    cells.append(f"<td class='col-total'>{fmt_int(v)}</td>")
                elif is_pct:
                    cells.append(f"<td>{v:.1f}%</td>" if v != 0 else "<td class='zero'>–</td>")
                else:
                    cells.append(f"<td>{fmt_int(v)}</td>" if v != 0 else "<td class='zero'>–</td>")
            rows.append(f"<tr><th>{label}{flag}</th>{''.join(cells)}</tr>")
        headers = "".join(f"<th>{c}</th>" for c in mat.columns)
        return (
            f"<table class='{cls}'><thead><tr><th>Mes emissao</th>{headers}</tr></thead>"
            f"<tbody>{''.join(rows)}</tbody></table>"
        )

    resumo_html_rows = []
    for mes, row in resumo.iterrows():
        flag = " <span class='tag-parcial'>parcial</span>" if mes == "2026-04" else ""
        resumo_html_rows.append(
            f"<tr><th>{mes}{flag}</th>"
            f"<td>{fmt_int(row['emitidos'])}</td>"
            f"<td>{fmt_int(row['resgatados'])}</td>"
            f"<td>{fmt_pct(row['taxa_resgate_pct'])}</td>"
            f"<td>{fmt_brl(row['valor_resgatado_brl'])}</td>"
            f"<td>{fmt_brl(row['ticket_medio_brl']) if pd.notna(row['ticket_medio_brl']) else '—'}</td>"
            f"<td>{fmt_tempo_mediana(row['mediana_horas_ate_resgate'])}</td>"
            f"</tr>"
        )
    resumo_html = (
        "<table class='tabela-dados'><thead><tr>"
        "<th>Mes emissao</th><th>Bonus emitidos</th><th>Resgatados</th>"
        "<th>Taxa resgate</th><th>Valor BTR</th><th>Ticket medio</th>"
        "<th>Mediana tempo resgate</th>"
        "</tr></thead><tbody>"
        + "".join(resumo_html_rows) +
        "</tbody></table>"
    )

    # Top campanhas HTML
    top_rows = []
    for bid, row in top_camp.iterrows():
        top_rows.append(
            f"<tr><td class='mono'>{bid}</td>"
            f"<td>{fmt_int(row['emitidos'])}</td>"
            f"<td>{fmt_int(row['resgatados'])}</td>"
            f"<td>{fmt_pct(row['taxa_resgate_pct'])}</td>"
            f"<td>{fmt_brl(row['valor_brl'])}</td>"
            f"<td>{row['mediana_horas']:.1f}h</td></tr>"
        )
    top_html = (
        "<table class='tabela-dados'><thead><tr>"
        "<th>ID campanha (c_bonus_id)</th><th>Emitidos</th><th>Resgatados</th>"
        "<th>Taxa</th><th>Valor BTR</th><th>Mediana h</th>"
        "</tr></thead><tbody>" + "".join(top_rows) + "</tbody></table>"
    )

    matriz_qtd_html = tabela_matriz_html(matriz_qtd, is_pct=False)
    matriz_pct_html = tabela_matriz_html(matriz_pct, is_pct=True)
    por_tipo_html   = tabela_matriz_html(por_tipo, is_pct=False)

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<title>Tempo de Resgate de Bonus — MultiBet</title>
<style>
  :root {{
    --azul:#1e3a8a; --azul-claro:#3b82f6; --bg:#f8fafc; --cinza:#64748b;
    --verde:#059669; --laranja:#d97706; --vermelho:#dc2626;
  }}
  * {{ box-sizing: border-box; }}
  body {{ font-family: -apple-system, "Segoe UI", Roboto, Arial, sans-serif;
         margin:0; background:var(--bg); color:#0f172a; line-height:1.5; }}
  header {{ background:linear-gradient(135deg, var(--azul), var(--azul-claro));
           color:#fff; padding:32px 48px; }}
  header h1 {{ margin:0 0 8px 0; font-size:28px; }}
  header p  {{ margin:0; opacity:0.9; font-size:14px; }}
  main {{ max-width:1280px; margin:0 auto; padding:32px 24px; }}
  section {{ background:#fff; border-radius:12px; padding:24px 32px; margin-bottom:24px;
             box-shadow: 0 1px 3px rgba(0,0,0,0.05), 0 1px 2px rgba(0,0,0,0.06); }}
  section h2 {{ color:var(--azul); margin-top:0; border-bottom:2px solid #e2e8f0; padding-bottom:12px; }}
  .kpis {{ display:grid; grid-template-columns:repeat(auto-fit, minmax(200px, 1fr)); gap:16px; margin:16px 0 24px 0; }}
  .kpi  {{ background:#f1f5f9; border-left:4px solid var(--azul); padding:16px 20px; border-radius:6px; }}
  .kpi .label {{ font-size:12px; color:var(--cinza); text-transform:uppercase; letter-spacing:0.5px; }}
  .kpi .valor {{ font-size:22px; font-weight:700; color:var(--azul); margin-top:6px; }}
  .kpi.risco {{ border-left-color:var(--laranja); }}
  .kpi.risco .valor {{ color:var(--laranja); }}
  table.tabela-dados {{ width:100%; border-collapse:collapse; font-size:13px; margin-top:12px; }}
  table.tabela-dados th {{ background:#e2e8f0; color:#1e293b; text-align:left; padding:10px 12px;
                           border-bottom:2px solid #cbd5e1; font-weight:600; }}
  table.tabela-dados td {{ padding:8px 12px; border-bottom:1px solid #f1f5f9; }}
  table.tabela-dados th:first-child {{ background:#cbd5e1; }}
  .col-total {{ background:#f1f5f9; font-weight:700; }}
  .zero {{ color:#cbd5e1; text-align:center; }}
  .tag-parcial {{ background:var(--laranja); color:#fff; padding:2px 8px; border-radius:4px;
                  font-size:10px; margin-left:6px; font-weight:700; }}
  .mono {{ font-family: "SF Mono", Consolas, monospace; font-size:12px; }}
  .callout {{ background:#fef3c7; border-left:4px solid var(--laranja); padding:16px 20px;
              border-radius:6px; margin-top:16px; }}
  .callout strong {{ color:var(--laranja); }}
  .legenda {{ background:#f8fafc; border:1px solid #e2e8f0; padding:20px 24px; border-radius:8px;
              font-size:13px; line-height:1.7; }}
  .legenda dt {{ font-weight:600; color:var(--azul); margin-top:8px; }}
  .legenda dd {{ margin-left:16px; margin-bottom:8px; color:#334155; }}
  footer {{ text-align:center; color:var(--cinza); font-size:12px; padding:24px; }}
</style>
</head>
<body>
<header>
  <h1>Tempo de Resgate de Bonus (BTR) — MultiBet</h1>
  <p>Cohort por mes de emissao · Janeiro a 17/Abril/2026 · Fonte: Athena (bonus_ec2 + ps_bi)</p>
</header>
<main>

<section>
  <h2>Como ler este relatorio</h2>
  <div class="legenda">
    <dl>
      <dt>BTR (Bonus Turned Real)</dt>
      <dd>Bonus que bateu o wagering exigido e foi convertido em dinheiro real sacavel.
          Equivale ao status <code>BONUS_ISSUED_OFFER</code> na tabela <code>bonus_ec2.tbl_ecr_bonus_details_inactive</code>.</dd>
      <dt>Mes de emissao (cohort)</dt>
      <dd>Agrupamento pela data que o bonus foi emitido ao jogador (<code>c_created_time</code> em BRT).
          Um bonus emitido em janeiro teve 3+ meses de observacao; um emitido em abril tem menos tempo (cohort parcial).</dd>
      <dt>Bucket de tempo (hibrido)</dt>
      <dd>Intervalo entre emissao e resgate. A investigacao mostrou que <strong>99,6% dos bonus do MultiBet
          sao resgatados em menos de 24h</strong> — logo os buckets diarios originais (0-1d/1-3d/...) jogariam quase tudo num bucket so.
          Solucao: sub-diario onde ha sinal (<strong>&lt;1min</strong> · <strong>1-5min</strong> · <strong>5-60min</strong> ·
          <strong>1-6h</strong> · <strong>6-24h</strong>) + diario para casos raros (<strong>1-3d</strong> · <strong>3-7d</strong> ·
          <strong>7-14d</strong> · <strong>14-30d</strong> · <strong>30d+</strong>).</dd>
      <dt>Leitura dos buckets sub-diarios</dt>
      <dd><strong>&lt;1min e 1-5min:</strong> cashback/freespin com conversao automatica (sem wagering real).
          <strong>5-60min e 1-6h:</strong> wagering curto / bonus usado rapido. <strong>6-24h+:</strong> wagering que exigiu tempo real.</dd>
      <dt>ATIVO / EXPIROU / DROPPED</dt>
      <dd><strong>ATIVO</strong>: bonus ainda na base, pode resgatar. <strong>EXPIROU</strong>: chegou no c_bonus_expired_date sem bater wagering.
          <strong>DROPPED</strong>: cancelado pelo sistema/backoffice (ex.: abuso, solicitacao).</dd>
      <dt>Cohort parcial (abril/2026)</dt>
      <dd>Marcado com <span class="tag-parcial">parcial</span>. Como a janela de observacao e curta (&lt;30 dias),
          a taxa de resgate desse mes NAO e comparavel diretamente com os demais. Tende a subestimar o resgate.</dd>
      <dt>Freebet vs Cash Bonus</dt>
      <dd>Freebet/freespin (<code>c_is_freebet=true</code>) tem logica diferente: ganho vira bonus convertivel. Tempos tendem a ser mais curtos.</dd>
    </dl>
  </div>
</section>

<section>
  <h2>Resumo executivo</h2>
  <div class="kpis">
    <div class="kpi"><div class="label">Bonus emitidos</div><div class="valor">{fmt_int(total_bonus)}</div></div>
    <div class="kpi"><div class="label">Bonus resgatados (BTR)</div><div class="valor">{fmt_int(total_resgatados)}</div></div>
    <div class="kpi"><div class="label">Taxa global de resgate</div><div class="valor">{fmt_pct(taxa_global)}</div></div>
    <div class="kpi"><div class="label">Valor BTR total</div><div class="valor">{fmt_brl(valor_total)}</div></div>
    <div class="kpi"><div class="label">Mediana tempo resgate</div><div class="valor">{mediana_geral_h:.1f}h</div></div>
    <div class="kpi risco"><div class="label">Resgate em &lt; 1h (risco)</div><div class="valor">{flags['pct_btr_sub_1h']}%</div></div>
    <div class="kpi risco"><div class="label">Resgate em &lt; 24h</div><div class="valor">{flags['pct_btr_sub_24h']}%</div></div>
    <div class="kpi risco"><div class="label">Serial fast redeemers (≥3 BTR &lt;24h)</div><div class="valor">{fmt_int(flags['serial_fast_redeemers'])}</div></div>
  </div>
  <div class="callout">
    <strong>Insight principal:</strong> 99,6% dos bonus sao resgatados em menos de 24 horas
    (mediana {fmt_tempo_mediana(mediana_geral_h)}). Isso confirma que a operacao hoje e dominada por
    <strong>bonus automaticos / de conversao rapida</strong> (cashback, freespin com auto-credit,
    reload). Wagering que exige dias de jogo e pratica residual.
    <br><br>
    <strong>Janeiro vs. fevereiro:</strong> houve queda de <strong>65% no volume emitido</strong>
    (299K -> 107K bonus). Nao e bug de dados — foi mudanca operacional.
    Top 5 campanhas de janeiro (<code>202511010377237</code>, <code>202511010377122</code>,
    <code>202511010377564</code>, <code>2025123107372339</code>, <code>2026010616453785</code>)
    somaram 160K bonus sozinhas. Algumas dessas campanhas foram encerradas / reduzidas em fev.
    Verificar no backoffice Pragmatic o que mudou.
    <br><br>
    <strong>Sob otica antifraude:</strong> resgate em &lt;1h = {flags['pct_btr_sub_1h']}% dos BTRs.
    Nao e necessariamente abuso — grande parte e cashback nativamente instantaneo —, mas
    jogadores com 3+ resgates em &lt;24h ({fmt_int(flags['serial_fast_redeemers'])} identificados)
    sao candidatos a auditoria (possivel multi-account / bonus hunter).
    <br><br>
    <strong>Observacao estatistica:</strong> abril/2026 e cohort parcial (observacao ate 17/04).
    Como a maioria dos resgates ocorre em horas, o impacto da janela incompleta e pequeno —
    mas os poucos bonus de 14-30d emitidos em abril ainda nao teriam tido tempo de resgatar.
  </div>
</section>

<section>
  <h2>Consolidado por mes — Indicadores chave</h2>
  {resumo_html}
</section>

<section>
  <h2>Matriz: Mes × Bucket (quantidade de bonus)</h2>
  {matriz_qtd_html}
</section>

<section>
  <h2>Matriz: Mes × Bucket (% do total do mes)</h2>
  <p style="color:var(--cinza); font-size:13px;">
    Leia cada linha como 100% — mostra a distribuicao dos bonus daquele mes entre buckets de tempo e estados finais.
  </p>
  {matriz_pct_html}
</section>

<section>
  <h2>Quebra por tipo: Cash Bonus vs Freebet/Freespin</h2>
  {por_tipo_html}
</section>

<section>
  <h2>Top 10 campanhas por volume (c_bonus_id)</h2>
  <p style="color:var(--cinza); font-size:13px;">
    Campanhas com maior volume de emissao no periodo. Use o ID para cruzar com o backoffice
    Pragmatic e identificar a campanha (ex.: bonus de boas-vindas, cashback, reload).
  </p>
  {top_html}
</section>

<section>
  <h2>Metodologia</h2>
  <div class="legenda">
    <p><strong>Fonte:</strong> AWS Athena (Iceberg), sufixo <code>_ec2</code>. Extracao em {HOJE_STR}.</p>
    <p><strong>Tabelas:</strong> <code>bonus_ec2.tbl_ecr_bonus_details</code> + <code>bonus_ec2.tbl_ecr_bonus_details_inactive</code>
       (union) + <code>bonus_ec2.tbl_bonus_summary_details</code> (valor e data de issue) + <code>ps_bi.dim_user</code> (filtro test).</p>
    <p><strong>Filtros:</strong> <code>is_test = false</code>; emissao entre {DATA_INICIO} e {HOJE_STR} (inclusivo).</p>
    <p><strong>Tempo de resgate:</strong> <code>c_updated_time − c_created_time</code> em horas/dias. Considera-se "resgatado" apenas bonus com <code>c_bonus_status = 'BONUS_ISSUED_OFFER'</code> na tabela <code>_inactive</code> (timestamp da transicao para inativo). Campos <code>c_win_issue_timestamp</code>, <code>c_claimed_date</code> e <code>c_issue_date</code> foram testados e descartados (zerados/nulos/duplicam created).</p>
    <p><strong>Fuso:</strong> UTC convertido para America/Sao_Paulo em toda exibicao.</p>
    <p><strong>Flags de risco:</strong> resgate sub-1h (bonus hunter classico), sub-24h, e serial fast redeemers (≥3 BTR em &lt;24h — possivel multi-account/bot).</p>
    <p><strong>Validacao cruzada (17/04/2026):</strong> valor BTR deste relatorio foi cruzado contra
    <code>fund_ec2.tbl_realcash_sub_fund_txn</code> (type 20 CR), fonte usada no relatorio de Distribuicao de Bonus Mar/2026.
    Fev/26: R$ 1.369.471 em ambas as metodologias (match 100%, centavo a centavo).
    Marco/26: delta de 0,6% (R$ 9.643 em R$ 1,51M). Verificado tambem que 99,9% dos BTRs em um mes vem de bonus
    emitidos no MESMO mes — so 0,2% do valor vem de cohorts anteriores.</p>
    <p><strong>Limitacoes:</strong> (a) schemas Pragmatic nao tem classificacao nativa welcome/reload/cashback — segmentacao por tipo limitada a freebet vs cash; (b) cohort de abril/2026 e parcial; (c) bonus DROPPED podem incluir cancelamentos legitimos (ex.: alteracao de regra CRM).</p>
    <p><strong>Input metodologico:</strong> CRM (escopo BTR + buckets), Risco (flags antifraude), Estatistica (cohort por emissao + marcacao parcial).</p>
  </div>
</section>

</main>
<footer>
  Relatorio gerado em {datetime.now().strftime('%d/%m/%Y %H:%M')} · MultiBet · Squad Intelligence Engine
</footer>
</body>
</html>
"""
    return html


# ---------------------------------------------------------------------------
# 5) MAIN
# ---------------------------------------------------------------------------
def main():
    log.info("=" * 70)
    log.info("  REPORT TEMPO DE RESGATE DE BONUS")
    log.info("=" * 70)

    df_raw = extrair()
    df = enriquecer(df_raw)
    log.info("  Enriquecido: %s linhas | resgatados: %s (%.1f%%)",
             f"{len(df):,}",
             f"{df['resgatou'].sum():,}",
             df['resgatou'].mean() * 100)

    log.info("Calculando agregados...")
    resumo     = resumo_conversao(df)
    matriz_q   = matriz_mes_bucket(df)
    matriz_p   = matriz_mes_bucket_pct(df)
    por_tipo   = matriz_por_tipo(df)
    top_camp   = top_campanhas(df, 10)
    flags      = flags_risco(df)

    log.info("Resumo por mes:\n%s", resumo.to_string())
    log.info("Flags risco: %s", flags)

    log.info("Renderizando HTML...")
    html = render_html(df, resumo, matriz_q, matriz_p, por_tipo, top_camp, flags)

    ts = datetime.now().strftime("%Y%m%d_%H%M")
    html_path = REPORTS_DIR / "tempo_resgate_bonus_FINAL.html"
    csv_path  = REPORTS_DIR / f"tempo_resgate_bonus_raw_{ts}.csv"

    html_path.write_text(html, encoding="utf-8")
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")

    log.info("  HTML: %s", html_path)
    log.info("  CSV raw: %s", csv_path)
    log.info("=" * 70)
    log.info("Pronto. Abra o HTML pra revisar.")


if __name__ == "__main__":
    main()
