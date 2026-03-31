"""
Pipeline: etl_aquisicao_trafego_diario (Athena -> Super Nova DB)
================================================================
Extrai metricas diarias de aquisicao por canal de trafego (Google, Meta, etc.)
e persiste no Super Nova DB em multibet.aquisicao_trafego_diario.

Fontes:
  - REG: bireports_ec2.tbl_ecr (c_sign_up_time BRT, c_test_user=false)
  - FTD: bireports_ec2.tbl_ecr (REGs base) + ps_bi.dim_user (ftd_datetime same-day)
    NOTA: FTD = same-day conversion (registrou E depositou no mesmo dia).
    Nao usar ps_bi.dim_user sozinho — tem registros inflados para alguns affiliates.
  - Financeiro: bireports_ec2.tbl_ecr_wise_daily_bi_summary (centavos/100)
  - Canal: affiliate_id mapeado via CHANNEL_MAP

Regras CLAUDE.md aplicadas:
  - Timezone: AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'
  - GGR: somente realcash (sub-fund isolation)
  - Test users excluidos
  - affiliate_id VARCHAR no ps_bi
  - Valores bireports: centavos / 100.0

Estrategia: DELETE periodo + INSERT (idempotente por faixa de datas)

Execucao:
    python pipelines/etl_aquisicao_trafego_diario.py              # D-1
    python pipelines/etl_aquisicao_trafego_diario.py --days 7     # ultimos 7 dias
    python pipelines/etl_aquisicao_trafego_diario.py --days 30    # carga historica

Agendamento sugerido: rodar diariamente apos meia-noite (BRT) com --days 2
para garantir D-1 completo e cobrir eventuais reprocessamentos.
"""

import sys
import os
import logging
import argparse
from datetime import date, timedelta, datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.athena import query_athena
from db.supernova import execute_supernova, get_supernova_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("etl_aquisicao_trafego")

# =====================================================================
# MAPEAMENTO DE CANAIS — affiliate_ids por canal
# Mesmos IDs do config.py do dashboard + futuros canais
# =====================================================================
CHANNEL_MAP = {
    "Google": ["297657", "445431", "468114"],
    "Meta": ["532570", "532571", "464673"],
}

# Todos os IDs mapeados (para queries consolidadas)
ALL_MAPPED_IDS = []
for _ids in CHANNEL_MAP.values():
    ALL_MAPPED_IDS.extend(_ids)

# =====================================================================
# DDL — executada automaticamente na primeira rodada
# =====================================================================
DDL = """
CREATE TABLE IF NOT EXISTS multibet.aquisicao_trafego_diario (
    -- Dimensoes
    dt                  DATE         NOT NULL,
    channel             VARCHAR(30)  NOT NULL,

    -- Funil de aquisicao
    registros           INTEGER      DEFAULT 0,
    ftd_count           INTEGER      DEFAULT 0,
    ftd_amount          NUMERIC(14,2) DEFAULT 0,
    conv_reg_ftd_pct    NUMERIC(5,1) DEFAULT 0,
    ftd_ticket_medio    NUMERIC(14,2) DEFAULT 0,

    -- Financeiro
    depositos_amount    NUMERIC(14,2) DEFAULT 0,
    saques_amount       NUMERIC(14,2) DEFAULT 0,
    net_deposit         NUMERIC(14,2) DEFAULT 0,

    -- Receita
    ggr_casino          NUMERIC(14,2) DEFAULT 0,
    ggr_sport           NUMERIC(14,2) DEFAULT 0,
    bonus_cost          NUMERIC(14,2) DEFAULT 0,
    ngr                 NUMERIC(14,2) DEFAULT 0,

    -- Engajamento
    players_ativos      INTEGER      DEFAULT 0,

    -- Metadata
    refreshed_at        TIMESTAMPTZ  DEFAULT NOW(),

    -- Constraint: 1 linha por dia por canal
    CONSTRAINT pk_aquisicao_trafego PRIMARY KEY (dt, channel)
);

-- Indices para queries comuns do front
CREATE INDEX IF NOT EXISTS idx_aqt_dt ON multibet.aquisicao_trafego_diario (dt);
CREATE INDEX IF NOT EXISTS idx_aqt_channel ON multibet.aquisicao_trafego_diario (channel);

COMMENT ON TABLE multibet.aquisicao_trafego_diario IS
    'Metricas diarias de aquisicao por canal de trafego pago. '
    'Alimentado pelo ETL etl_aquisicao_trafego_diario.py (Athena -> Super Nova DB). '
    'Consumido pelo front db.supernovagaming.com.br aba Aquisicao Trafego.';
"""

# =====================================================================
# VIEW — consumida pelo front (mesmo padrao matriz_financeiro_*)
# Agrega por dia com linha de total e colunas formatadas
# =====================================================================
DDL_VIEW = """
CREATE OR REPLACE VIEW multibet.vw_aquisicao_trafego AS
SELECT
    dt                                          AS data,
    channel                                     AS canal,
    registros                                   AS cadastros,
    ftd_count                                   AS ftd,
    ftd_amount                                  AS ftd_amount,
    conv_reg_ftd_pct                            AS conversao_ftd_pct,
    ftd_ticket_medio                            AS ticket_medio_ftd,
    depositos_amount                            AS deposito,
    saques_amount                               AS saque,
    net_deposit,
    ggr_casino,
    ggr_sport,
    ggr_casino + ggr_sport                      AS ggr_total,
    bonus_cost,
    ngr,
    players_ativos,
    refreshed_at
FROM multibet.aquisicao_trafego_diario
ORDER BY dt DESC, channel;

COMMENT ON VIEW multibet.vw_aquisicao_trafego IS
    'View de consumo do front — aba Aquisicao Trafego. '
    'Dados da tabela aquisicao_trafego_diario. '
    'Filtrar por canal e data no front. Colunas em portugues para o front.';
"""


def ensure_table():
    """Cria tabela, indices e view se nao existirem."""
    log.info("Verificando/criando tabela multibet.aquisicao_trafego_diario...")
    execute_supernova(DDL)
    log.info("Tabela OK")
    log.info("Criando/atualizando view multibet.vw_aquisicao_trafego...")
    execute_supernova(DDL_VIEW)
    log.info("View OK")


# =====================================================================
# QUERIES ATHENA — por canal, por dia
# =====================================================================
def _aff_sql(ids: list) -> str:
    """Gera filtro SQL: ('id1', 'id2', ...)"""
    return "(" + ", ".join(f"'{i}'" for i in ids) + ")"


def query_reg(dt: str, aff: str) -> str:
    """REG: cadastros no dia (BRT). Fonte: bireports_ec2.tbl_ecr."""
    return f"""
    SELECT COUNT(*) AS reg
    FROM bireports_ec2.tbl_ecr
    WHERE CAST(c_sign_up_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '{dt}'
      AND CAST(c_affiliate_id AS VARCHAR) IN {aff}
      AND c_test_user = false
    """


def query_ftd(dt: str, aff: str) -> str:
    """FTD = same-day conversion: registrou no dia E fez primeiro deposito no mesmo dia.
    IMPORTANTE: usar bireports_ec2 como base de REGs (sem inflacao) e dim_user
    apenas para checar ftd_datetime. ps_bi.dim_user tem registros inflados para
    alguns affiliates (ex: 297657 mostra 1.8x) — nunca usar sozinho para FTD.
    Fontes: bireports_ec2.tbl_ecr (REGs) + ps_bi.dim_user (ftd_datetime)."""
    return f"""
    WITH regs_do_dia AS (
        SELECT c_ecr_id
        FROM bireports_ec2.tbl_ecr
        WHERE CAST(c_sign_up_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '{dt}'
          AND CAST(c_affiliate_id AS VARCHAR) IN {aff}
          AND c_test_user = false
    )
    SELECT
        COUNT(*) AS ftd,
        COALESCE(SUM(u.ftd_amount_inhouse), 0) AS ftd_amount
    FROM regs_do_dia r
    JOIN ps_bi.dim_user u ON r.c_ecr_id = u.ecr_id
    WHERE CAST(u.ftd_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '{dt}'
    """


def query_financeiro(dt: str, aff: str) -> str:
    """Financeiro + GGR + Players Ativos. Fonte: bireports_ec2 (centavos/100)."""
    return f"""
    WITH base_players AS (
        SELECT DISTINCT ecr_id
        FROM ps_bi.dim_user
        WHERE CAST(affiliate_id AS VARCHAR) IN {aff}
          AND is_test = false
    )
    SELECT
        COALESCE(SUM(s.c_deposit_success_amount), 0) / 100.0         AS dep_amount,
        COALESCE(SUM(s.c_co_success_amount), 0) / 100.0              AS saques,
        COALESCE(SUM(s.c_casino_realcash_bet_amount
                    - s.c_casino_realcash_win_amount), 0) / 100.0     AS ggr_casino,
        COALESCE(SUM(s.c_sb_realcash_bet_amount
                    - s.c_sb_realcash_win_amount), 0) / 100.0         AS ggr_sport,
        COALESCE(SUM(s.c_bonus_issued_amount), 0) / 100.0            AS bonus_cost,
        COUNT(DISTINCT s.c_ecr_id)                                    AS players_ativos
    FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary s
    JOIN base_players p ON s.c_ecr_id = p.ecr_id
    WHERE s.c_created_date = DATE '{dt}'
    """


# =====================================================================
# EXTRACAO + PERSISTENCIA
# =====================================================================
def extract_channel_day(channel: str, ids: list, dt: str) -> dict:
    """Extrai todas as metricas de um canal para um dia."""
    aff = _aff_sql(ids)

    log.info(f"  Extraindo {channel} | {dt} | affiliates: {ids}")

    # REG
    df_reg = query_athena(query_reg(dt, aff), database="bireports_ec2")
    reg = int(df_reg.iloc[0]["reg"])

    # FTD (cross-database: bireports_ec2 + ps_bi)
    df_ftd = query_athena(query_ftd(dt, aff), database="bireports_ec2")
    ftd = int(df_ftd.iloc[0]["ftd"])
    ftd_amount = float(df_ftd.iloc[0]["ftd_amount"])

    # Financeiro
    df_fin = query_athena(query_financeiro(dt, aff), database="ps_bi")
    r = df_fin.iloc[0]
    dep = float(r["dep_amount"])
    saq = float(r["saques"])
    ggr_c = float(r["ggr_casino"])
    ggr_s = float(r["ggr_sport"])
    bonus = float(r["bonus_cost"])
    players = int(r["players_ativos"])

    # Calculados
    conv_pct = round(ftd / max(reg, 1) * 100, 1)
    ticket_medio = round(ftd_amount / max(ftd, 1), 2)
    ngr = round(ggr_c + ggr_s - bonus, 2)
    net_dep = round(dep - saq, 2)

    return {
        "dt": dt,
        "channel": channel,
        "registros": reg,
        "ftd_count": ftd,
        "ftd_amount": round(ftd_amount, 2),
        "conv_reg_ftd_pct": conv_pct,
        "ftd_ticket_medio": ticket_medio,
        "depositos_amount": round(dep, 2),
        "saques_amount": round(saq, 2),
        "net_deposit": net_dep,
        "ggr_casino": round(ggr_c, 2),
        "ggr_sport": round(ggr_s, 2),
        "bonus_cost": round(bonus, 2),
        "ngr": ngr,
        "players_ativos": players,
    }


def persist_rows(rows: list):
    """
    Persiste lista de dicts no Super Nova DB.
    Estrategia: DELETE por (dt, channel) + INSERT (idempotente).
    """
    if not rows:
        log.warning("Nenhuma linha para persistir")
        return

    tunnel, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            for row in rows:
                # DELETE existente (idempotente)
                cur.execute(
                    "DELETE FROM multibet.aquisicao_trafego_diario WHERE dt = %s AND channel = %s",
                    (row["dt"], row["channel"]),
                )
                # INSERT novo
                cur.execute("""
                    INSERT INTO multibet.aquisicao_trafego_diario
                        (dt, channel, registros, ftd_count, ftd_amount, conv_reg_ftd_pct,
                         ftd_ticket_medio, depositos_amount, saques_amount, net_deposit,
                         ggr_casino, ggr_sport, bonus_cost, ngr, players_ativos, refreshed_at)
                    VALUES
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                """, (
                    row["dt"], row["channel"], row["registros"], row["ftd_count"],
                    row["ftd_amount"], row["conv_reg_ftd_pct"], row["ftd_ticket_medio"],
                    row["depositos_amount"], row["saques_amount"], row["net_deposit"],
                    row["ggr_casino"], row["ggr_sport"], row["bonus_cost"],
                    row["ngr"], row["players_ativos"],
                ))
            conn.commit()
            log.info(f"Persistidas {len(rows)} linhas no Super Nova DB")
    finally:
        conn.close()
        tunnel.stop()


# =====================================================================
# MAIN
# =====================================================================
def run(days: int = 1, include_today: bool = True):
    """
    Executa ETL para os ultimos N dias (D-1 ate D-N) + hoje (parcial).

    Args:
        days: Quantidade de dias retroativos (D-1 ate D-N)
        include_today: Se True, inclui o dia atual (dados parciais)
    """
    ensure_table()

    end_date = date.today() - timedelta(days=1)  # D-1 (dia fechado)
    start_date = end_date - timedelta(days=days - 1)

    # Incluir hoje (parcial) como ultimo dia
    final_date = date.today() if include_today else end_date

    log.info(f"ETL Aquisicao Trafego: {start_date} a {final_date} ({days} dias + hoje={'sim' if include_today else 'nao'})")
    log.info(f"Canais: {list(CHANNEL_MAP.keys())}")

    all_rows = []
    current = start_date
    while current <= final_date:
        dt_str = current.isoformat()
        is_today = current == date.today()

        # Extrair por canal
        for channel, ids in CHANNEL_MAP.items():
            try:
                row = extract_channel_day(channel, ids, dt_str)
                all_rows.append(row)
                parcial = " (PARCIAL)" if is_today else ""
                log.info(
                    f"  {channel} {dt_str}{parcial}: REG={row['registros']} FTD={row['ftd_count']} "
                    f"NGR=R${row['ngr']:,.2f} Players={row['players_ativos']}"
                )
            except Exception as e:
                log.error(f"  ERRO {channel} {dt_str}: {e}", exc_info=True)

        # Extrair consolidado (todos os canais, COUNT DISTINCT real — sem duplicar players)
        try:
            row_all = extract_channel_day("Consolidado", ALL_MAPPED_IDS, dt_str)
            all_rows.append(row_all)
            parcial = " (PARCIAL)" if is_today else ""
            log.info(
                f"  Consolidado {dt_str}{parcial}: REG={row_all['registros']} FTD={row_all['ftd_count']} "
                f"NGR=R${row_all['ngr']:,.2f} Players={row_all['players_ativos']}"
            )
        except Exception as e:
            log.error(f"  ERRO Consolidado {dt_str}: {e}", exc_info=True)

        current += timedelta(days=1)

    # Persistir tudo
    persist_rows(all_rows)

    # Resumo
    log.info("=" * 60)
    log.info(f"ETL concluido: {len(all_rows)} linhas processadas")
    for row in all_rows:
        parcial = " *" if row["dt"] == date.today().isoformat() else ""
        log.info(
            f"  {row['dt']}{parcial} | {row['channel']:>8} | "
            f"REG={row['registros']:>5} FTD={row['ftd_count']:>4} "
            f"Conv={row['conv_reg_ftd_pct']:>5.1f}% "
            f"NGR=R${row['ngr']:>12,.2f}"
        )
    if include_today:
        log.info("  * = dados parciais (dia em andamento)")
    log.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Aquisicao Trafego Diario")
    parser.add_argument(
        "--days", type=int, default=1,
        help="Quantidade de dias retroativos (default: 1 = apenas D-1)"
    )
    parser.add_argument(
        "--no-today", action="store_true",
        help="Nao incluir dia atual (parcial)"
    )
    args = parser.parse_args()
    run(days=args.days, include_today=not args.no_today)