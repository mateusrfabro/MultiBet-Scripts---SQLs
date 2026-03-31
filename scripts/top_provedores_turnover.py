"""
Top 10 Provedores por Turnover + Top 5 Jogos por Provedor
=========================================================
Periodo: 01/01/2026 a 25/03/2026

Fontes:
  - PRIMARIA: bireports_ec2.tbl_ecr_txn_type_wise_daily_game_play_summary
    (dados completos, 26 vendors, valores em centavos /100)
  - JOGOS:   ps_bi.fct_casino_activity_daily + tbl_vendor_games_mapping_data
    (game-level detail com nomes dos jogos)
  - VALIDACAO: ps_bi.fct_casino_activity_daily (via dim_game — parcial)

Entrega: Excel com 4 abas:
  1. Top 10 Provedores
  2. Top 5 Jogos por Provedor
  3. Validacao Cruzada
  4. Legenda
"""

import sys
import os
import io
import logging
import warnings
from datetime import datetime

# Fix encoding Windows
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import pandas as pd

warnings.filterwarnings("ignore")

# Adiciona raiz do projeto ao path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.athena import query_athena

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# -- Parametros --
DATA_INICIO = "2026-01-01"
DATA_FIM = "2026-03-25"
OUTPUT_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "reports",
    f"top_provedores_turnover_{DATA_INICIO.replace('-','')}_{DATA_FIM.replace('-','')}_FINAL.xlsx",
)


def query_top_provedores_bireports():
    """
    FONTE PRIMARIA (bireports_ec2) - Top 10 provedores por turnover.
    Usa tbl_ecr_txn_type_wise_daily_game_play_summary (dados completos).
    Valores em centavos -> divide por 100.
    c_txn_type = 27 (Bet). Exclui test users.
    Calcula GGR = Bets - Wins (tipo 27 - tipo 45).
    """
    sql = f"""
    WITH bets AS (
        SELECT
            s.c_vendor_id                         AS provedor,
            COUNT(DISTINCT s.c_ecr_id)            AS jogadores_unicos,
            SUM(s.c_txn_count)                    AS total_apostas,
            SUM(s.c_txn_real_cash_amount_ecr_crncy) / 100.0  AS turnover_real_brl,
            SUM(
                COALESCE(s.c_txn_crp_amount_ecr_crncy, 0)
                + COALESCE(s.c_txn_drp_amount_ecr_crncy, 0)
                + COALESCE(s.c_txn_wrp_amount_ecr_crncy, 0)
                + COALESCE(s.c_txn_rrp_amount_ecr_crncy, 0)
            ) / 100.0                             AS turnover_bonus_brl
        FROM bireports_ec2.tbl_ecr_txn_type_wise_daily_game_play_summary s
        JOIN bireports_ec2.tbl_ecr e ON s.c_ecr_id = e.c_ecr_id
        WHERE s.c_created_date BETWEEN DATE '{DATA_INICIO}' AND DATE '{DATA_FIM}'
          AND s.c_txn_type = 27
          AND e.c_test_user = false
        GROUP BY s.c_vendor_id
    ),
    wins AS (
        SELECT
            s.c_vendor_id                         AS provedor,
            SUM(s.c_txn_real_cash_amount_ecr_crncy) / 100.0  AS wins_real_brl,
            SUM(
                COALESCE(s.c_txn_crp_amount_ecr_crncy, 0)
                + COALESCE(s.c_txn_drp_amount_ecr_crncy, 0)
                + COALESCE(s.c_txn_wrp_amount_ecr_crncy, 0)
                + COALESCE(s.c_txn_rrp_amount_ecr_crncy, 0)
            ) / 100.0                             AS wins_bonus_brl
        FROM bireports_ec2.tbl_ecr_txn_type_wise_daily_game_play_summary s
        JOIN bireports_ec2.tbl_ecr e ON s.c_ecr_id = e.c_ecr_id
        WHERE s.c_created_date BETWEEN DATE '{DATA_INICIO}' AND DATE '{DATA_FIM}'
          AND s.c_txn_type = 45
          AND e.c_test_user = false
        GROUP BY s.c_vendor_id
    )
    SELECT
        b.provedor,
        b.jogadores_unicos,
        b.total_apostas,
        (b.turnover_real_brl + b.turnover_bonus_brl)  AS turnover_total_brl,
        b.turnover_real_brl,
        b.turnover_bonus_brl,
        COALESCE(w.wins_real_brl, 0) + COALESCE(w.wins_bonus_brl, 0) AS wins_total_brl,
        COALESCE(w.wins_real_brl, 0)                  AS wins_real_brl,
        -- GGR = Bets Real - Wins Real
        b.turnover_real_brl - COALESCE(w.wins_real_brl, 0)  AS ggr_brl,
        -- Hold Rate = GGR / Turnover Real
        CASE
            WHEN b.turnover_real_brl > 0
            THEN (b.turnover_real_brl - COALESCE(w.wins_real_brl, 0))
                 / b.turnover_real_brl * 100
            ELSE 0
        END                                       AS hold_rate_pct
    FROM bets b
    LEFT JOIN wins w ON b.provedor = w.provedor
    ORDER BY turnover_total_brl DESC
    LIMIT 10
    """
    log.info("Executando: Top 10 provedores (bireports_ec2)...")
    df = query_athena(sql, database="bireports_ec2")
    log.info(f"  -> {len(df)} provedores retornados")
    return df


def query_top_jogos_por_provedor(provedores: list):
    """
    Top 5 jogos por provedor usando ps_bi.fct_casino_activity_daily
    + bireports_ec2.tbl_vendor_games_mapping_data para nomes dos jogos.

    Faz LEFT JOIN com o catalogo de jogos para pegar game_desc e vendor_id.
    """
    provedor_list = ", ".join([f"'{p}'" for p in provedores])

    sql = f"""
    WITH game_data AS (
        SELECT
            m.c_vendor_id                    AS provedor,
            m.c_game_desc                    AS jogo,
            CAST(f.game_id AS VARCHAR)       AS game_id,
            m.c_game_type_desc               AS tipo_jogo,
            COUNT(DISTINCT f.player_id)      AS jogadores_unicos,
            SUM(f.bet_count)                 AS total_apostas,
            SUM(f.bet_amount_local)          AS turnover_total_brl,
            SUM(f.real_bet_amount_local)     AS turnover_real_brl,
            SUM(f.win_amount_local)          AS wins_total_brl,
            SUM(f.ggr_local)                 AS ggr_brl,
            CASE
                WHEN SUM(f.real_bet_amount_local) > 0
                THEN SUM(f.ggr_local) / SUM(f.real_bet_amount_local) * 100
                ELSE 0
            END                              AS hold_rate_pct
        FROM ps_bi.fct_casino_activity_daily f
        JOIN bireports_ec2.tbl_vendor_games_mapping_data m
            ON CAST(f.game_id AS VARCHAR) = m.c_game_id
        JOIN ps_bi.dim_user u
            ON f.player_id = u.ecr_id
        WHERE f.activity_date BETWEEN DATE '{DATA_INICIO}' AND DATE '{DATA_FIM}'
          AND u.is_test = false
          AND m.c_vendor_id IN ({provedor_list})
        GROUP BY m.c_vendor_id, m.c_game_desc, f.game_id, m.c_game_type_desc
    ),
    ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY provedor
                ORDER BY turnover_total_brl DESC
            ) AS rank_turnover
        FROM game_data
    )
    SELECT *
    FROM ranked
    WHERE rank_turnover <= 5
    ORDER BY provedor, rank_turnover
    """
    log.info("Executando: Top 5 jogos por provedor (ps_bi + mapping)...")
    df = query_athena(sql, database="ps_bi")
    log.info(f"  -> {len(df)} linhas retornadas ({df['provedor'].nunique()} provedores)")
    return df


def query_validacao_psbi():
    """
    VALIDACAO CRUZADA (ps_bi) - Turnover por vendor via dim_game.
    Para comparar com bireports_ec2.
    Nota: ps_bi.dim_game e INCOMPLETO (414 jogos vs catalogo real).
    """
    sql = f"""
    SELECT
        g.vendor_id                          AS provedor,
        SUM(f.bet_amount_local)              AS turnover_total_brl,
        SUM(f.real_bet_amount_local)         AS turnover_real_brl,
        COUNT(DISTINCT f.player_id)          AS jogadores_unicos
    FROM ps_bi.fct_casino_activity_daily f
    JOIN ps_bi.dim_game g ON f.game_id = g.game_id
    JOIN ps_bi.dim_user u ON f.player_id = u.ecr_id
    WHERE f.activity_date BETWEEN DATE '{DATA_INICIO}' AND DATE '{DATA_FIM}'
      AND u.is_test = false
    GROUP BY g.vendor_id
    ORDER BY turnover_total_brl DESC
    """
    log.info("Executando: Validacao cruzada (ps_bi via dim_game)...")
    df = query_athena(sql, database="ps_bi")
    log.info(f"  -> {len(df)} provedores (ps_bi dim_game - parcial)")
    return df


def gerar_validacao_cruzada(df_bireports, df_psbi):
    """
    Cruza dados bireports_ec2 (completo) vs ps_bi (parcial) por provedor.
    """
    df_a = df_bireports[["provedor", "turnover_total_brl", "turnover_real_brl", "jogadores_unicos"]].copy()
    df_a.columns = ["provedor", "turnover_bireports", "turnover_real_bireports", "players_bireports"]

    df_b = df_psbi[["provedor", "turnover_total_brl", "turnover_real_brl", "jogadores_unicos"]].copy()
    df_b.columns = ["provedor", "turnover_psbi", "turnover_real_psbi", "players_psbi"]

    merged = df_a.merge(df_b, on="provedor", how="left")

    for col in ["turnover_psbi", "turnover_real_psbi", "players_psbi"]:
        merged[col] = merged[col].fillna(0)

    merged["diff_abs"] = merged["turnover_bireports"] - merged["turnover_psbi"]
    merged["diff_pct"] = merged.apply(
        lambda r: (r["diff_abs"] / r["turnover_bireports"] * 100)
        if r["turnover_bireports"] != 0 else 0, axis=1
    )
    merged["cobertura_psbi_pct"] = merged.apply(
        lambda r: (r["turnover_psbi"] / r["turnover_bireports"] * 100)
        if r["turnover_bireports"] != 0 else 0, axis=1
    )

    merged = merged.sort_values("turnover_bireports", ascending=False)
    return merged


def gerar_legenda():
    """Cria DataFrame com dicionario de colunas e glossario."""
    rows = [
        # -- Aba Top 10 Provedores --
        ("Top 10 Provedores", "provedor", "Nome do provedor/vendor (ex: alea_pgsoft, pragmaticplay)", "texto"),
        ("Top 10 Provedores", "jogadores_unicos", "Qtd de jogadores distintos que apostaram nesse provedor", "inteiro"),
        ("Top 10 Provedores", "total_apostas", "Numero total de apostas (rounds/bets)", "inteiro"),
        ("Top 10 Provedores", "turnover_total_brl", "Volume total apostado (real + bonus) em R$", "R$"),
        ("Top 10 Provedores", "turnover_real_brl", "Volume apostado com dinheiro real em R$", "R$"),
        ("Top 10 Provedores", "turnover_bonus_brl", "Volume apostado com bonus em R$", "R$"),
        ("Top 10 Provedores", "wins_total_brl", "Total ganho pelos jogadores em R$", "R$"),
        ("Top 10 Provedores", "wins_real_brl", "Total ganho com dinheiro real em R$", "R$"),
        ("Top 10 Provedores", "ggr_brl", "GGR = Turnover Real - Wins Real (receita bruta da casa)", "R$"),
        ("Top 10 Provedores", "hold_rate_pct", "GGR / Turnover Real x 100 (% que a casa retem)", "%"),
        # -- Aba Top 5 Jogos --
        ("Top 5 Jogos/Provedor", "provedor", "Nome do provedor/vendor", "texto"),
        ("Top 5 Jogos/Provedor", "jogo", "Nome do jogo (game_desc do catalogo)", "texto"),
        ("Top 5 Jogos/Provedor", "game_id", "ID unico do jogo no sistema", "texto"),
        ("Top 5 Jogos/Provedor", "tipo_jogo", "Categoria do jogo (Slot, Table, Live, etc.)", "texto"),
        ("Top 5 Jogos/Provedor", "rank_turnover", "Posicao do jogo dentro do provedor (1 = mais apostado)", "inteiro"),
        ("Top 5 Jogos/Provedor", "turnover_total_brl", "Volume apostado naquele jogo em R$", "R$"),
        ("Top 5 Jogos/Provedor", "ggr_brl", "GGR daquele jogo em R$", "R$"),
        ("Top 5 Jogos/Provedor", "hold_rate_pct", "Hold Rate daquele jogo (%)", "%"),
        # -- Aba Validacao Cruzada --
        ("Validacao Cruzada", "turnover_bireports", "Turnover total segundo bireports_ec2 (fonte primaria, completa)", "R$"),
        ("Validacao Cruzada", "turnover_psbi", "Turnover segundo ps_bi via dim_game (parcial — catalogo incompleto)", "R$"),
        ("Validacao Cruzada", "cobertura_psbi_pct", "% do turnover bireports coberto pelo ps_bi", "%"),
        ("Validacao Cruzada", "diff_abs", "Diferenca absoluta (bireports - ps_bi)", "R$"),
        # -- Glossario --
        ("Glossario", "Turnover", "Volume total apostado (soma de todas as apostas/bets)", ""),
        ("Glossario", "GGR", "Gross Gaming Revenue = Apostas - Ganhos (receita bruta da casa)", ""),
        ("Glossario", "Hold Rate", "Percentual de retencao: GGR / Turnover Real x 100", ""),
        ("Glossario", "Real vs Bonus", "Real = dinheiro depositado. Bonus = creditos promocionais", ""),
        ("Glossario", "bireports_ec2", "Dados brutos agregados diarios, valores em centavos (/100). FONTE COMPLETA", ""),
        ("Glossario", "ps_bi", "Camada dbt BI mart, valores em BRL. Catalogo de jogos parcial (414 jogos)", ""),
        ("Glossario", "Nota", "ps_bi.dim_game nao cobre todos os jogos — bireports tem cobertura total de vendors", ""),
    ]
    return pd.DataFrame(rows, columns=["Aba", "Campo", "Descricao", "Unidade"])


def exportar_excel(df_provedores, df_jogos, df_validacao, df_legenda):
    """Salva tudo num Excel com 4 abas."""
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
        df_provedores.to_excel(writer, sheet_name="Top 10 Provedores", index=False)
        df_jogos.to_excel(writer, sheet_name="Top 5 Jogos por Provedor", index=False)
        df_validacao.to_excel(writer, sheet_name="Validacao Cruzada", index=False)
        df_legenda.to_excel(writer, sheet_name="Legenda", index=False)

    log.info(f"Excel salvo em: {OUTPUT_FILE}")


def main():
    log.info(f"=== Top Provedores Turnover === Periodo: {DATA_INICIO} a {DATA_FIM}")
    log.info("")

    # -- 1. Fonte primaria: bireports_ec2 (completa) --
    df_provedores = query_top_provedores_bireports()
    print(f"\n{'='*70}")
    print(f"  TOP 10 PROVEDORES POR TURNOVER (bireports_ec2)")
    print(f"  Periodo: {DATA_INICIO} a {DATA_FIM}")
    print(f"{'='*70}\n")

    for i, row in df_provedores.iterrows():
        print(f"  #{i+1:2d}  {row['provedor']:<25s}  "
              f"Turnover: R$ {row['turnover_total_brl']:>15,.2f}  "
              f"GGR: R$ {row['ggr_brl']:>12,.2f}  "
              f"Hold: {row['hold_rate_pct']:>5.1f}%  "
              f"Players: {row['jogadores_unicos']:>7,.0f}")

    total_top10 = df_provedores["turnover_total_brl"].sum()
    print(f"\n  {'':25s}  TOTAL:  R$ {total_top10:>15,.2f}")
    print()

    # -- 2. Top 5 jogos por provedor --
    provedores_list = df_provedores["provedor"].tolist()
    df_jogos = query_top_jogos_por_provedor(provedores_list)

    print(f"{'='*70}")
    print(f"  TOP 5 JOGOS POR PROVEDOR (por turnover)")
    print(f"{'='*70}\n")

    for provedor in provedores_list:
        subset = df_jogos[df_jogos["provedor"] == provedor]
        if subset.empty:
            print(f"  > {provedor}")
            print(f"    (sem detalhamento de jogos no catalogo)")
            print()
            continue
        print(f"  > {provedor}")
        for _, row in subset.iterrows():
            jogo_nome = str(row.get("jogo", "N/A"))[:40]
            print(f"    {int(row['rank_turnover']):d}. {jogo_nome:<42s}  "
                  f"Turnover: R$ {row['turnover_total_brl']:>12,.2f}  "
                  f"GGR: R$ {row['ggr_brl']:>10,.2f}  "
                  f"Players: {row['jogadores_unicos']:>6,.0f}")
        print()

    # -- 3. Validacao cruzada (ps_bi dim_game) --
    df_psbi = query_validacao_psbi()
    df_validacao = gerar_validacao_cruzada(df_provedores, df_psbi)

    print(f"{'='*70}")
    print(f"  VALIDACAO CRUZADA (bireports vs ps_bi dim_game)")
    print(f"{'='*70}\n")

    for _, row in df_validacao.iterrows():
        cob = row["cobertura_psbi_pct"]
        status = "OK" if cob > 80 else "PARCIAL" if cob > 0 else "SEM COBERTURA"
        print(f"  [{status:>14s}]  {row['provedor']:<25s}  "
              f"bireports: R$ {row['turnover_bireports']:>15,.2f}  "
              f"ps_bi: R$ {row['turnover_psbi']:>15,.2f}  "
              f"cobertura: {cob:>5.1f}%")
    print()

    # -- Auditoria resumo --
    total_bireports = df_provedores["turnover_total_brl"].sum()
    total_psbi_val = df_validacao["turnover_psbi"].sum()
    cobertura_geral = (total_psbi_val / total_bireports * 100) if total_bireports else 0

    print(f"{'='*70}")
    print(f"  AUDITORIA RESUMO")
    print(f"{'='*70}")
    print(f"  Total Turnover Top 10 (bireports):  R$ {total_bireports:>15,.2f}")
    print(f"  Total coberto ps_bi (dim_game):     R$ {total_psbi_val:>15,.2f}")
    print(f"  Cobertura geral ps_bi:              {cobertura_geral:>5.1f}%")
    print(f"  Periodo:                            {DATA_INICIO} a {DATA_FIM}")
    print(f"  Test users:                         EXCLUIDOS")
    print(f"  Gerado em:                          {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    if cobertura_geral < 50:
        print(f"  [!] NOTA: ps_bi.dim_game cobre apenas {cobertura_geral:.1f}% do catalogo.")
        print(f"      Isso e esperado — o catalogo dbt (dim_game) tem apenas 414 jogos.")
        print(f"      O ranking de provedores usa bireports_ec2 (fonte completa).")
        print(f"      O detalhamento por jogo pode estar incompleto para vendors com")
        print(f"      jogos nao mapeados no dim_game/tbl_vendor_games_mapping_data.")
    print()

    # -- 4. Export --
    df_legenda = gerar_legenda()
    exportar_excel(df_provedores, df_jogos, df_validacao, df_legenda)
    print(f"  Arquivo exportado: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()