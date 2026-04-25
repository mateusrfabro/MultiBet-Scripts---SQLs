"""
Helpers SQL e de entrega - Super Nova Gaming / MultiBet.

Centraliza fragmentos SQL que se repetem em todos os scripts (timezone BRT,
filtro test users, CAST de affiliate_id) e helpers de saida padronizada
(CSV + legenda). Evita copy-paste e divergencia entre entregas.

Uso rapido:
    from db.helpers import (
        FILTER_NOT_TEST_PSBI,
        to_brt,
        affiliate_in,
        save_csv_with_legenda,
        clean_tz_columns,
    )
"""
from __future__ import annotations

import os
from datetime import datetime
from typing import Iterable

import pandas as pd


# ============================================================================
# SQL: filtros canonicos
# ============================================================================

# Padrao para exclusao de test users em ps_bi.dim_user / ps_bi.fct_*
FILTER_NOT_TEST_PSBI = "(is_test = false OR is_test IS NULL)"

# Padrao para bireports_ec2.tbl_ecr* e ecr_ec2.tbl_ecr
FILTER_NOT_TEST_BIREPORTS = "c_test_user = false"


def to_brt(col: str) -> str:
    """
    Retorna a expressao SQL para converter um timestamp UTC em BRT (America/Sao_Paulo).

    Regra obrigatoria do projeto (CLAUDE.md): toda entrega ao negocio com
    timestamp precisa estar em BRT. Sintaxe Presto/Trino.

    ATENCAO: NAO usar em campos que ja estao em BRT, como
    `vendor_ec2.tbl_sports_book_bet_details.c_ts_realend` (validado 14/04/2026).

    >>> to_brt("u.signup_datetime")
    "u.signup_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'"
    """
    return f"{col} AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'"


def to_brt_date(col: str) -> str:
    """Mesma conversao do to_brt, mas retorna so a DATE (sem hora)."""
    return f"CAST({to_brt(col)} AS DATE)"


def affiliate_in(aff_ids: Iterable[str | int], column: str = "affiliate_id") -> str:
    """
    Retorna fragmento SQL `CAST(column AS VARCHAR) IN ('...', '...')`.

    Critico: affiliate_id em ps_bi eh VARCHAR. Comparar com integer causa
    TYPE_MISMATCH (feedback_affiliate_id_varchar.md).

    >>> affiliate_in(["363722", "532570"])
    "CAST(affiliate_id AS VARCHAR) IN ('363722', '532570')"
    """
    ids_str = ", ".join(f"'{x}'" for x in aff_ids)
    return f"CAST({column} AS VARCHAR) IN ({ids_str})"


# ============================================================================
# Formatacao de saida para console / WhatsApp
# ============================================================================

def fmt_brl(v) -> str:
    """Formata numero em padrao BRL: R$ 1.234,56. None/NaN vira R$ 0,00."""
    if v is None or str(v) in ("None", "nan", "NaT", ""):
        return "R$ 0,00"
    try:
        v = float(v)
    except (TypeError, ValueError):
        return "R$ 0,00"
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def fmt_int(v) -> str:
    """Formata inteiro em padrao BR: 12.345. None/NaN vira 0."""
    if v is None or str(v) in ("None", "nan", "NaT", ""):
        return "0"
    try:
        return f"{int(float(v)):,}".replace(",", ".")
    except (TypeError, ValueError):
        return "0"


def fmt_pct(v, casas: int = 1) -> str:
    """Formata percentual (recebe fracao ou percentual ja calculado)."""
    if v is None or str(v) in ("None", "nan", "NaT", ""):
        return "0%"
    try:
        return f"{float(v):.{casas}f}%"
    except (TypeError, ValueError):
        return "0%"


# ============================================================================
# Helpers de DataFrame (cleanup recorrente)
# ============================================================================

def clean_tz_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converte colunas datetime com timezone para string. Evita erro recorrente
    ao salvar em Excel ou serializar.
    """
    for col in df.columns:
        if hasattr(df[col], "dt") and hasattr(df[col].dt, "tz") and df[col].dt.tz is not None:
            df[col] = df[col].astype(str)
    return df


# ============================================================================
# Padrao de entrega: CSV + legenda (regra obrigatoria CLAUDE.md)
# ============================================================================

def save_csv_with_legenda(
    df: pd.DataFrame,
    csv_path: str,
    *,
    titulo: str,
    columns_dict: dict[str, str],
    glossario: dict[str, str] | None = None,
    fonte: str = "AWS Athena",
    periodo: str = "",
    regras: list[str] | None = None,
    validacao: list[str] | None = None,
    acao_sugerida: str = "",
    extraidor: str = "Squad Intelligence Engine",
) -> tuple[str, str]:
    """
    Salva DataFrame como CSV + gera arquivo de legenda _legenda.txt ao lado.

    Regra do projeto: nenhuma entrega sem dicionario/glossario.
    """
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    clean_tz_columns(df)
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")

    base = csv_path.rsplit(".csv", 1)[0]
    legenda_path = f"{base}_legenda.txt"

    linhas: list[str] = []
    linhas.append("=" * 60)
    linhas.append(titulo)
    linhas.append("=" * 60)
    linhas.append("")
    linhas.append(f"EXTRAIDO POR  : {extraidor}")
    linhas.append(f"DATA EXTRACAO : {datetime.now().strftime('%Y-%m-%d %H:%M BRT')}")
    linhas.append(f"FONTE         : {fonte}")
    if periodo:
        linhas.append(f"PERIODO       : {periodo}")
    linhas.append(f"LINHAS        : {len(df)}")
    linhas.append("")

    linhas.append("-" * 60)
    linhas.append("DICIONARIO DE COLUNAS")
    linhas.append("-" * 60)
    linhas.append("")
    for col, desc in columns_dict.items():
        linhas.append(f"{col:20} {desc}")
    linhas.append("")

    if glossario:
        linhas.append("-" * 60)
        linhas.append("GLOSSARIO")
        linhas.append("-" * 60)
        linhas.append("")
        for termo, desc in glossario.items():
            linhas.append(f"{termo:20} {desc}")
        linhas.append("")

    if regras:
        linhas.append("-" * 60)
        linhas.append("REGRAS DE EXTRACAO")
        linhas.append("-" * 60)
        linhas.append("")
        for r in regras:
            linhas.append(f"- {r}")
        linhas.append("")

    if validacao:
        linhas.append("-" * 60)
        linhas.append("VALIDACAO")
        linhas.append("-" * 60)
        linhas.append("")
        for v in validacao:
            linhas.append(v)
        linhas.append("")

    if acao_sugerida:
        linhas.append("-" * 60)
        linhas.append("ACAO SUGERIDA")
        linhas.append("-" * 60)
        linhas.append("")
        linhas.append(acao_sugerida)
        linhas.append("")

    with open(legenda_path, "w", encoding="utf-8") as f:
        f.write("\n".join(linhas))

    return csv_path, legenda_path
