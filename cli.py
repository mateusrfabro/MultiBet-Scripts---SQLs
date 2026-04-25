#!/usr/bin/env python
"""
Super Nova CLI - entry point para demandas recorrentes do squad.

Uso:
    python cli.py affiliate-base 363722                         # base lifetime 1 aff
    python cli.py affiliate-base 363722 --name "Pri Simoes"     # com label
    python cli.py affiliate-base 363722 532570 --name "Tres"    # consolidado

    python cli.py affiliate-daily 363722                        # D-1 auto
    python cli.py affiliate-daily 363722 --date 2026-04-23
    python cli.py affiliate-daily 363722 532570 --date 2026-04-23

Comandos disponiveis:
    affiliate-base   - base lifetime de players (ecr_id, smartico_id, nome...)
    affiliate-daily  - report diario (REG, FTD, GGR, NGR, Saques...)

Cada comando gera CSV + legenda padronizada em reports/ + print console.
Auditor roda automaticamente (cross-check Athena) antes da entrega.
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path


# --- garantir que raiz do projeto esta no path ---
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

REPORTS_DIR = ROOT / "reports"


def _setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    # Silenciar warning recorrente do pandas/pyathena
    import warnings
    warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy.*")


def _cmd_affiliate_base(args: argparse.Namespace) -> int:
    from snova_cli.commands import affiliate_base
    affiliate_base.run(
        affiliate_ids=args.affiliate_ids,
        output_dir=str(REPORTS_DIR),
        label=args.name,
    )
    return 0


def _cmd_affiliate_daily(args: argparse.Namespace) -> int:
    from snova_cli.commands import affiliate_daily
    affiliate_daily.run(
        affiliate_ids=args.affiliate_ids,
        data=args.date,
        output_dir=str(REPORTS_DIR),
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="snova",
        description="Super Nova CLI - demandas recorrentes do squad de dados",
    )
    p.add_argument("-v", "--verbose", action="store_true", help="Log DEBUG")

    sub = p.add_subparsers(dest="command", required=True, metavar="COMANDO")

    # --- affiliate-base ---
    p_base = sub.add_parser(
        "affiliate-base",
        help="Base lifetime de players de um ou mais affiliates (CRM)",
    )
    p_base.add_argument("affiliate_ids", nargs="+", help="IDs do(s) affiliate(s)")
    p_base.add_argument("--name", help="Nome amigavel do affiliate (aparece no filename/titulo)")
    p_base.set_defaults(func=_cmd_affiliate_base)

    # --- affiliate-daily ---
    p_daily = sub.add_parser(
        "affiliate-daily",
        help="Report diario (REG, FTD, GGR, NGR, Saques) para affiliate(s)",
    )
    p_daily.add_argument("affiliate_ids", nargs="+", help="IDs do(s) affiliate(s)")
    p_daily.add_argument("--date", help="Data YYYY-MM-DD (default: D-1 automatico)")
    p_daily.set_defaults(func=_cmd_affiliate_daily)

    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    _setup_logging(args.verbose)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
