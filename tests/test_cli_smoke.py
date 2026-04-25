"""
Smoke test minimo do snova_cli — roda em <10s, NAO toca o Athena.

Valida apenas que:
- CLI parsa --help sem erro
- Comandos definidos sao acessiveis
- Imports da CLI nao quebraram
- Helpers e auditor sao importaveis e contratos basicos OK

NAO substitui validacao empirica (rodar comando contra Athena em data real).
Roda como sanity check pre-commit / pre-deploy:

    python -m unittest tests.test_cli_smoke -v
"""
from __future__ import annotations

import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PYTHON = sys.executable


class TestCLISmoke(unittest.TestCase):

    def test_cli_help(self):
        """python cli.py --help retorna 0 e menciona os comandos."""
        result = subprocess.run(
            [PYTHON, "cli.py", "--help"],
            cwd=ROOT, capture_output=True, text=True, timeout=15,
        )
        self.assertEqual(result.returncode, 0, msg=f"stderr: {result.stderr}")
        self.assertIn("affiliate-base", result.stdout)
        self.assertIn("affiliate-daily", result.stdout)

    def test_cli_subcommand_help(self):
        """Subcommands tem --help."""
        for cmd in ("affiliate-base", "affiliate-daily"):
            with self.subTest(cmd=cmd):
                result = subprocess.run(
                    [PYTHON, "cli.py", cmd, "--help"],
                    cwd=ROOT, capture_output=True, text=True, timeout=15,
                )
                self.assertEqual(result.returncode, 0)
                self.assertIn("affiliate_ids", result.stdout)


class TestImports(unittest.TestCase):

    def test_db_helpers(self):
        from db.helpers import (
            FILTER_NOT_TEST_PSBI, FILTER_NOT_TEST_BIREPORTS,
            to_brt, to_brt_date, affiliate_in,
            fmt_brl, fmt_int, fmt_pct,
            clean_tz_columns, save_csv_with_legenda,
        )
        # Contratos minimos
        self.assertIn("is_test = false", FILTER_NOT_TEST_PSBI)
        self.assertIn("c_test_user", FILTER_NOT_TEST_BIREPORTS)
        self.assertIn("America/Sao_Paulo", to_brt("col"))
        self.assertIn("CAST", to_brt_date("col"))
        self.assertEqual(affiliate_in(["123"]), "CAST(affiliate_id AS VARCHAR) IN ('123')")
        self.assertEqual(fmt_brl(1234.5), "R$ 1.234,50")
        self.assertEqual(fmt_int(1234), "1.234")
        self.assertEqual(fmt_int(None), "0")

    def test_db_auditor(self):
        from db.auditor import AthenaAuditor
        a = AthenaAuditor()
        a.add_count("source_a", 100)
        a.add_count("source_b", 100)
        a.compare_counts(baseline_label="source_a")
        self.assertTrue(a.is_approved())

        b = AthenaAuditor()
        b.add_count("source_a", 100)
        b.add_count("source_b", 80)  # 20% off
        b.compare_counts(baseline_label="source_a")
        self.assertFalse(b.is_approved())  # divergencia >5% padrao = FALHA

    def test_snova_cli_modules(self):
        from snova_cli.commands import affiliate_base, affiliate_daily
        # Confirmar que entry points existem
        self.assertTrue(callable(affiliate_base.run))
        self.assertTrue(callable(affiliate_daily.run))


if __name__ == "__main__":
    unittest.main(verbosity=2)
