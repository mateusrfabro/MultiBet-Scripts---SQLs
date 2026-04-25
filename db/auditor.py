"""
Auditor Athena - validacao cruzada entre fontes antes de entregar ao stakeholder.

Regra do projeto (CLAUDE.md): nenhum report vai pro Head/CTO/CGO sem validacao
empirica. BigQuery foi desativado em 19/04/2026, entao validacao e 100% Athena
cross-checking diferentes tabelas (ex: ps_bi.dim_user vs ecr_ec2.tbl_ecr).

Uso:
    from db.auditor import AthenaAuditor

    a = AthenaAuditor()
    a.add_count("ps_bi.dim_user", 2278)
    a.add_count("ecr_ec2.tbl_ecr", 2278)
    a.add_count("ecr_ec2.tbl_ecr_banner", 2278)
    a.check_unique("ps_bi.dim_user", df, "external_id")
    a.check_nulls(df, ["ecr_id", "external_id"])

    a.report()           # imprime sumario
    if a.is_approved():  # bool para gate de entrega
        ...
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Literal

import pandas as pd


log = logging.getLogger(__name__)


Status = Literal["OK", "ALERTA", "FALHA"]


@dataclass
class CheckResult:
    label: str
    status: Status
    detail: str


@dataclass
class AthenaAuditor:
    """
    Auditor incremental. Adiciona checks e no final chama report()/is_approved().

    Tolerancias default:
      - divergencia de contagem < 2% = OK, < 5% = ALERTA, >= 5% = FALHA
      - duplicatas ou nulls > 0 = FALHA
    """
    divergencia_ok: float = 2.0        # % abaixo do qual considera OK
    divergencia_alerta: float = 5.0    # % acima do qual vira FALHA

    _counts: dict[str, int] = field(default_factory=dict)
    _results: list[CheckResult] = field(default_factory=list)

    # ------------------------------------------------------------
    # Contagens cross-source
    # ------------------------------------------------------------

    def add_count(self, label: str, n: int) -> None:
        """Adiciona uma contagem de uma fonte. A primeira e usada como baseline."""
        self._counts[label] = int(n)

    def compare_counts(self, baseline_label: str | None = None) -> None:
        """
        Compara todas as contagens adicionadas contra um baseline (primeiro
        label por default). Gera um CheckResult para cada divergencia.
        """
        if not self._counts:
            return

        if baseline_label is None:
            baseline_label = next(iter(self._counts))

        baseline = self._counts[baseline_label]
        self._results.append(CheckResult(
            label=f"BASELINE: {baseline_label}",
            status="OK",
            detail=f"{baseline} registros",
        ))

        for lab, n in self._counts.items():
            if lab == baseline_label:
                continue
            delta = n - baseline
            div = abs(delta) / baseline * 100 if baseline else 0
            if div < self.divergencia_ok:
                status: Status = "OK"
            elif div < self.divergencia_alerta:
                status = "ALERTA"
            else:
                status = "FALHA"
            self._results.append(CheckResult(
                label=lab,
                status=status,
                detail=f"{n} (delta {delta:+d} | {div:.2f}%)",
            ))

    # ------------------------------------------------------------
    # Integridade de DataFrame
    # ------------------------------------------------------------

    def check_unique(self, label: str, df: pd.DataFrame, col: str) -> None:
        """Valida que a coluna nao tem duplicatas."""
        total = len(df)
        unicos = df[col].nunique(dropna=False)
        dup = total - unicos
        status: Status = "OK" if dup == 0 else "FALHA"
        self._results.append(CheckResult(
            label=f"Unicidade {label}.{col}",
            status=status,
            detail=f"{unicos}/{total} unicos | {dup} duplicatas",
        ))

    def check_nulls(self, df: pd.DataFrame, cols: list[str]) -> None:
        """Valida que colunas criticas nao tem nulls."""
        for c in cols:
            n_null = int(df[c].isna().sum())
            status: Status = "OK" if n_null == 0 else "FALHA"
            self._results.append(CheckResult(
                label=f"Nulls {c}",
                status=status,
                detail=f"{n_null} nulls em {len(df)} linhas",
            ))

    # ------------------------------------------------------------
    # Output
    # ------------------------------------------------------------

    def report(self) -> list[str]:
        """Imprime sumario no log e retorna lista de linhas (util para legenda)."""
        linhas: list[str] = []
        linhas.append("-" * 70)
        for r in self._results:
            line = f"  {r.label:35} [{r.status:<6}] {r.detail}"
            linhas.append(line)
            log.info(line)
        linhas.append("-" * 70)
        if self.is_approved():
            linhas.append("  AUDITOR APROVADO")
        else:
            linhas.append("  AUDITOR REPROVADO")
        linhas.append("-" * 70)
        for l in linhas[-3:]:
            log.info(l)
        return linhas

    def is_approved(self) -> bool:
        return not any(r.status == "FALHA" for r in self._results)

    def has_alert(self) -> bool:
        return any(r.status == "ALERTA" for r in self._results)

    def results(self) -> list[CheckResult]:
        return list(self._results)
