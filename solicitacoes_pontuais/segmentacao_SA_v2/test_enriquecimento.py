"""
Teste integrado dos blocos 4, 5, 6 do enriquecimento.

Pega 200 players reais do CSV atual + matriz_risco e roda os blocos.
Imprime amostra do DataFrame final pra validacao visual.
"""
import sys
import logging
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from db.supernova import execute_supernova
from pipelines.segmentacao_sa_enriquecimento import (
    bloco_4_derivaveis,
    bloco_5_risk_tags_flags,
    bloco_6_kyc,
    bloco_1_2_metricas_30d,
    bloco_3_top_jogos_e_temporal,
    bloco_5b_btr_bonus,
)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def main():
    # Carrega 200 players de pcr_atual + matriz_risco (mesma logica do pipeline)
    log.info("Carregando 200 players de multibet.pcr_atual + matriz_risco...")
    rows = execute_supernova(
        """
        SELECT p.player_id, p.external_id, p.rating, p.pvs,
               p.num_deposits, p.recency_days, p.product_type,
               p.casino_rounds, p.sport_bets,
               p.c_category, p.registration_date,
               COALESCE(m.classificacao, 'Nao Identificado') AS classificacao_risco,
               m.score_norm AS score_risco
        FROM multibet.pcr_atual p
        LEFT JOIN multibet.matriz_risco m ON CAST(p.external_id AS VARCHAR) = m.user_ext_id
        WHERE p.rating IN ('A', 'S')
        LIMIT 200;
        """,
        fetch=True,
    )
    df = pd.DataFrame(rows, columns=[
        "player_id", "external_id", "rating", "pvs",
        "num_deposits", "recency_days", "product_type",
        "casino_rounds", "sport_bets",
        "c_category", "registration_date",
        "classificacao_risco", "score_risco",
    ])
    log.info(f"  -> {len(df)} players carregados")

    # ----- BLOCO 4 -----
    df = bloco_4_derivaveis(df, snapshot_date="2026-04-27")

    # ----- BLOCO 5 (risk_tags) -----
    df = bloco_5_risk_tags_flags(df, snapshot_date="2026-04-27")

    # ----- BLOCO 6 (KYC) -----
    df = bloco_6_kyc(df, snapshot_date="2026-04-27")

    # ----- BLOCO 1+2 (metricas 30d financeiras + aposta) -----
    df = bloco_1_2_metricas_30d(df, snapshot_date="2026-04-27")

    # ----- BLOCO 3 (top jogos / providers / horario por TIER) -----
    df = bloco_3_top_jogos_e_temporal(df, snapshot_date="2026-04-27")

    # ----- BLOCO 5b (BTR + bonus extras) -----
    df = bloco_5b_btr_bonus(df, snapshot_date="2026-04-27")

    # Salva CSV sample com TODAS as colunas pra Castrin validar
    csv_out = Path("output/sample_segmentacao_sa_v2_3_dev.csv")
    csv_out.parent.mkdir(exist_ok=True)
    df.to_csv(csv_out, index=False, sep=";", decimal=",", encoding="utf-8-sig")
    log.info(f"\nCSV sample salvo: {csv_out} ({len(df)} linhas, {len(df.columns)} colunas)")

    # ----- RESULTADO -----
    print("\n" + "=" * 120)
    print("DataFrame final (5 amostras):")
    print("=" * 120)
    cols_show = [
        "player_id", "rating", "c_category",
        "LIFECYCLE_STATUS", "RG_STATUS", "ACCOUNT_RESTRICTED_FLAG",
        "SELF_EXCLUDED_FLAG", "PRIMARY_VERTICAL", "BONUS_ABUSE_FLAG",
        "KYC_STATUS", "self_exclusion_status",
        "cool_off_status", "restricted_product",
    ]
    cols_show = [c for c in cols_show if c in df.columns]
    print(df[cols_show].head(5).to_string(index=False))

    print("\n" + "=" * 120)
    print("Distribuicoes:")
    print("=" * 120)
    for col in ("LIFECYCLE_STATUS", "RG_STATUS", "PRIMARY_VERTICAL",
                "BONUS_ABUSE_FLAG", "KYC_STATUS",
                "ACCOUNT_RESTRICTED_FLAG", "SELF_EXCLUDED_FLAG"):
        if col in df.columns:
            print(f"\n{col}:")
            print(df[col].value_counts(dropna=False).to_string())

    # Quantas colunas novas foram adicionadas?
    novas = [c for c in df.columns if c in (
        "LIFECYCLE_STATUS", "RG_STATUS", "ACCOUNT_RESTRICTED_FLAG",
        "SELF_EXCLUDED_FLAG", "PRIMARY_VERTICAL", "BONUS_ABUSE_FLAG",
        "KYC_STATUS", "kyc_level", "self_exclusion_status",
        "cool_off_status", "restricted_product"
    )]
    print(f"\n[OK] {len(novas)} colunas novas adicionadas: {novas}")


if __name__ == "__main__":
    main()
