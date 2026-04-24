#!/bin/bash
# =============================================================================
# run_views_casino_sportsbook.sh
# =============================================================================
# Refresh diario das 10 pipelines: Casino + Sportsbook + Retention + Cohort (SuperNova DB).
#
# IMPORTANTE: deploy ISOLADO — nao interfere com aplicacoes existentes na EC2.
# - Pasta propria: /home/ec2-user/multibet/views_casino_sportsbook/
# - Logs proprios: /home/ec2-user/multibet/views_casino_sportsbook/logs/
# - Reutiliza venv, .env, bigquery_credentials.json e bastion-analytics-key.pem
#   da raiz /home/ec2-user/multibet/ (sem modificar nenhum desses arquivos)
#
# Ordem de execucao (OBRIGATORIA — silvers antes das gold views):
#   Bloco 1 (silvers independentes, poderiam rodar em paralelo mas rodam em
#            sequencia aqui pra facilitar debug e nao sobrecarregar bastion SSH):
#     1. fct_casino_activity          (~30s)
#     2. fct_sports_activity          (~60s, inclui qty_bets via SB_BUYIN)
#     3. fact_casino_rounds           (~5min, catalogo bireports v4.1)
#     4. fact_sports_bets_by_sport    (~3min)
#     5. fact_sports_bets             (~6min, inclui open bets com cap odds)
#
#   Bloco 2 (depende dos silvers de casino + sports activity):
#     6. fct_active_players_by_period       (~2min, tabela 18 linhas)
#     7. fct_player_performance_by_period   (~4min, tabela ~500K-800K linhas)
#
#   Bloco 3 (independente, fonte Athena direta):
#     8. vw_active_player_retention_weekly   (~60s, retention semanal)
#     9. agg_cohort_acquisition              (~2min, incremental D30 window)
#
#   Bloco 4 (DDL only, sem custo Athena):
#    10. create_views_casino_sportsbook (~10s, recria as 9 views gold)
#
# Tempo total estimado: ~25 minutos
# =============================================================================

set -e  # Fail fast: se qualquer pipeline falhar, aborta o resto

# Configuracao de caminhos (RAIZ e a instalacao existente, nao mexer)
MULTIBET_ROOT="/home/ec2-user/multibet"
DEPLOY_DIR="${MULTIBET_ROOT}/views_casino_sportsbook"
VENV_PYTHON="${MULTIBET_ROOT}/venv/bin/python3"
LOG_DIR="${DEPLOY_DIR}/logs"
LOG_FILE="${LOG_DIR}/views_$(date +%Y-%m-%d).log"

# Garante que o diretorio de logs existe (idempotente, nao mexe em nada fora)
mkdir -p "${LOG_DIR}"

# Carrega env da raiz (compartilhado com outros pipelines existentes)
# Nota: .env pode ter CRLF (Windows) — strip \r antes de source
set -a
source <(sed 's/\r$//' "${MULTIBET_ROOT}/.env")
set +a

# Trabalha a partir do deploy_dir (pra imports relativos funcionarem)
cd "${DEPLOY_DIR}"

# Python path inclui deploy_dir pra achar pipelines/ e db/ da raiz
export PYTHONPATH="${DEPLOY_DIR}:${MULTIBET_ROOT}:${PYTHONPATH}"

# Helper de log
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_FILE}"
}

# Helper de execucao com log dedicado por pipeline
run_pipeline() {
    local name=$1
    local start_ts=$(date +%s)
    log "▶ ${name} — iniciando"
    if "${VENV_PYTHON}" "pipelines/${name}.py" >> "${LOG_FILE}" 2>&1; then
        local elapsed=$(($(date +%s) - start_ts))
        log "✓ ${name} — concluido em ${elapsed}s"
    else
        local elapsed=$(($(date +%s) - start_ts))
        log "✗ ${name} — FALHOU apos ${elapsed}s"
        exit 1
    fi
}

log "================================================================"
log "Iniciando refresh views Casino & Sportsbook v4.1"
log "Deploy: ${DEPLOY_DIR}"
log "Log:    ${LOG_FILE}"
log "================================================================"

# Bloco 1: silvers
run_pipeline "fct_casino_activity"
run_pipeline "fct_sports_activity"
run_pipeline "fact_casino_rounds"
run_pipeline "fact_sports_bets_by_sport"
run_pipeline "fact_sports_bets"

# Bloco 2: agregacoes cross-produto (precisam dos silvers de casino + sports)
run_pipeline "fct_active_players_by_period"
run_pipeline "fct_player_performance_by_period"

# Bloco 3: pipelines independentes (fonte Athena direta, sem dependencia dos silvers)
run_pipeline "vw_active_player_retention_weekly"
run_pipeline "agg_cohort_acquisition"

# Bloco 4: DDL gold (recria views, idempotente)
run_pipeline "create_views_casino_sportsbook"

log "================================================================"
log "✅ Refresh completo — 10 pipelines executados com sucesso"
log "================================================================"
