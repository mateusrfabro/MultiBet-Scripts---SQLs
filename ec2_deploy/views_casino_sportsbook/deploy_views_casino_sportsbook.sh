#!/bin/bash
# =============================================================================
# deploy_views_casino_sportsbook.sh
# =============================================================================
# Deploy ISOLADO das views gold Casino & Sportsbook v4.1 na EC2 ETL.
#
# ⚠️  NÃO EXECUTAR SEM VALIDAÇÃO PRÉVIA DO GUSTA. Este script é idempotente
#     e seguro, mas so deve rodar apos o Gusta confirmar que as views estao
#     100% corretas em ambiente de dev.
#
# Principios (ver feedback_ec2_deploy_nao_mexer_existente.md):
#   1. Pasta isolada: /home/ec2-user/multibet/views_casino_sportsbook/
#   2. Reutiliza venv, .env, credentials da raiz (sem modificar)
#   3. Crontab APPEND-ONLY (backup antes, nunca edita entries existentes)
#   4. Logs separados em subpasta propria
#   5. Rollback plan documentado
#
# Uso:
#   1. Copiar esta pasta inteira pra EC2:
#      scp -i etl-key.pem -r ec2_deploy/views_casino_sportsbook \
#          ec2-user@54.197.63.138:/home/ec2-user/multibet/
#   2. SSH na EC2:
#      ssh -i etl-key.pem ec2-user@54.197.63.138
#   3. Rodar este script:
#      cd /home/ec2-user/multibet/views_casino_sportsbook
#      chmod +x deploy_views_casino_sportsbook.sh run_views_casino_sportsbook.sh
#      bash deploy_views_casino_sportsbook.sh
#
# Rollback (se algo der errado):
#   bash rollback_views_casino_sportsbook.sh
# =============================================================================

set -e

DEPLOY_DIR="/home/ec2-user/multibet/views_casino_sportsbook"
BACKUP_DIR="/home/ec2-user/multibet/backups"
TS=$(date +%Y%m%d-%H%M%S)
CRONTAB_BACKUP="${BACKUP_DIR}/crontab.backup.${TS}.txt"
MARKER="# VIEWS_CASINO_SPORTSBOOK_V4 — managed by deploy_views_casino_sportsbook.sh"

echo "================================================================"
echo "Deploy Views Casino & Sportsbook v4.1 — isolado"
echo "Timestamp: ${TS}"
echo "================================================================"

# --- 1. Validacoes pre-deploy (nao prosseguir se algo estranho) ------------
if [ ! -d "/home/ec2-user/multibet" ]; then
    echo "✗ ERRO: /home/ec2-user/multibet nao existe. Abortando."
    exit 1
fi

if [ ! -f "/home/ec2-user/multibet/.env" ]; then
    echo "✗ ERRO: .env nao encontrado em /home/ec2-user/multibet/. Abortando."
    exit 1
fi

if [ ! -f "/home/ec2-user/multibet/venv/bin/python3" ]; then
    echo "✗ ERRO: venv nao encontrado em /home/ec2-user/multibet/venv/. Abortando."
    exit 1
fi

if [ ! -f "/home/ec2-user/.ssh/bastion-analytics-key.pem" ] && [ ! -f "/home/ec2-user/multibet/bastion-analytics-key.pem" ]; then
    echo "✗ ERRO: bastion-analytics-key.pem nao encontrado em ~/.ssh/ nem em multibet/. Abortando."
    exit 1
fi

echo "✓ Pre-deploy validado: raiz /home/ec2-user/multibet/ intacta"

# --- 2. Backup do crontab atual (OBRIGATORIO) ------------------------------
mkdir -p "${BACKUP_DIR}"
crontab -l > "${CRONTAB_BACKUP}" 2>/dev/null || echo "# (crontab vazio no deploy)" > "${CRONTAB_BACKUP}"
echo "✓ Crontab atual salvo em ${CRONTAB_BACKUP}"
echo "  Entries atuais: $(grep -cvE '^(#|$)' ${CRONTAB_BACKUP})"

# --- 3. Idempotencia: checa se ja foi deployado antes ----------------------
if crontab -l 2>/dev/null | grep -Fq "${MARKER}"; then
    echo "⚠️  Deploy ja aplicado anteriormente (marker encontrado no crontab)."
    echo "   Para reaplicar: rode rollback_views_casino_sportsbook.sh primeiro."
    echo "   Saindo sem alterar nada."
    exit 0
fi

# --- 4. Confere que este diretorio tem o que precisa -----------------------
if [ ! -d "${DEPLOY_DIR}/pipelines" ]; then
    echo "✗ ERRO: ${DEPLOY_DIR}/pipelines nao existe (copiou esta pasta certa?)."
    exit 1
fi

EXPECTED_PIPELINES=(
    "fct_casino_activity.py"
    "fct_sports_activity.py"
    "fact_casino_rounds.py"
    "fact_sports_bets_by_sport.py"
    "fact_sports_bets.py"
    "fct_active_players_by_period.py"
    "fct_player_performance_by_period.py"
    "vw_active_player_retention_weekly.py"
    "agg_cohort_acquisition.py"
    "create_views_casino_sportsbook.py"
)
for p in "${EXPECTED_PIPELINES[@]}"; do
    if [ ! -f "${DEPLOY_DIR}/pipelines/${p}" ]; then
        echo "✗ ERRO: pipelines/${p} nao encontrado."
        exit 1
    fi
done
echo "✓ 10 pipelines esperados encontrados"

if [ ! -f "${DEPLOY_DIR}/run_views_casino_sportsbook.sh" ]; then
    echo "✗ ERRO: run_views_casino_sportsbook.sh nao encontrado."
    exit 1
fi

# --- 5. Setup permissoes + estrutura de logs -------------------------------
chmod +x "${DEPLOY_DIR}/run_views_casino_sportsbook.sh"
mkdir -p "${DEPLOY_DIR}/logs"
echo "✓ Permissoes e logs configurados"

# --- 6. Smoke test: roda manualmente uma vez antes de agendar --------------
echo "================================================================"
echo "Smoke test: executando refresh manual (primeira vez)..."
echo "================================================================"
bash "${DEPLOY_DIR}/run_views_casino_sportsbook.sh"
SMOKE_EXIT=$?
if [ ${SMOKE_EXIT} -ne 0 ]; then
    echo "✗ ERRO: smoke test falhou (exit ${SMOKE_EXIT}). Abortando deploy."
    echo "   Crontab NAO foi modificado. Logs em ${DEPLOY_DIR}/logs/"
    exit 1
fi
echo "✓ Smoke test passou"

# --- 7. Adiciona cron entries (APPEND-ONLY) --------------------------------
# Horario escolhido: 04:30 BRT (07:30 UTC)
# - Depois do grandes_ganhos (00:30 BRT)
# - Depois do bloco de sync_all (madrugada ~02:00-04:00)
# - Antes do expediente (BR comeca 09:00)
# - Nao conflita com etl_aquisicao_trafego (hourly :10)
CRON_ENTRY="30 7 * * * ${DEPLOY_DIR}/run_views_casino_sportsbook.sh >> ${DEPLOY_DIR}/logs/cron.log 2>&1"

TMP_CRON=$(mktemp)
crontab -l 2>/dev/null > "${TMP_CRON}" || true
echo "" >> "${TMP_CRON}"
echo "${MARKER}" >> "${TMP_CRON}"
echo "# Refresh diario 10 pipelines: Casino+Sportsbook+Retention+Cohort" >> "${TMP_CRON}"
echo "# Horario: 04:30 BRT (07:30 UTC) — evita conflito com pipelines existentes" >> "${TMP_CRON}"
echo "${CRON_ENTRY}" >> "${TMP_CRON}"
crontab "${TMP_CRON}"
rm -f "${TMP_CRON}"

echo "✓ Crontab atualizado (append-only)"
echo ""
echo "================================================================"
echo "Deploy completo ✅"
echo "================================================================"
echo ""
echo "Proximos passos:"
echo "  1. Verificar: crontab -l | tail -10"
echo "  2. Aguardar proxima execucao automatica (04:30 BRT)"
echo "  3. Logs do cron: tail -f ${DEPLOY_DIR}/logs/cron.log"
echo "  4. Logs por execucao: ls ${DEPLOY_DIR}/logs/"
echo ""
echo "Rollback (se necessario):"
echo "  bash ${DEPLOY_DIR}/rollback_views_casino_sportsbook.sh"
echo ""
echo "Backup do crontab antigo: ${CRONTAB_BACKUP}"
