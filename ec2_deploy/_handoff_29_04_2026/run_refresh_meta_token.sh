#!/bin/bash
# =================================================================
# Refresh Meta User Token — cron mensal (dia 1, 02:00 BRT = 05:00 UTC)
# Chama oauth/access_token?grant_type=fb_exchange_token e atualiza o
# META_ADS_ACCESS_TOKEN no .env do orquestrador.
#
# Crontab sugerido:
#   0 5 1 * * /home/ec2-user/multibet/run_refresh_meta_token.sh
#
# Log: pipelines/logs/refresh_meta_token_YYYY-MM.log
#
# Pre-requisitos no .env:
#   META_ADS_ACCESS_TOKEN  — token atual (user token, Caixinha)
#   META_APP_ID            — 1272866485031838
#   META_APP_SECRET        — secret do Caixinha
#
# Referencia: memory/reference_meta_marketing_api.md
# =================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$SCRIPT_DIR/pipelines/logs"

mkdir -p "$LOG_DIR"

LOGFILE="$LOG_DIR/refresh_meta_token_$(date +%Y-%m).log"

echo "=========================================" >> "$LOGFILE"
echo "Inicio: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOGFILE"
echo "=========================================" >> "$LOGFILE"

cd "$SCRIPT_DIR"
source venv/bin/activate

python3 pipelines/refresh_meta_token.py >> "$LOGFILE" 2>&1

EXIT_CODE=$?

echo "Fim: $(date '+%Y-%m-%d %H:%M:%S') | Exit code: $EXIT_CODE" >> "$LOGFILE"
echo "" >> "$LOGFILE"

# Exit != 0: cron pode ser configurado pra notificar (Slack/email)
exit $EXIT_CODE
