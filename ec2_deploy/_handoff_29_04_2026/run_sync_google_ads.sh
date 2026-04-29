#!/bin/bash
# =================================================================
# Sync Google Ads Spend — cron diario (03:00 BRT = 06:00 UTC)
# Puxa spend da Google Ads API e persiste em
# multibet.fact_ad_spend no Super Nova DB.
#
# Crontab:
#   0 6 * * * /home/ec2-user/multibet/run_sync_google_ads.sh
#
# Log: pipelines/logs/sync_google_ads_YYYY-MM-DD.log
# =================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$SCRIPT_DIR/pipelines/logs"

mkdir -p "$LOG_DIR"

LOGFILE="$LOG_DIR/sync_google_ads_$(date +%Y-%m-%d).log"

echo "=========================================" >> "$LOGFILE"
echo "Inicio: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOGFILE"
echo "=========================================" >> "$LOGFILE"

cd "$SCRIPT_DIR"
source venv/bin/activate

# --days 3: cobre D-1 + reprocessamentos do Google (D-2 e D-3)
python3 pipelines/sync_google_ads_spend.py --days 3 >> "$LOGFILE" 2>&1

EXIT_CODE=$?

echo "Fim: $(date '+%Y-%m-%d %H:%M:%S') | Exit code: $EXIT_CODE" >> "$LOGFILE"
echo "" >> "$LOGFILE"

exit $EXIT_CODE
