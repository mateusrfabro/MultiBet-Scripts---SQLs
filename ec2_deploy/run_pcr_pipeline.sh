#!/bin/bash
# =================================================================
# PCR (Player Credit Rating) — cron diario (03:30 BRT = 06:30 UTC)
# Calcula ratings E-S e persiste no Super Nova DB (TRUNCATE+INSERT).
#
# Crontab:
#   30 6 * * * /home/ec2-user/multibet/run_pcr_pipeline.sh
#
# Log: pipelines/logs/pcr_pipeline_YYYY-MM-DD.log
# =================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$SCRIPT_DIR/pipelines/logs"

mkdir -p "$LOG_DIR"

LOGFILE="$LOG_DIR/pcr_pipeline_$(date +%Y-%m-%d).log"

echo "=========================================" >> "$LOGFILE"
echo "Inicio: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOGFILE"
echo "=========================================" >> "$LOGFILE"

cd "$SCRIPT_DIR"
source venv/bin/activate

python3 pipelines/pcr_pipeline.py >> "$LOGFILE" 2>&1

EXIT_CODE=$?

echo "Fim: $(date '+%Y-%m-%d %H:%M:%S') | Exit code: $EXIT_CODE" >> "$LOGFILE"
echo "" >> "$LOGFILE"

exit $EXIT_CODE
