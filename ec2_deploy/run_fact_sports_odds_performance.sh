#!/bin/bash
# =================================================================
# fact_sports_odds_performance — cron diario (05:00 BRT = 08:00 UTC)
# Pipeline incremental (rolling window 7 dias) Sportsbook Win/Loss
# por faixa de odds. Persiste em multibet.fact_sports_odds_performance.
#
# Crontab:
#   0 8 * * * /home/ec2-user/multibet/run_fact_sports_odds_performance.sh
#
# Log: pipelines/logs/fact_sports_odds_performance_YYYY-MM-DD.log
# =================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$SCRIPT_DIR/pipelines/logs"

mkdir -p "$LOG_DIR"

LOGFILE="$LOG_DIR/fact_sports_odds_performance_$(date +%Y-%m-%d).log"

echo "=========================================" >> "$LOGFILE"
echo "Inicio: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOGFILE"
echo "=========================================" >> "$LOGFILE"

cd "$SCRIPT_DIR"
source venv/bin/activate

# Modo incremental (rolling window 7 dias por padrao)
python3 pipelines/fact_sports_odds_performance.py >> "$LOGFILE" 2>&1

EXIT_CODE=$?

echo "Fim: $(date '+%Y-%m-%d %H:%M:%S') | Exit code: $EXIT_CODE" >> "$LOGFILE"
echo "" >> "$LOGFILE"

exit $EXIT_CODE
