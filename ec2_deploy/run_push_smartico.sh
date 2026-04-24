#!/bin/bash
# =================================================================
# run_push_smartico.sh — Cron wrapper
# Publica tags da Matriz de Risco no Smartico via S2S API.
# Roda APOS o pipeline da matriz (cron 05:30 UTC = 02:30 BRT).
#
# Comportamento:
#   - Envia apenas diffs (snapshot atual vs anterior)
#   - skip_cjm=True SEMPRE (nao dispara automations)
#   - Log em pipelines/logs/push_smartico_YYYY-MM-DD.log
# =================================================================
set -e

cd /home/ec2-user/multibet
source venv/bin/activate

DATE=$(date +%Y-%m-%d)
LOG="pipelines/logs/push_smartico_${DATE}.log"

echo "[$(date)] Iniciando push Smartico..." >> "$LOG"
python3 pipelines/push_risk_to_smartico.py 2>&1 | tee -a "$LOG"
echo "[$(date)] Finalizado." >> "$LOG"
