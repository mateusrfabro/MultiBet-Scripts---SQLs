#!/bin/bash
# ============================================================
# views_front — refresh game_image_mapping a cada 4h
# Demanda: CTO Gabriel Barbosa (via Castrin) — views vw_front_*
# ============================================================
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$SCRIPT_DIR/pipelines/logs"
mkdir -p "$LOG_DIR"
LOGFILE="$LOG_DIR/views_front_$(date +%Y-%m-%d).log"

echo "=========================================" >> "$LOGFILE"
echo "Inicio: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOGFILE"

cd "$SCRIPT_DIR"
source venv/bin/activate

# NOTA: NAO roda --scraper aqui (Playwright nao instalado na EC2 ETL).
# O CSV jogos.csv eh atualizado manualmente quando necessario.
# Se um jogo ficar SEM imagem, o validate_and_fix_images() do grandes_ganhos
# tenta CDN auto-discovery em seguida.
python3 pipelines/game_image_mapper.py >> "$LOGFILE" 2>&1
EXIT_CODE=$?

echo "Fim: $(date '+%Y-%m-%d %H:%M:%S') | Exit code: $EXIT_CODE" >> "$LOGFILE"
echo "" >> "$LOGFILE"
exit $EXIT_CODE
