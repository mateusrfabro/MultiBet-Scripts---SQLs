#!/bin/bash
# ============================================================
# Anti-Abuse Bot — Campanha Multiverso
# Roda em loop a cada 5 minutos, envia alerta no Slack
# quando detecta jogadores com risco ALTO.
#
# Uso:
#   ./run_anti_abuse.sh          # inicia em background
#   ./run_anti_abuse.sh stop     # para o processo
#   ./run_anti_abuse.sh status   # verifica se está rodando
# ============================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON="$SCRIPT_DIR/venv/bin/python3"
SCRIPT="$SCRIPT_DIR/pipelines/anti_abuse_multiverso.py"
LOG_DIR="$SCRIPT_DIR/logs"
REPORTS_DIR="$SCRIPT_DIR/reports"
PID_FILE="$SCRIPT_DIR/anti_abuse.pid"
JSON_RETENTION_DAYS=7  # mantém snapshots dos últimos 7 dias

mkdir -p "$LOG_DIR" "$REPORTS_DIR"
LOG_FILE="$LOG_DIR/anti_abuse_$(date +%Y-%m-%d).log"

case "$1" in
    stop)
        if [ -f "$PID_FILE" ]; then
            PID=$(cat "$PID_FILE")
            kill "$PID" 2>/dev/null && echo "Bot parado (PID $PID)" || echo "Processo nao encontrado"
            rm -f "$PID_FILE"
        else
            echo "PID file nao encontrado — bot pode nao estar rodando"
        fi
        ;;
    status)
        if [ -f "$PID_FILE" ]; then
            PID=$(cat "$PID_FILE")
            if kill -0 "$PID" 2>/dev/null; then
                echo "Bot rodando (PID $PID)"
                echo "Log: $LOG_DIR/"
            else
                echo "PID $PID nao encontrado — bot pode ter parado"
                rm -f "$PID_FILE"
            fi
        else
            echo "Bot nao esta rodando"
        fi
        ;;
    *)
        # Inicia em background
        cd "$SCRIPT_DIR" || exit 1

        if [ -f "$PID_FILE" ]; then
            PID=$(cat "$PID_FILE")
            if kill -0 "$PID" 2>/dev/null; then
                echo "Bot ja esta rodando (PID $PID). Use '$0 stop' para parar."
                exit 1
            fi
        fi

        # Limpa snapshots JSON com mais de 7 dias antes de iniciar
        find "$REPORTS_DIR" -name "anti_abuse_*.json" -mtime +$JSON_RETENTION_DAYS -delete 2>/dev/null
        echo "Snapshots antigos removidos (retencao: ${JSON_RETENTION_DAYS} dias)"

        nohup "$PYTHON" "$SCRIPT" --loop --json >> "$LOG_FILE" 2>&1 &
        echo $! > "$PID_FILE"
        echo "Bot iniciado (PID $!) — log: $LOG_FILE | snapshots: $REPORTS_DIR"
        ;;
esac
