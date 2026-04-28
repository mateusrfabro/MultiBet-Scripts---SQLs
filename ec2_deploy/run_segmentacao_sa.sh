#!/bin/bash
# =================================================================
# Segmentacao A+S Diaria — cron 04:00 BRT = 07:00 UTC
# 30 minutos APOS o PCR upstream (que roda no orquestrador as 03:30 BRT).
#
# Fluxo:
#   1. PCR roda no orquestrador (03:30 BRT) -> popula multibet.pcr_atual
#   2. ESTE script roda na EC2 ETL (04:00 BRT)
#      -> filtra A+S, joina matriz_risco, calcula tendencia
#      -> persiste em multibet.segmentacao_sa_diaria (incremental)
#      -> atualiza multibet.celula_monitor_diario (flag 3 rodadas)
#      -> gera CSV BR + legenda
#      -> envia email aos 6 destinatarios CRM
#
# Crontab:
#   0 7 * * * /home/ec2-user/multibet/run_segmentacao_sa.sh
#
# Log: pipelines/logs/segmentacao_sa_YYYY-MM-DD.log
# =================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$SCRIPT_DIR/pipelines/logs"

mkdir -p "$LOG_DIR"

LOGFILE="$LOG_DIR/segmentacao_sa_$(date +%Y-%m-%d).log"

echo "=========================================" >> "$LOGFILE"
echo "Inicio: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOGFILE"
echo "=========================================" >> "$LOGFILE"

cd "$SCRIPT_DIR"
source venv/bin/activate

# v2 (28/04/2026): pipeline expandido para 57 colunas + push Smartico
# external_markers (tags SEG_*) para toda a base PCR (~136k jogadores).
#
# Modo padrao: --push-smartico --smartico-dry-run (gera JSON em reports/,
# NAO envia tags). Quando Raphael (CRM) validar o canary, trocar para
# --smartico-confirm pra liberar full push.
#
# CHEATSHEET:
#   --no-email                   pula envio email
#   --no-db                      pula persistencia banco
#   --push-smartico              habilita Smartico push
#   --smartico-dry-run           gera JSON, nao envia (DEFAULT seguro)
#   --smartico-canary            so 1 jogador (rating A estavel)
#   --smartico-confirm           OBRIGATORIO pra envio real

python3 pipelines/segmentacao_sa_diaria.py \
    --push-smartico \
    --smartico-dry-run \
    >> "$LOGFILE" 2>&1

EXIT_CODE=$?

echo "Fim: $(date '+%Y-%m-%d %H:%M:%S') | Exit code: $EXIT_CODE" >> "$LOGFILE"

exit $EXIT_CODE
