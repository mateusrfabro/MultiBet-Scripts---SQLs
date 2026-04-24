#!/bin/bash
# =============================================================================
# rollback_views_casino_sportsbook.sh
# =============================================================================
# Rollback do deploy views casino/sportsbook v4.1.
#
# O que faz:
#   1. Remove cron entries deste deploy (marcadas por # VIEWS_CASINO_SPORTSBOOK_V4)
#   2. MANTEM a pasta /home/ec2-user/multibet/views_casino_sportsbook/
#      (por seguranca — nao deleta codigo nem logs)
#   3. NAO toca nas silver tables ou views gold ja populadas no Super Nova DB
#      (elas permanecem funcionais com os dados da ultima execucao manual)
#
# Para deletar a pasta tambem:
#   bash rollback_views_casino_sportsbook.sh --purge
# =============================================================================

set -e

DEPLOY_DIR="/home/ec2-user/multibet/views_casino_sportsbook"
BACKUP_DIR="/home/ec2-user/multibet/backups"
TS=$(date +%Y%m%d-%H%M%S)
CRONTAB_BACKUP="${BACKUP_DIR}/crontab.pre-rollback.${TS}.txt"
MARKER="# VIEWS_CASINO_SPORTSBOOK_V4"

echo "================================================================"
echo "Rollback Views Casino & Sportsbook v4.1"
echo "================================================================"

# Backup do crontab atual antes de qualquer mudanca
mkdir -p "${BACKUP_DIR}"
crontab -l > "${CRONTAB_BACKUP}" 2>/dev/null || echo "# (crontab vazio)" > "${CRONTAB_BACKUP}"
echo "✓ Crontab pre-rollback salvo em ${CRONTAB_BACKUP}"

# Remove entries deste deploy do crontab (tudo entre a linha marker e a proxima linha vazia)
TMP_CRON=$(mktemp)
crontab -l 2>/dev/null | awk -v marker="${MARKER}" '
    $0 ~ marker {skip=1; next}
    skip && /^[[:space:]]*$/ {skip=0; next}
    skip && /^#/ {next}
    skip && /run_views_casino_sportsbook/ {next}
    skip {skip=0}
    {print}
' > "${TMP_CRON}"

crontab "${TMP_CRON}"
rm -f "${TMP_CRON}"
echo "✓ Cron entries removidas"

# --purge: deleta pasta tambem
if [ "$1" == "--purge" ]; then
    echo "⚠️  Modo --purge: deletando ${DEPLOY_DIR}"
    rm -rf "${DEPLOY_DIR}"
    echo "✓ Pasta removida"
fi

echo ""
echo "================================================================"
echo "Rollback completo"
echo "================================================================"
echo ""
echo "Verificacao:"
echo "  crontab -l | grep -c VIEWS_CASINO_SPORTSBOOK_V4  # deve ser 0"
echo ""
echo "Restauracao (se precisar desfazer o rollback):"
echo "  bash ${DEPLOY_DIR}/deploy_views_casino_sportsbook.sh"
echo ""
echo "Atencao: as silver tables e views gold no Super Nova DB"
echo "permanecem funcionais com os dados da ultima execucao."
