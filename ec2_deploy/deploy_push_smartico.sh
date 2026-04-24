#!/bin/bash
# =================================================================
# DEPLOY: Push Risk Matrix → Smartico (EC2 ETL)
#
# PRE-REQUISITO: Copiar arquivos para a EC2 ANTES de rodar este script.
#   scp db/smartico_api.py ec2-user@<IP>:/home/ec2-user/multibet/db/
#   scp pipelines/push_risk_to_smartico.py ec2-user@<IP>:/home/ec2-user/multibet/pipelines/
#   scp run_push_smartico.sh ec2-user@<IP>:/home/ec2-user/multibet/
#
# O QUE FAZ:
#   - Verifica pre-requisitos (venv, db/smartico_api.py, .env com token)
#   - Da permissao ao run_push_smartico.sh
#   - Testa com --dry-run
#   - Adiciona cron diario (05:30 UTC = 02:30 BRT)
#
# O QUE NAO FAZ:
#   - NAO altera outros pipelines ou crons existentes
#   - NAO instala pacotes novos (requests ja deve estar no venv)
# =================================================================
set -e

echo "========================================="
echo "DEPLOY PUSH SMARTICO (EC2 ETL)"
echo "========================================="

cd /home/ec2-user/multibet

# 1. Verificar pre-requisitos
echo "[1/5] Verificando pre-requisitos..."
ERRORS=0

if [ ! -d "venv" ]; then
    echo "  ERRO: venv/ nao existe"
    ERRORS=1
fi
if [ ! -f "db/smartico_api.py" ]; then
    echo "  ERRO: db/smartico_api.py nao existe (SCP primeiro)"
    ERRORS=1
fi
if [ ! -f "db/supernova.py" ]; then
    echo "  ERRO: db/supernova.py nao existe"
    ERRORS=1
fi
if [ ! -f ".env" ]; then
    echo "  ERRO: .env nao existe"
    ERRORS=1
fi
if [ ! -f "pipelines/push_risk_to_smartico.py" ]; then
    echo "  ERRO: pipelines/push_risk_to_smartico.py nao existe (SCP primeiro)"
    ERRORS=1
fi
if [ ! -f "run_push_smartico.sh" ]; then
    echo "  ERRO: run_push_smartico.sh nao existe (SCP primeiro)"
    ERRORS=1
fi

# Verifica token Smartico no .env
if ! grep -q "SMARTICO_API_TOKEN" .env; then
    echo "  ERRO: SMARTICO_API_TOKEN nao encontrado no .env"
    echo "  Adicione ao .env:"
    echo "    SMARTICO_API_URL=https://apis6.smartico.ai/api/external/events/v2"
    echo "    SMARTICO_API_TOKEN=<token>"
    echo "    SMARTICO_BRAND_ID=multibet"
    ERRORS=1
else
    echo "  OK: SMARTICO_API_TOKEN encontrado no .env"
fi

# Verifica venv e deps
source venv/bin/activate
if ! python3 -c "import requests" 2>/dev/null; then
    echo "  AVISO: requests nao instalado. Instalando..."
    pip install requests
fi

if [ $ERRORS -eq 1 ]; then
    echo ""
    echo "  ABORTANDO: corrija os erros acima antes de continuar"
    exit 1
fi

echo "  OK: todos os pre-requisitos atendidos"

# 2. Permissoes
echo "[2/5] Configurando permissoes..."
chmod +x run_push_smartico.sh
echo "  OK: run_push_smartico.sh executavel"

# 3. Criar diretorios
echo "[3/5] Criando diretorios..."
mkdir -p pipelines/logs
mkdir -p output
echo "  OK: diretorios criados"

# 4. Teste dry-run
echo "[4/5] Testando push (--dry-run)..."
echo "  Isso vai conectar ao PostgreSQL e simular o envio."
read -p "  Deseja rodar o teste agora? (s/N): " CONFIRM
if [ "$CONFIRM" = "s" ] || [ "$CONFIRM" = "S" ]; then
    python3 pipelines/push_risk_to_smartico.py --dry-run
    if [ $? -eq 0 ]; then
        echo "  OK: teste dry-run passou!"
    else
        echo "  AVISO: teste falhou. Verifique os logs acima."
    fi
else
    echo "  SKIP: teste adiado."
fi

# 5. Configurar cron (05:30 UTC = 02:30 BRT, 30 min apos pipeline)
echo "[5/5] Configurando cron diario..."
CRON_LINE="30 5 * * * /home/ec2-user/multibet/run_push_smartico.sh"
if crontab -l 2>/dev/null | grep -q "run_push_smartico"; then
    echo "  Cron existente encontrado. Substituindo..."
    (crontab -l 2>/dev/null | grep -v "push_smartico"; echo "$CRON_LINE") | crontab -
    echo "  OK: cron atualizado"
else
    (crontab -l 2>/dev/null; echo "$CRON_LINE") | crontab -
    echo "  OK: cron adicionado"
fi

echo ""
echo "========================================="
echo "DEPLOY COMPLETO!"
echo "========================================="
echo ""
echo "Crontab atual:"
crontab -l
echo ""
echo "Horario: 05:30 UTC (02:30 BRT) — diario, 30 min apos pipeline da matriz"
echo ""
echo "Comandos uteis:"
echo "  # Rodar manualmente (envia diffs):"
echo "  cd /home/ec2-user/multibet && source venv/bin/activate"
echo "  python3 pipelines/push_risk_to_smartico.py"
echo ""
echo "  # Rodar dry-run:"
echo "  python3 pipelines/push_risk_to_smartico.py --dry-run"
echo ""
echo "  # Forcar reenvio de todos:"
echo "  python3 pipelines/push_risk_to_smartico.py --force"
echo ""
echo "  # Ver logs:"
echo "  tail -f pipelines/logs/push_smartico_\$(date +%Y-%m-%d).log"
echo "========================================="
