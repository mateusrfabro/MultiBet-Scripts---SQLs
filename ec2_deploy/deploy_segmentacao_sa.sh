#!/bin/bash
# =================================================================
# DEPLOY: Segmentacao A+S Diaria na EC2 ETL
#
# PRE-REQUISITO: Copiar arquivos para a EC2 ANTES de rodar este script:
#   scp -i ~/Downloads/etl-key.pem pipelines/segmentacao_sa_diaria.py \
#       ec2-user@<IP>:/home/ec2-user/multibet/pipelines/
#   scp -i ~/Downloads/etl-key.pem db/email_sender.py \
#       ec2-user@<IP>:/home/ec2-user/multibet/db/
#   scp -i ~/Downloads/etl-key.pem ec2_deploy/run_segmentacao_sa.sh \
#       ec2-user@<IP>:/home/ec2-user/multibet/
#
# O QUE FAZ:
#   - Verifica pre-requisitos (venv, db/, .env, modulos Python)
#   - Da permissao ao run_segmentacao_sa.sh
#   - Testa o pipeline com --no-email (gera CSV + grava banco, sem disparar email)
#   - Adiciona cron diario (07:00 UTC = 04:00 BRT)
#
# CONFIG SMTP (REQUISITO PRA EMAIL FUNCIONAR):
#   Adicionar no .env da EC2:
#     SMTP_HOST=...
#     SMTP_PORT=587
#     SMTP_USER=...
#     SMTP_PASS=...
#     SMTP_FROM=...
#     SMTP_USE_SSL=false
# =================================================================
set -e

echo "========================================="
echo "DEPLOY SEGMENTACAO A+S DIARIA (EC2)"
echo "========================================="

cd /home/ec2-user/multibet

# 1. Verificar pre-requisitos
echo "[1/5] Verificando pre-requisitos..."
ERRORS=0

if [ ! -d "venv" ]; then echo "  ERRO: venv/ nao existe"; ERRORS=1; fi
if [ ! -f "db/supernova.py" ]; then echo "  ERRO: db/supernova.py"; ERRORS=1; fi
if [ ! -f "db/email_sender.py" ]; then echo "  ERRO: db/email_sender.py (SCP primeiro)"; ERRORS=1; fi
if [ ! -f ".env" ]; then echo "  ERRO: .env"; ERRORS=1; fi
if [ ! -f "pipelines/segmentacao_sa_diaria.py" ]; then
    echo "  ERRO: pipelines/segmentacao_sa_diaria.py (SCP primeiro)"; ERRORS=1
fi
if [ ! -f "run_segmentacao_sa.sh" ]; then
    echo "  ERRO: run_segmentacao_sa.sh (SCP primeiro)"; ERRORS=1
fi

source venv/bin/activate
for mod in psycopg2 sshtunnel pandas numpy; do
    if ! python3 -c "import $mod" 2>/dev/null; then
        echo "  ERRO: $mod nao instalado no venv"; ERRORS=1
    fi
done

if ! grep -q "SMTP_HOST" .env; then
    echo "  AVISO: SMTP_HOST nao encontrado no .env — email NAO sera enviado"
    echo "         (CSV vai ser gerado e gravado no banco normalmente)"
fi

if [ $ERRORS -eq 1 ]; then
    echo ""
    echo "  ABORTANDO: corrija os erros acima"
    exit 1
fi
echo "  OK: pre-requisitos atendidos"

# 2. Permissoes
echo "[2/5] Configurando permissoes..."
chmod +x run_segmentacao_sa.sh
echo "  OK: run_segmentacao_sa.sh executavel"

# 3. Diretorios
echo "[3/5] Criando diretorios..."
mkdir -p output pipelines/logs
echo "  OK"

# 4. Smoke test (--no-email pra nao disparar antes de validar)
echo "[4/5] Testando pipeline (--no-email)..."
read -p "  Rodar smoke test agora? (s/N): " CONFIRM
if [ "$CONFIRM" = "s" ] || [ "$CONFIRM" = "S" ]; then
    python3 pipelines/segmentacao_sa_diaria.py --no-email
    if [ $? -eq 0 ]; then
        echo "  OK: smoke test passou"
        ls -la output/players_segmento_SA_*_FINAL.csv 2>/dev/null | tail -1
    else
        echo "  AVISO: smoke test falhou — verifique log acima"
    fi
else
    echo "  SKIP: rodar manualmente depois com:"
    echo "    cd /home/ec2-user/multibet && source venv/bin/activate"
    echo "    python3 pipelines/segmentacao_sa_diaria.py --no-email"
fi

# 5. Cron (07:00 UTC = 04:00 BRT)
echo "[5/5] Configurando cron diario..."
CRON_LINE="0 7 * * * /home/ec2-user/multibet/run_segmentacao_sa.sh"
if crontab -l 2>/dev/null | grep -q "run_segmentacao_sa"; then
    echo "  Cron existente encontrado. Substituindo..."
    (crontab -l 2>/dev/null | grep -v "segmentacao_sa"; echo "$CRON_LINE") | crontab -
else
    (crontab -l 2>/dev/null; echo "$CRON_LINE") | crontab -
fi
echo "  OK: cron 07:00 UTC (04:00 BRT) configurado"

echo ""
echo "========================================="
echo "DEPLOY COMPLETO!"
echo "========================================="
echo ""
echo "Crontab atual:"
crontab -l
echo ""
echo "Comandos uteis:"
echo "  # Rodar manualmente (full producao com email):"
echo "  cd /home/ec2-user/multibet && source venv/bin/activate"
echo "  python3 pipelines/segmentacao_sa_diaria.py"
echo ""
echo "  # Rodar sem email (CSV + banco):"
echo "  python3 pipelines/segmentacao_sa_diaria.py --no-email"
echo ""
echo "  # Logs:"
echo "  tail -f pipelines/logs/segmentacao_sa_\$(date +%Y-%m-%d).log"
echo ""
echo "  # Validar no banco:"
echo "  SELECT rating, tendencia, COUNT(*) FROM multibet.segmentacao_sa_atual"
echo "    GROUP BY rating, tendencia ORDER BY rating, tendencia;"
echo "  SELECT * FROM multibet.celula_monitor_diario"
echo "    WHERE flag_investigar = true;"
echo "========================================="
