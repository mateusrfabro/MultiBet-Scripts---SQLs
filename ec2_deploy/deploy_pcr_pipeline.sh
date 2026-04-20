#!/bin/bash
# =================================================================
# DEPLOY: PCR (Player Credit Rating) na EC2 ETL
#
# PRE-REQUISITO: Copiar arquivos para a EC2 ANTES de rodar este script.
#   scp -i ~/Downloads/etl-key.pem pipelines/pcr_pipeline.py ec2-user@<IP>:/home/ec2-user/multibet/pipelines/
#   scp -i ~/Downloads/etl-key.pem ec2_deploy/run_pcr_pipeline.sh ec2-user@<IP>:/home/ec2-user/multibet/
#
# O QUE FAZ:
#   - Verifica pre-requisitos (venv, db/, .env, deps)
#   - Da permissao ao run_pcr_pipeline.sh
#   - Testa o pipeline com --dry-run (apenas CSV, sem PostgreSQL)
#   - Adiciona cron diario (06:30 UTC = 03:30 BRT)
#
# O QUE NAO FAZ:
#   - NAO altera db/ (athena.py, supernova.py ja existem)
#   - NAO altera outros pipelines
#   - NAO altera outras entradas do cron
#   - NAO instala pacotes
# =================================================================
set -e

echo "========================================="
echo "DEPLOY PCR — PLAYER CREDIT RATING (EC2)"
echo "========================================="

cd /home/ec2-user/multibet

# 1. Verificar pre-requisitos
echo "[1/5] Verificando pre-requisitos..."
ERRORS=0

if [ ! -d "venv" ]; then
    echo "  ERRO: venv/ nao existe"
    ERRORS=1
fi
if [ ! -f "db/athena.py" ]; then
    echo "  ERRO: db/athena.py nao existe"
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
if [ ! -f "pipelines/pcr_pipeline.py" ]; then
    echo "  ERRO: pipelines/pcr_pipeline.py nao existe (SCP primeiro)"
    ERRORS=1
fi
if [ ! -f "run_pcr_pipeline.sh" ]; then
    echo "  ERRO: run_pcr_pipeline.sh nao existe (SCP primeiro)"
    ERRORS=1
fi

# Verifica venv e deps
source venv/bin/activate
if ! python3 -c "import pyathena" 2>/dev/null; then
    echo "  ERRO: pyathena nao instalado no venv"
    ERRORS=1
fi
if ! python3 -c "import psycopg2" 2>/dev/null; then
    echo "  ERRO: psycopg2 nao instalado no venv"
    ERRORS=1
fi
if ! python3 -c "import sshtunnel" 2>/dev/null; then
    echo "  ERRO: sshtunnel nao instalado no venv"
    ERRORS=1
fi
if ! python3 -c "import numpy" 2>/dev/null; then
    echo "  ERRO: numpy nao instalado no venv"
    ERRORS=1
fi

# Verifica variaveis Athena no .env
if ! grep -q "ATHENA_AWS_ACCESS_KEY_ID" .env; then
    echo "  ERRO: ATHENA_AWS_ACCESS_KEY_ID nao encontrado no .env"
    ERRORS=1
fi

if [ $ERRORS -eq 1 ]; then
    echo ""
    echo "  ABORTANDO: corrija os erros acima antes de continuar"
    exit 1
fi

echo "  OK: todos os pre-requisitos atendidos"

# 2. Permissoes
echo "[2/5] Configurando permissoes..."
chmod +x run_pcr_pipeline.sh
echo "  OK: run_pcr_pipeline.sh executavel"

# 3. Criar diretorios
echo "[3/5] Criando diretorios..."
mkdir -p output
mkdir -p pipelines/logs
echo "  OK: output/ e pipelines/logs/ criados"

# 4. Teste dry-run
echo "[4/5] Testando pipeline (--dry-run)..."
echo "  Isso vai executar a query no Athena (pode levar 5-10 min)."
echo "  O resultado sera salvo em output/ sem gravar no PostgreSQL."
read -p "  Deseja rodar o teste agora? (s/N): " CONFIRM
if [ "$CONFIRM" = "s" ] || [ "$CONFIRM" = "S" ]; then
    python3 pipelines/pcr_pipeline.py --dry-run
    if [ $? -eq 0 ]; then
        echo "  OK: teste dry-run passou!"
        echo "  Verifique o CSV em output/"
        ls -la output/pcr_ratings_*_FINAL.csv 2>/dev/null
    else
        echo "  AVISO: teste falhou. Verifique os logs acima."
        echo "  O cron sera configurado mesmo assim (pode corrigir e re-testar)."
    fi
else
    echo "  SKIP: teste adiado. Pode rodar manualmente depois:"
    echo "    cd /home/ec2-user/multibet"
    echo "    source venv/bin/activate"
    echo "    python3 pipelines/pcr_pipeline.py --dry-run"
fi

# 5. Configurar cron (06:30 UTC = 03:30 BRT)
echo "[5/5] Configurando cron diario..."
CRON_LINE="30 6 * * * /home/ec2-user/multibet/run_pcr_pipeline.sh"
if crontab -l 2>/dev/null | grep -q "run_pcr_pipeline"; then
    echo "  Cron existente encontrado. Substituindo..."
    (crontab -l 2>/dev/null | grep -v "pcr_pipeline"; echo "$CRON_LINE") | crontab -
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
echo "Horario: 06:30 UTC (03:30 BRT) — diario"
echo ""
echo "Comandos uteis:"
echo "  # Rodar manualmente (com gravacao no PostgreSQL):"
echo "  cd /home/ec2-user/multibet && source venv/bin/activate"
echo "  python3 pipelines/pcr_pipeline.py"
echo ""
echo "  # Rodar apenas dry-run (sem PostgreSQL):"
echo "  python3 pipelines/pcr_pipeline.py --dry-run"
echo ""
echo "  # Ver logs:"
echo "  tail -f pipelines/logs/pcr_pipeline_\$(date +%Y-%m-%d).log"
echo ""
echo "  # Consultar no banco (via psql ou DBeaver):"
echo "  SELECT rating, COUNT(*), ROUND(AVG(pvs),1) FROM multibet.pcr_ratings GROUP BY rating;"
echo "  SELECT * FROM multibet.pcr_atual WHERE c_category = 'active' ORDER BY pvs DESC LIMIT 20;"
echo "  SELECT * FROM multibet.pcr_resumo;"
echo "========================================="
