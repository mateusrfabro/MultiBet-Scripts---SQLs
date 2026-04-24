#!/bin/bash
# =================================================================
# DEPLOY: fact_sports_odds_performance na EC2 ETL
#
# PRE-REQUISITO: Copiar arquivos para a EC2 ANTES de rodar este script.
#   scp -i ~/Downloads/etl-key.pem ec2_deploy/pipelines/fact_sports_odds_performance.py ec2-user@<IP>:/home/ec2-user/multibet/pipelines/
#   scp -i ~/Downloads/etl-key.pem ec2_deploy/run_fact_sports_odds_performance.sh ec2-user@<IP>:/home/ec2-user/multibet/
#
# O QUE FAZ:
#   - Verifica pre-requisitos (venv, db/, .env, deps)
#   - Da permissao ao run script
#   - Cria a tabela no Super Nova DB (se nao existir)
#   - Roda backfill desde 2026-01-01 (UMA VEZ)
#   - Adiciona cron diario (08:00 UTC = 05:00 BRT)
#
# O QUE NAO FAZ:
#   - NAO altera db/ (athena.py, supernova.py ja existem)
#   - NAO altera outros pipelines
#   - NAO altera outras entradas do cron
#   - NAO instala pacotes
# =================================================================
set -e

echo "========================================="
echo "DEPLOY fact_sports_odds_performance (EC2)"
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
if [ ! -f "pipelines/fact_sports_odds_performance.py" ]; then
    echo "  ERRO: pipelines/fact_sports_odds_performance.py nao existe (SCP primeiro)"
    ERRORS=1
fi
if [ ! -f "run_fact_sports_odds_performance.sh" ]; then
    echo "  ERRO: run_fact_sports_odds_performance.sh nao existe (SCP primeiro)"
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

if [ $ERRORS -eq 1 ]; then
    echo ""
    echo "  ABORTANDO: corrija os erros acima antes de continuar"
    exit 1
fi

echo "  OK: todos os pre-requisitos atendidos"

# 2. Permissoes
echo "[2/5] Configurando permissoes..."
chmod +x run_fact_sports_odds_performance.sh
echo "  OK: run_fact_sports_odds_performance.sh executavel"

# 3. Criar diretorios
echo "[3/5] Criando diretorios..."
mkdir -p output
mkdir -p pipelines/logs
echo "  OK: output/ e pipelines/logs/ criados"

# 4. Backfill (uma vez, desde 2026-01-01)
echo "[4/5] Backfill desde 2026-01-01..."
echo "  Isso vai executar query no Athena (~10 min) e popular ~330 dias x 4 faixas x 2 modos."
read -p "  Deseja rodar o backfill agora? (s/N): " CONFIRM
if [ "$CONFIRM" = "s" ] || [ "$CONFIRM" = "S" ]; then
    python3 pipelines/fact_sports_odds_performance.py --backfill
    if [ $? -eq 0 ]; then
        echo "  OK: backfill concluido!"
    else
        echo "  AVISO: backfill falhou. Verifique os logs acima."
        echo "  O cron sera configurado mesmo assim (rolling window vai recuperar)."
    fi
else
    echo "  SKIP: backfill adiado. Pode rodar manualmente depois:"
    echo "    cd /home/ec2-user/multibet"
    echo "    source venv/bin/activate"
    echo "    python3 pipelines/fact_sports_odds_performance.py --backfill"
fi

# 5. Configurar cron (08:00 UTC = 05:00 BRT)
echo "[5/5] Configurando cron diario..."
CRON_LINE="0 8 * * * /home/ec2-user/multibet/run_fact_sports_odds_performance.sh"
if crontab -l 2>/dev/null | grep -q "run_fact_sports_odds_performance"; then
    echo "  Cron existente encontrado. Substituindo..."
    (crontab -l 2>/dev/null | grep -v "fact_sports_odds_performance"; echo "$CRON_LINE") | crontab -
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
echo "Crontab atual (filtrado):"
crontab -l | grep -E "fact_sports_odds|^#" | head -5
echo ""
echo "Horario: 08:00 UTC (05:00 BRT) — diario"
echo "Janela: nao conflita com outros pipelines (PCR 06:30, smartico 05:30, views 07:30)"
echo ""
echo "Comandos uteis:"
echo "  # Rodar manualmente (incremental rolling window 7 dias):"
echo "  cd /home/ec2-user/multibet && source venv/bin/activate"
echo "  python3 pipelines/fact_sports_odds_performance.py"
echo ""
echo "  # Rodar backfill completo (desde 2026-01-01):"
echo "  python3 pipelines/fact_sports_odds_performance.py --backfill"
echo ""
echo "  # Rolling window custom (ex: 14 dias):"
echo "  python3 pipelines/fact_sports_odds_performance.py --days 14"
echo ""
echo "  # Dry-run (so query, sem persistir):"
echo "  python3 pipelines/fact_sports_odds_performance.py --dry-run"
echo ""
echo "  # Ver logs:"
echo "  tail -f pipelines/logs/fact_sports_odds_performance_\$(date +%Y-%m-%d).log"
echo ""
echo "  # Consultar no banco:"
echo "  SELECT odds_range, bet_mode, SUM(total_bets), SUM(ggr), AVG(hold_rate_pct)"
echo "  FROM multibet.fact_sports_odds_performance"
echo "  GROUP BY 1,2 ORDER BY 1,2;"
echo ""
echo "  SELECT * FROM multibet.vw_odds_performance_summary;"
echo "========================================="
