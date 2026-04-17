#!/bin/bash
# =================================================================
# DEPLOY: views_front (game_image_mapping enriquecido + vw_front_*)
# Demanda: CTO Gabriel Barbosa (via Castrin) — 17/04/2026
#
# O QUE FAZ:
#   - Aplica DDL v2 do game_image_mapping (ADD COLUMNS — idempotente)
#   - Atualiza pipelines/game_image_mapper.py
#   - Cria/atualiza wrapper run_views_front.sh
#   - Adiciona cron 4h (00/04/08/12/16/20 BRT)
#   - Aplica views vw_front_* (CREATE OR REPLACE — idempotente)
#
# O QUE NAO FAZ:
#   - NAO altera grandes_ganhos.py (so usa o mesmo game_image_mapping)
#   - NAO altera outros pipelines/cron jobs
#   - NAO faz scraper (jogos.csv vai por scp manual quando precisa)
# =================================================================
set -e

echo "========================================="
echo "DEPLOY VIEWS_FRONT (game_image_mapping v2)"
echo "========================================="

cd /home/ec2-user/multibet

# 1. Pre-requisitos
echo "[1/6] Verificando pre-requisitos..."
ERRORS=0
[ ! -d "venv" ]                && { echo "  ERRO: venv/ nao existe"; ERRORS=1; }
[ ! -f "db/athena.py" ]        && { echo "  ERRO: db/athena.py nao existe"; ERRORS=1; }
[ ! -f "db/supernova.py" ]     && { echo "  ERRO: db/supernova.py nao existe"; ERRORS=1; }
[ ! -f ".env" ]                && { echo "  ERRO: .env nao existe"; ERRORS=1; }
[ ! -f "pipelines/jogos.csv" ] && { echo "  AVISO: jogos.csv nao existe (jogos sem imagem ate proximo scraper)"; }
[ $ERRORS -eq 1 ] && { echo "  ABORTANDO"; exit 1; }
echo "  OK"

# 2. Backup do pipeline antigo (se existir)
echo "[2/6] Backup do pipeline antigo..."
[ -f "pipelines/game_image_mapper.py" ] && \
    cp pipelines/game_image_mapper.py "pipelines/game_image_mapper.py.bkp_$(date +%Y%m%d_%H%M%S)" && \
    echo "  OK: backup criado"

# 3. Aplicar DDL v2 (ALTER TABLE — idempotente)
echo "[3/6] Aplicando DDL v2 (ALTER TABLE game_image_mapping)..."
source venv/bin/activate
python3 << 'PYEOF'
import sys
sys.path.insert(0, '/home/ec2-user/multibet')
from db.supernova import get_supernova_connection
with open('/home/ec2-user/multibet/sql/ddl_game_image_mapping_v2.sql') as f:
    sql = f.read()
ssh, conn = get_supernova_connection()
try:
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    print("  OK: ALTER TABLE aplicado")
finally:
    conn.close(); ssh.close()
PYEOF

# 4. Rodar pipeline (popula colunas novas)
echo "[4/6] Rodando game_image_mapper.py para popular colunas novas..."
python3 pipelines/game_image_mapper.py | tail -25
[ $? -ne 0 ] && { echo "  ERRO: pipeline falhou"; exit 1; }
echo "  OK: pipeline executado"

# 5. Aplicar views vw_front_* (CREATE OR REPLACE — idempotente)
echo "[5/6] Criando views vw_front_*..."
python3 << 'PYEOF'
import sys
sys.path.insert(0, '/home/ec2-user/multibet')
from db.supernova import get_supernova_connection, execute_supernova
with open('/home/ec2-user/multibet/sql/ddl_views_front.sql') as f:
    sql = f.read()
ssh, conn = get_supernova_connection()
try:
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    print("  OK: views criadas")
finally:
    conn.close(); ssh.close()

# Smoke test rapido
for vw in ["vw_front_top_24h", "vw_front_live_casino", "vw_front_by_vendor",
           "vw_front_by_category", "vw_front_jackpot"]:
    r = execute_supernova(f"SELECT COUNT(*) FROM multibet.{vw}", fetch=True)
    print(f"  {vw:<28} {r[0][0]} linhas")
PYEOF

# 6. Cron 4h (00, 04, 08, 12, 16, 20 BRT = 03, 07, 11, 15, 19, 23 UTC)
echo "[6/6] Configurando cron 4h..."
chmod +x run_views_front.sh
CRON_LINE="0 3,7,11,15,19,23 * * * /home/ec2-user/multibet/run_views_front.sh"
if crontab -l 2>/dev/null | grep -q "run_views_front"; then
    echo "  Cron existente. Substituindo..."
    (crontab -l 2>/dev/null | grep -v "run_views_front"; echo "$CRON_LINE") | crontab -
else
    (crontab -l 2>/dev/null; echo "$CRON_LINE") | crontab -
fi
echo "  OK: cron configurado"

echo ""
echo "========================================="
echo "DEPLOY COMPLETO!"
echo "========================================="
echo ""
echo "Crontab atual (filtrado):"
crontab -l | grep -E "views_front|grandes_ganhos|game_image" || true
echo ""
echo "Logs em: pipelines/logs/views_front_*.log"
echo "Proximo refresh: 4h (rodada manual: ./run_views_front.sh)"
echo "========================================="
