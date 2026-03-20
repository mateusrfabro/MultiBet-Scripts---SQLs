"""
CRM Dashboard API — Fase 2: Camada de Visibilidade
=====================================================
Flask API para servir dados de performance de campanhas CRM.
Conexão direta ao Super Nova DB (PostgreSQL) via psycopg2.

Endpoints:
  GET /api/v1/summary          — KPIs agregados 2026
  GET /api/v1/campaigns        — Lista campanhas (filtro por dt)
  GET /api/v1/campaign/<id>    — Deep dive (funil + financeiro + comparativo)
  GET /api/v1/filters          — Listas para dropdowns (categoria, responsavel)

Uso:
    python crm_dashboard/app.py
    # http://localhost:5000
"""

import os
import sys
import json
import logging
from decimal import Decimal
from datetime import datetime, date

from flask import Flask, jsonify, request

# Path setup
APP_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(APP_DIR)
sys.path.insert(0, PROJECT_DIR)
os.chdir(PROJECT_DIR)

from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_DIR, ".env"))

import psycopg2
import psycopg2.extras
from sshtunnel import SSHTunnelForwarder

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

app = Flask(__name__)


# =============================================================================
# CONEXAO — Super Nova DB (PostgreSQL via SSH tunnel)
# =============================================================================
BASTION_HOST = "34.238.84.114"
BASTION_PORT = 22
BASTION_USER = "ec2-user"
BASTION_KEY = os.getenv("SUPERNOVA_PEM_PATH", "C:/Users/NITRO/Downloads/bastion-analytics-key.pem")

PG_HOST = "supernova-db.c8r8mcwe6zq9.us-east-1.rds.amazonaws.com"
PG_PORT = 5432
PG_DB = os.getenv("SUPERNOVA_DB", "supernova_db")
PG_USER = os.getenv("SUPERNOVA_USER", "analytics_user")
PG_PASS = os.getenv("SUPERNOVA_PASS")


def get_db():
    """Abre túnel SSH e retorna (tunnel, connection)."""
    tunnel = SSHTunnelForwarder(
        (BASTION_HOST, BASTION_PORT),
        ssh_username=BASTION_USER,
        ssh_pkey=BASTION_KEY,
        remote_bind_address=(PG_HOST, PG_PORT),
    )
    tunnel.start()
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=tunnel.local_bind_port,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS,
    )
    return tunnel, conn


def query(sql, params=None):
    """Executa SQL e retorna lista de dicts com tratamento de exceções."""
    tunnel = None
    conn = None
    try:
        tunnel, conn = get_db()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        return [dict(row) for row in rows]
    except psycopg2.Error as e:
        log.error(f"Erro de banco: {e}")
        raise
    finally:
        if conn:
            conn.close()
        if tunnel:
            tunnel.stop()


def safe_json(obj):
    """Converte tipos Python para JSON-safe."""
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: safe_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [safe_json(i) for i in obj]
    return obj


def parse_jsonb(row, *cols):
    """Converte colunas JSONB de string para dict nativo."""
    for col in cols:
        val = row.get(col)
        if isinstance(val, str):
            try:
                row[col] = json.loads(val)
            except (json.JSONDecodeError, TypeError):
                row[col] = {}
    return row


# =============================================================================
# GET /api/v1/summary — KPIs agregados 2026
# =============================================================================
@app.route("/api/v1/summary")
def api_summary():
    """NGR Total, ROI Médio Global, Custo Acumulado e Total de Campanhas."""
    try:
        rows = query("""
            SELECT
                COUNT(DISTINCT f.campanha_id)                          AS total_campanhas,
                SUM((f.financeiro->>'ngr_brl')::numeric)               AS ngr_total,
                SUM((f.financeiro->>'ggr_brl')::numeric)               AS ggr_total,
                SUM((f.financeiro->>'btr_brl')::numeric)               AS btr_total,
                SUM((f.comparativo->>'custo_total')::numeric)          AS custo_acumulado,
                AVG((f.comparativo->>'roi')::numeric)
                    FILTER (WHERE (f.comparativo->>'roi') IS NOT NULL) AS roi_medio,
                SUM((f.financeiro->>'total_users')::int)               AS users_impactados
            FROM multibet.fact_crm_daily_performance f
            WHERE f.period = 'DURING'
        """)
        return jsonify(safe_json(rows[0] if rows else {}))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# =============================================================================
# GET /api/v1/campaigns — Lista campanhas com filtros
# =============================================================================
@app.route("/api/v1/campaigns")
def api_campaigns():
    """
    Retorna lista de campanhas. Filtros opcionais via query string:
      ?categoria=Retencao
      ?dt_start=2026-01-01&dt_end=2026-03-17
    Colunas: entity_id, friendly_name, categoria, roi, ngr_incremental.
    """
    try:
        categoria = request.args.get("categoria")
        dt_start = request.args.get("dt_start")
        dt_end = request.args.get("dt_end")

        filters = []
        if categoria and categoria != "all":
            filters.append(f"d.categoria = '{categoria}'")
        if dt_start:
            filters.append(f"f.campanha_start >= '{dt_start}'")
        if dt_end:
            filters.append(f"f.campanha_end <= '{dt_end}'")

        where_extra = ""
        if filters:
            where_extra = "AND " + " AND ".join(filters)

        rows = query(f"""
            SELECT
                REPLACE(f.campanha_id, 'ENTITY_', '') AS entity_id,
                f.campanha_id,
                COALESCE(d.friendly_name, f.campanha_name) AS friendly_name,
                COALESCE(d.categoria, 'General')            AS categoria,
                f.campanha_start,
                f.campanha_end,
                (f.financeiro->>'total_users')::int          AS total_users,
                (f.financeiro->>'ngr_brl')::numeric          AS ngr_brl,
                (f.comparativo->>'ngr_incremental')::numeric AS ngr_incremental,
                (f.comparativo->>'roi')::numeric             AS roi,
                (f.comparativo->>'meta_atingimento_pct')::numeric AS meta_pct,
                (f.comparativo->>'custo_total')::numeric     AS custo_total,
                (f.comparativo->>'custo_sms_disparopro')::numeric AS custo_sms_disparopro,
                (f.comparativo->>'custo_sms_pushfy')::numeric     AS custo_sms_pushfy,
                (f.comparativo->>'custo_whatsapp')::numeric       AS custo_whatsapp,
                CASE
                    WHEN f.comparativo->>'roi' IS NOT NULL THEN 'OK'
                    WHEN (f.financeiro->>'total_users')::int = 0 THEN 'Vazio'
                    ELSE 'Reprocessavel'
                END AS status
            FROM multibet.fact_crm_daily_performance f
            LEFT JOIN multibet.dim_crm_friendly_names d
                ON REPLACE(f.campanha_id, 'ENTITY_', '') = d.entity_id
            WHERE f.period = 'DURING'
            {where_extra}
            ORDER BY COALESCE((f.comparativo->>'roi')::numeric, -9999) DESC
        """)

        return jsonify(safe_json(rows))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# =============================================================================
# GET /api/v1/campaign/<id> — Deep dive
# =============================================================================
@app.route("/api/v1/campaign/<campaign_id>")
def api_campaign_detail(campaign_id):
    """
    Retorna objeto completo da campanha com BEFORE/DURING/AFTER.
    Payload inclui funil, financeiro e comparativo como JSON nativos
    para consumo direto pelo Chart.js.

    Funil: Disparos → Cliques → Bônus Claimed (Status 3)
    Comparativo: NGR M-1 (Baseline) vs NGR During
    """
    try:
        rows = query(f"""
            SELECT
                f.campanha_id,
                COALESCE(d.friendly_name, f.campanha_name) AS friendly_name,
                COALESCE(d.categoria, 'General')            AS categoria,
                f.period,
                f.period_start,
                f.period_end,
                f.campanha_start,
                f.campanha_end,
                f.funil,
                f.financeiro,
                f.comparativo
            FROM multibet.fact_crm_daily_performance f
            LEFT JOIN multibet.dim_crm_friendly_names d
                ON REPLACE(f.campanha_id, 'ENTITY_', '') = d.entity_id
            WHERE f.campanha_id = %s
            ORDER BY CASE f.period
                WHEN 'BEFORE' THEN 1 WHEN 'DURING' THEN 2 WHEN 'AFTER' THEN 3
            END
        """, (campaign_id,))

        if not rows:
            return jsonify({"error": "Campaign not found"}), 404

        # Parsear JSONB em objetos nativos
        for row in rows:
            parse_jsonb(row, "funil", "financeiro", "comparativo")

        # Montar resposta estruturada para Chart.js
        periods = {}
        meta = {}
        for row in rows:
            p = row["period"]
            periods[p] = {
                "period_start": row["period_start"],
                "period_end": row["period_end"],
                "funil": row["funil"],
                "financeiro": row["financeiro"],
                "comparativo": row["comparativo"],
            }
            if p == "DURING":
                meta = {
                    "campanha_id": row["campanha_id"],
                    "friendly_name": row["friendly_name"],
                    "categoria": row["categoria"],
                    "campanha_start": row["campanha_start"],
                    "campanha_end": row["campanha_end"],
                }

        # Montar dados do funil para Chart.js
        # Disparos → Cliques → Bônus Claimed
        during_funil = periods.get("DURING", {}).get("funil", {})
        during_fin = periods.get("DURING", {}).get("financeiro", {})
        before_fin = periods.get("BEFORE", {}).get("financeiro", {})

        chart_funil = {
            "labels": ["Disparos", "Entregues", "Abertas", "Cliques", "Convertidas"],
            "values": [
                during_funil.get("comunicacoes_enviadas", 0),
                during_funil.get("comunicacoes_entregues", 0),
                during_funil.get("comunicacoes_abertas", 0),
                during_funil.get("comunicacoes_clicadas", 0),
                during_funil.get("comunicacoes_convertidas", 0),
            ],
        }

        chart_ngr_comparison = {
            "labels": ["NGR Baseline (M-1)", "NGR During"],
            "values": [
                before_fin.get("ngr_brl", 0),
                during_fin.get("ngr_brl", 0),
            ],
        }

        response = {
            "meta": meta,
            "periods": periods,
            "charts": {
                "funil_conversao": chart_funil,
                "ngr_comparison": chart_ngr_comparison,
            },
        }

        return jsonify(safe_json(response))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# =============================================================================
# GET /api/v1/filters — Dropdowns
# =============================================================================
@app.route("/api/v1/filters")
def api_filters():
    """Retorna listas de categoria e responsavel para dropdowns."""
    try:
        categorias = query("""
            SELECT categoria, COUNT(*) AS cnt
            FROM multibet.dim_crm_friendly_names
            WHERE categoria IS NOT NULL
            GROUP BY categoria
            ORDER BY cnt DESC
        """)

        responsaveis = query("""
            SELECT responsavel, COUNT(*) AS cnt
            FROM multibet.dim_crm_friendly_names
            WHERE responsavel IS NOT NULL
            GROUP BY responsavel
            ORDER BY cnt DESC
        """)

        return jsonify({
            "categorias": safe_json(categorias),
            "responsaveis": safe_json(responsaveis),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# =============================================================================
# HEALTH CHECK
# =============================================================================
@app.route("/api/v1/health")
def api_health():
    """Verifica conexão com Super Nova DB."""
    try:
        rows = query("SELECT 1 AS ok")
        return jsonify({"status": "healthy", "database": "connected"})
    except Exception as e:
        return jsonify({"status": "unhealthy", "error": str(e)}), 503


# =============================================================================
# MAIN
# =============================================================================
if __name__ == "__main__":
    log.info("=" * 60)
    log.info("  CRM Dashboard API — Fase 2: Camada de Visibilidade")
    log.info("  http://localhost:5000")
    log.info("  Endpoints:")
    log.info("    GET /api/v1/summary")
    log.info("    GET /api/v1/campaigns?categoria=Retencao")
    log.info("    GET /api/v1/campaign/ENTITY_754")
    log.info("    GET /api/v1/filters")
    log.info("    GET /api/v1/health")
    log.info("=" * 60)
    app.run(debug=True, host="127.0.0.1", port=5000)
