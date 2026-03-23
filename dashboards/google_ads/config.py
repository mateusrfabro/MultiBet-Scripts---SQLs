"""
Configuracao do Dashboard Google Ads Affiliates.

Altere aqui os parametros sem mexer no codigo principal.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# =====================================================================
# AFFILIATE IDs — Google Ads (gestora de trafego)
# =====================================================================
AFFILIATE_IDS = ["297657", "445431", "468114"]

# =====================================================================
# PERIODOS — visoes temporais do dashboard
# =====================================================================
# Quantos dias de historico carregar para graficos de tendencia
TREND_DAYS = 7

# =====================================================================
# CACHE — evita consultas repetidas ao Athena (custo por scan)
# =====================================================================
CACHE_TTL_SECONDS = 60 * 60  # 1 hora

# =====================================================================
# FLASK
# =====================================================================
FLASK_HOST = os.getenv("DASHBOARD_HOST", "0.0.0.0")
FLASK_PORT = int(os.getenv("DASHBOARD_PORT", "5050"))
FLASK_DEBUG = os.getenv("DASHBOARD_DEBUG", "false").lower() == "true"
SECRET_KEY = os.getenv("DASHBOARD_SECRET_KEY", "trocar-em-producao-gerar-com-secrets")

# =====================================================================
# AUTENTICACAO — usuario/senha para acesso ao dashboard
# =====================================================================
DASHBOARD_USER = os.getenv("DASHBOARD_USER", "google")
DASHBOARD_PASS = os.getenv("DASHBOARD_PASS", "Multibet2026!")

# =====================================================================
# RATE LIMITING
# =====================================================================
RATE_LIMIT = "30 per minute"