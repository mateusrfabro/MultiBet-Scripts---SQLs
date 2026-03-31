"""
Configuracao do Dashboard de Trafego Pago — Multi-Canal.

Altere aqui os parametros sem mexer no codigo principal.
Suporta multiplos canais (Google Ads, Meta, etc.) via CHANNELS dict.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# =====================================================================
# CANAIS DE TRAFEGO — cada canal tem seus affiliate IDs
# =====================================================================
CHANNELS = {
    "google": {
        "label": "Google Ads",
        "affiliate_ids": ["297657", "445431", "468114"],
        "color": "#4285F4",
    },
    "meta": {
        "label": "Meta Ads",
        "affiliate_ids": ["532570", "532571", "464673"],
        "color": "#1877F2",
    },
}

# Todos os affiliates (modo consolidado)
ALL_AFFILIATE_IDS = []
for _ch in CHANNELS.values():
    ALL_AFFILIATE_IDS.extend(_ch["affiliate_ids"])

# Retrocompatibilidade — usado em imports legados
AFFILIATE_IDS = ALL_AFFILIATE_IDS

# Canal padrao quando nao especificado
DEFAULT_CHANNEL = "google"

# =====================================================================
# PERIODOS — visoes temporais do dashboard
# =====================================================================
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
SECRET_KEY = os.getenv("DASHBOARD_SECRET_KEY", os.urandom(32).hex())

# =====================================================================
# AUTENTICACAO — usuario/senha para acesso ao dashboard
# =====================================================================
DASHBOARD_USER = os.getenv("DASHBOARD_USER", "multibet")
DASHBOARD_PASS = os.getenv("DASHBOARD_PASS", "Multibet2026!")

# =====================================================================
# RATE LIMITING
# =====================================================================
RATE_LIMIT = "30 per minute"