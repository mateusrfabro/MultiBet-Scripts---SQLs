"""
Dashboard Google Ads Affiliates — Flask App.

Produto self-service para gestora de trafego Google Ads.
Roda localmente + Cloudflare Tunnel para acesso externo com HTTPS.

Uso:
    python dashboards/google_ads/app.py

Acesso:
    http://localhost:5050  (local)
    https://seu-tunnel.cfargotunnel.com  (via Cloudflare Tunnel)
"""
import os
import sys
import logging
from datetime import datetime

from flask import Flask, render_template, jsonify, request, redirect, url_for, session
from functools import wraps

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from dashboards.google_ads.config import (
    FLASK_HOST, FLASK_PORT, FLASK_DEBUG, SECRET_KEY,
    DASHBOARD_USER, DASHBOARD_PASS, RATE_LIMIT, AFFILIATE_IDS,
)
from dashboards.google_ads.queries import get_dashboard_data, clear_cache

# =========================================================================
# SETUP
# =========================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger(__name__)

app = Flask(
    __name__,
    template_folder="templates",
    static_folder="static",
)
app.secret_key = SECRET_KEY

# Rate limiting
try:
    from flask_limiter import Limiter
    from flask_limiter.util import get_remote_address
    limiter = Limiter(
        get_remote_address,
        app=app,
        default_limits=[RATE_LIMIT],
        storage_uri="memory://",
    )
    log.info(f"Rate limiting ativo: {RATE_LIMIT}")
except ImportError:
    log.warning("flask-limiter nao instalado — rate limiting desabilitado")
    limiter = None


# =========================================================================
# AUTENTICACAO — login simples com sessao
# =========================================================================
def login_required(f):
    """Decorator: redireciona para login se nao autenticado."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("authenticated"):
            return redirect(url_for("login", next=request.url))
        return f(*args, **kwargs)
    return decorated


@app.route("/login", methods=["GET", "POST"])
def login():
    """Tela de login simples."""
    error = None
    if request.method == "POST":
        user = request.form.get("username", "")
        pwd = request.form.get("password", "")
        if user == DASHBOARD_USER and pwd == DASHBOARD_PASS:
            session["authenticated"] = True
            session["login_time"] = datetime.now().isoformat()
            log.info(f"Login OK de {request.remote_addr}")
            next_url = request.args.get("next", url_for("dashboard"))
            return redirect(next_url)
        else:
            error = "Usuario ou senha incorretos"
            log.warning(f"Login FALHOU de {request.remote_addr}")
    return render_template("login.html", error=error)


@app.route("/logout")
def logout():
    """Encerra sessao."""
    session.clear()
    return redirect(url_for("login"))


# =========================================================================
# ROTAS — Dashboard
# =========================================================================
@app.route("/")
@login_required
def dashboard():
    """Pagina principal do dashboard."""
    return render_template(
        "dashboard.html",
        affiliate_ids=AFFILIATE_IDS,
        now=datetime.now().strftime("%d/%m/%Y %H:%M"),
    )


@app.route("/api/data")
@login_required
def api_data():
    """
    Endpoint principal — retorna todos os dados do dashboard em JSON.

    O frontend chama este endpoint via fetch() e renderiza os dados.
    """
    try:
        data = get_dashboard_data()
        data["updated_at"] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        return jsonify(data)
    except Exception as e:
        log.error(f"Erro ao carregar dados: {e}", exc_info=True)
        return jsonify({
            "error": "Erro ao consultar dados. Tente novamente em alguns minutos."
        }), 500


@app.route("/api/refresh", methods=["POST"])
@login_required
def api_refresh():
    """Limpa o cache e forca recarregamento dos dados."""
    clear_cache()
    log.info(f"Cache limpo manualmente por {request.remote_addr}")
    return jsonify({"status": "ok", "message": "Cache limpo. Recarregue a pagina."})


# =========================================================================
# HEALTH CHECK (sem auth — para monitoramento)
# =========================================================================
@app.route("/health")
def health():
    """Health check simples."""
    return jsonify({"status": "ok", "timestamp": datetime.now().isoformat()})


# =========================================================================
# LOG de acesso
# =========================================================================
@app.after_request
def log_request(response):
    """Loga cada request para auditoria."""
    if request.path != "/health":
        log.info(
            f"{request.remote_addr} {request.method} {request.path} "
            f"-> {response.status_code}"
        )
    return response


# =========================================================================
# MAIN
# =========================================================================
if __name__ == "__main__":
    log.info(f"Dashboard Google Ads iniciando em {FLASK_HOST}:{FLASK_PORT}")
    log.info(f"Affiliates: {AFFILIATE_IDS}")
    log.info(f"Debug: {FLASK_DEBUG}")
    app.run(
        host=FLASK_HOST,
        port=FLASK_PORT,
        debug=FLASK_DEBUG,
    )