"""
Script para gerar o Refresh Token do Google Ads API.

Executa uma vez so — abre o navegador, voce loga com sua conta Google,
e o script gera o refresh_token que vai no .env.

Uso:
    python scripts/google_ads_generate_token.py

Pre-requisitos:
    pip install google-auth-oauthlib
"""

import os
import sys
from dotenv import load_dotenv

load_dotenv()

# Le do .env ou pede input manual
CLIENT_ID = os.getenv("GOOGLE_ADS_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("GOOGLE_ADS_CLIENT_SECRET", "")

# Escopo necessario para Google Ads API
SCOPES = ["https://www.googleapis.com/auth/adwords"]


def main():
    try:
        from google_auth_oauthlib.flow import InstalledAppFlow
    except ImportError:
        print("Erro: biblioteca nao instalada. Rode:")
        print("  pip install google-auth-oauthlib")
        sys.exit(1)

    # Verificar credenciais
    client_id = CLIENT_ID
    client_secret = CLIENT_SECRET

    if not client_id:
        client_id = input("Cole o Client ID: ").strip()
    if not client_secret:
        client_secret = input("Cole o Client Secret: ").strip()

    if not client_id or not client_secret:
        print("Erro: Client ID e Client Secret sao obrigatorios.")
        sys.exit(1)

    # Montar config OAuth2 (mesmo formato do JSON baixado do Google Cloud)
    client_config = {
        "installed": {
            "client_id": client_id,
            "client_secret": client_secret,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": ["http://localhost"],
        }
    }

    print("\n=== Gerando Refresh Token para Google Ads API ===\n")
    print("1. O navegador vai abrir pedindo login na sua conta Google")
    print("2. Use a mesma conta que tem acesso ao Google Ads (mateus.fabro@grupo-pgs.com)")
    print("3. Autorize o acesso")
    print("4. O token sera gerado automaticamente\n")

    # Iniciar fluxo OAuth2
    flow = InstalledAppFlow.from_client_config(client_config, scopes=SCOPES)

    # Roda servidor local para capturar o callback
    credentials = flow.run_local_server(
        port=8080,
        prompt="consent",
        access_type="offline",
    )

    refresh_token = credentials.refresh_token

    if not refresh_token:
        print("\nERRO: refresh_token nao foi gerado.")
        print("Tente novamente — certifique-se de clicar 'Permitir' no navegador.")
        sys.exit(1)

    print("\n" + "=" * 60)
    print("REFRESH TOKEN GERADO COM SUCESSO!")
    print("=" * 60)
    print(f"\n{refresh_token}\n")
    print("=" * 60)
    print("\nAdicione no seu .env:")
    print(f'GOOGLE_ADS_REFRESH_TOKEN={refresh_token}')
    print("\nAs outras variaveis que tambem devem estar no .env:")
    print(f'GOOGLE_ADS_CLIENT_ID={client_id}')
    print(f'GOOGLE_ADS_CLIENT_SECRET={client_secret}')
    print('GOOGLE_ADS_DEVELOPER_TOKEN=<pegar na MCC - Central de API>')
    print('GOOGLE_ADS_CUSTOMER_ID=4985069191')
    print('GOOGLE_ADS_LOGIN_CUSTOMER_ID=1004058739')
    print("=" * 60)


if __name__ == "__main__":
    main()