"""
Conexão com BigQuery (Smartico CRM).

Uso:
    from db.bigquery import query_bigquery
    df = query_bigquery("SELECT * FROM `projeto.dataset.tabela` LIMIT 10")
"""

import os
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

load_dotenv()


def get_client() -> bigquery.Client:
    """Cria e retorna um cliente autenticado do BigQuery."""
    credentials_path = os.getenv("BIGQUERY_CREDENTIALS_PATH")
    project_id = os.getenv("BIGQUERY_PROJECT_ID")

    if credentials_path and os.path.exists(credentials_path):
        # Autenticação via arquivo de chave de serviço (recomendado)
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=["https://www.googleapis.com/auth/bigquery"],
        )
        return bigquery.Client(credentials=credentials, project=project_id)
    else:
        # Fallback: Application Default Credentials (gcloud auth)
        return bigquery.Client(project=project_id)


def query_bigquery(sql: str) -> pd.DataFrame:
    """
    Executa uma query no BigQuery e retorna um DataFrame pandas.

    Args:
        sql: Consulta SQL padrão BigQuery (use backticks para nomes de tabelas)

    Returns:
        pd.DataFrame com os resultados
    """
    client = get_client()
    query_job = client.query(sql)
    return query_job.to_dataframe()


if __name__ == "__main__":
    # Teste de conexão
    try:
        df = query_bigquery("SELECT 1 AS ok")
        print("BigQuery conectado com sucesso!")
        print(df)
    except Exception as e:
        print(f"Erro ao conectar no BigQuery: {e}")
