"""
Conexão com Redshift (Pragmatic Solutions) — somente leitura.

Uso:
    from db.redshift import query_redshift
    df = query_redshift("SELECT * FROM tabela LIMIT 10")
"""

import os
import time
import logging
import pandas as pd
import redshift_connector
from dotenv import load_dotenv

log = logging.getLogger(__name__)

load_dotenv()


def get_connection():
    """Abre e retorna uma conexão com o Redshift."""
    return redshift_connector.connect(
        host=os.getenv("REDSHIFT_HOST"),
        port=int(os.getenv("REDSHIFT_PORT", 5439)),
        database=os.getenv("REDSHIFT_DB"),
        user=os.getenv("REDSHIFT_USER"),
        password=os.getenv("REDSHIFT_PASSWORD"),
    )


def query_redshift(sql: str, retries: int = 3, retry_delay: float = 5.0) -> pd.DataFrame:
    """
    Executa uma query no Redshift e retorna um DataFrame pandas.
    Reabre a conexão automaticamente em caso de erros transientes (57014 NFTSetupSessionStateOnRestore).

    Args:
        sql:         Consulta SQL (apenas SELECT)
        retries:     Número máximo de tentativas (padrão: 3)
        retry_delay: Segundos entre tentativas (padrão: 5)

    Returns:
        pd.DataFrame com os resultados
    """
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            with get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql)
                    cols = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
            return pd.DataFrame(rows, columns=cols)
        except Exception as e:
            last_err = e
            if attempt < retries:
                log.warning(f"Redshift query falhou (tentativa {attempt}/{retries}): {e}. Retentando em {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                log.error(f"Redshift query falhou após {retries} tentativas.")
    raise last_err


if __name__ == "__main__":
    # Teste de conexão
    try:
        df = query_redshift("SELECT 1 AS ok")
        print("Redshift conectado com sucesso!")
        print(df)
    except Exception as e:
        print(f"Erro ao conectar no Redshift: {e}")
