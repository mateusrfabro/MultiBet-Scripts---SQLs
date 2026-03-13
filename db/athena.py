"""
Conexão com AWS Athena (Iceberg Data Lake) — somente leitura.

Usa as mesmas 15 databases do Redshift, mas via Athena sobre Iceberg.
Região: sa-east-1 | Conta: 803633136520

Uso:
    from db.athena import query_athena
    df = query_athena("SELECT * FROM fund.tabela LIMIT 10")

    # Para especificar database padrão:
    df = query_athena("SELECT * FROM tabela LIMIT 10", database="fund")

    # Para listar databases/tabelas:
    df = query_athena("SHOW DATABASES")
    df = query_athena("SHOW TABLES IN fund")
"""

import os
import time
import logging
import pandas as pd
from pyathena import connect
from dotenv import load_dotenv

log = logging.getLogger(__name__)

load_dotenv()


def get_connection(database: str = "default"):
    """Abre e retorna uma conexão com o Athena."""
    return connect(
        aws_access_key_id=os.getenv("ATHENA_AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("ATHENA_AWS_SECRET_ACCESS_KEY"),
        s3_staging_dir=os.getenv("ATHENA_S3_STAGING",
                                 "s3://aws-athena-query-results-803633136520-sa-east-1/"),
        region_name=os.getenv("ATHENA_REGION", "sa-east-1"),
        schema_name=database,
    )


def query_athena(
    sql: str,
    database: str = "default",
    retries: int = 3,
    retry_delay: float = 5.0,
) -> pd.DataFrame:
    """
    Executa uma query no Athena e retorna um DataFrame pandas.

    Args:
        sql:         Consulta SQL (apenas SELECT / SHOW)
        database:    Database padrão para a query (default: "default")
        retries:     Número máximo de tentativas (padrão: 3)
        retry_delay: Segundos entre tentativas (padrão: 5)

    Returns:
        pd.DataFrame com os resultados
    """
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            conn = get_connection(database)
            df = pd.read_sql(sql, conn)
            return df
        except Exception as e:
            last_err = e
            if attempt < retries:
                log.warning(
                    f"Athena query falhou (tentativa {attempt}/{retries}): {e}. "
                    f"Retentando em {retry_delay}s..."
                )
                time.sleep(retry_delay)
            else:
                log.error(f"Athena query falhou após {retries} tentativas.")
    raise last_err


if __name__ == "__main__":
    # Teste de conexão
    try:
        df = query_athena("SHOW DATABASES")
        print("Athena conectado com sucesso!")
        print(f"\nDatabases disponíveis ({len(df)}):")
        print(df.to_string(index=False))
    except Exception as e:
        print(f"Erro ao conectar no Athena: {e}")