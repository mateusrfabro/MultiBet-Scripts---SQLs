"""
Conexão com Super Nova DB (PostgreSQL via SSH tunnel através do bastion).

Uso:
    from db.supernova import get_supernova_connection, execute_supernova
"""

import os
import psycopg2
import psycopg2.extras
from sshtunnel import SSHTunnelForwarder
from dotenv import load_dotenv

load_dotenv()

BASTION_HOST = "34.238.84.114"
BASTION_PORT = 22
BASTION_USER = "ec2-user"
BASTION_KEY  = os.getenv("SUPERNOVA_PEM_PATH", "bastion-analytics-key.pem")

PG_HOST = "supernova-db.c8r8mcwe6zq9.us-east-1.rds.amazonaws.com"
PG_PORT = 5432
PG_DB   = os.getenv("SUPERNOVA_DB",   "supernova_db")
PG_USER = os.getenv("SUPERNOVA_USER", "analytics_user")
PG_PASS = os.getenv("SUPERNOVA_PASS")


def get_supernova_connection():
    """
    Abre túnel SSH via bastion e retorna (tunnel, conn_postgres).
    Responsabilidade do chamador fechar:
        conn.close()
        tunnel.stop()
    """
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


def execute_supernova(sql: str, params=None, fetch: bool = False):
    """
    Executa SQL no Super Nova DB via SSH tunnel.

    Args:
        sql:    Comando SQL
        params: Parâmetros (opcional)
        fetch:  Se True, retorna as linhas

    Returns:
        Lista de tuplas se fetch=True, None caso contrário
    """
    tunnel, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            conn.commit()
            if fetch:
                return cur.fetchall()
    finally:
        conn.close()
        tunnel.stop()


if __name__ == "__main__":
    try:
        rows = execute_supernova("SELECT version()", fetch=True)
        print("Super Nova DB conectado com sucesso!")
        print(rows[0][0])
    except Exception as e:
        print(f"Erro: {e}")
