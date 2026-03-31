"""
Exploracao das tabelas multibet.grandes_ganhos e multibet.game_image_mapping
no Super Nova DB (PostgreSQL) via SSH tunnel.

READ-ONLY: nenhuma alteracao de dados.
"""

import paramiko
import psycopg2
import psycopg2.extras
import socket
import select
import threading
import time

# --- Configuracao ---
BASTION_HOST = "34.238.84.114"
BASTION_PORT = 22
BASTION_USER = "ec2-user"
BASTION_KEY  = "C:/Users/NITRO/Downloads/bastion-analytics-key.pem"

PG_HOST = "supernova-db.c8r8mcwe6zq9.us-east-1.rds.amazonaws.com"
PG_PORT = 5432
PG_DB   = "supernova_db"
PG_USER = "analytics_user"
PG_PASS = "Supernova123!"


def create_ssh_tunnel(local_port=15432):
    """Cria tunnel SSH via paramiko (compativel com paramiko 3.5)."""
    key = paramiko.RSAKey.from_private_key_file(BASTION_KEY)
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(BASTION_HOST, port=BASTION_PORT, username=BASTION_USER, pkey=key, timeout=15)

    transport = client.get_transport()
    # Bind local port to remote PG
    local_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    local_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    local_server.bind(("127.0.0.1", local_port))
    local_server.listen(5)
    local_server.settimeout(30)

    def forward_tunnel():
        while True:
            try:
                local_conn, addr = local_server.accept()
            except (socket.timeout, OSError):
                break
            try:
                channel = transport.open_channel(
                    "direct-tcpip",
                    (PG_HOST, PG_PORT),
                    addr,
                )
            except Exception as e:
                print(f"Tunnel channel error: {e}")
                local_conn.close()
                continue

            # Relay data between local_conn and channel
            def relay():
                while True:
                    r, _, _ = select.select([local_conn, channel], [], [], 5)
                    if local_conn in r:
                        data = local_conn.recv(4096)
                        if not data:
                            break
                        channel.sendall(data)
                    if channel in r:
                        data = channel.recv(4096)
                        if not data:
                            break
                        local_conn.sendall(data)
                channel.close()
                local_conn.close()

            threading.Thread(target=relay, daemon=True).start()

    t = threading.Thread(target=forward_tunnel, daemon=True)
    t.start()

    return client, local_server, local_port


def run_query(cur, title, sql):
    """Executa query e imprime resultado formatado."""
    print(f"\n{'='*80}")
    print(f"  {title}")
    print(f"{'='*80}")
    cur.execute(sql)
    if cur.description:
        col_names = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        # Print header
        print(f"  Colunas: {col_names}")
        print(f"  Rows: {len(rows)}")
        print("-" * 80)
        for row in rows:
            print(f"  {row}")
    else:
        print("  (sem resultados)")
    print()


def main():
    print("Conectando ao bastion SSH...")
    client, local_server, local_port = create_ssh_tunnel(15432)
    time.sleep(1)  # aguardar tunnel subir

    print(f"Tunnel aberto em 127.0.0.1:{local_port}")
    print("Conectando ao PostgreSQL...")

    conn = psycopg2.connect(
        host="127.0.0.1",
        port=local_port,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS,
        connect_timeout=15,
    )
    conn.set_session(readonly=True, autocommit=True)
    cur = conn.cursor()
    print("Conectado ao Super Nova DB!\n")

    # ---------------------------------------------------------------
    # 1. Estrutura de multibet.grandes_ganhos
    # ---------------------------------------------------------------
    run_query(cur, "1. ESTRUTURA: multibet.grandes_ganhos (colunas e tipos)", """
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = 'multibet' AND table_name = 'grandes_ganhos'
        ORDER BY ordinal_position
    """)

    # ---------------------------------------------------------------
    # 2. Amostra de linhas
    # ---------------------------------------------------------------
    run_query(cur, "2. AMOSTRA: multibet.grandes_ganhos (10 linhas)", """
        SELECT * FROM multibet.grandes_ganhos
        ORDER BY id DESC
        LIMIT 10
    """)

    # ---------------------------------------------------------------
    # 3. Total de linhas e contagem de NULLs/vazios em game_image_url
    # ---------------------------------------------------------------
    run_query(cur, "3. CONTAGEM: total rows vs game_image_url NULL/vazio", """
        SELECT
            COUNT(*) AS total_rows,
            COUNT(*) FILTER (WHERE game_image_url IS NULL OR game_image_url = '') AS missing_image_url,
            COUNT(*) FILTER (WHERE game_image_url IS NOT NULL AND game_image_url <> '') AS has_image_url
        FROM multibet.grandes_ganhos
    """)

    # ---------------------------------------------------------------
    # 4. Estrutura de multibet.game_image_mapping
    # ---------------------------------------------------------------
    run_query(cur, "4. ESTRUTURA: multibet.game_image_mapping (colunas e tipos)", """
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = 'multibet' AND table_name = 'game_image_mapping'
        ORDER BY ordinal_position
    """)

    # ---------------------------------------------------------------
    # 5. Todos os registros de game_image_mapping
    # ---------------------------------------------------------------
    run_query(cur, "5. TODOS OS REGISTROS: multibet.game_image_mapping", """
        SELECT * FROM multibet.game_image_mapping
        ORDER BY 1
    """)

    # ---------------------------------------------------------------
    # 6. Jogos distintos em grandes_ganhos SEM game_image_url
    # ---------------------------------------------------------------
    run_query(cur, "6. JOGOS DISTINTOS sem game_image_url em grandes_ganhos", """
        SELECT DISTINCT game_name, COUNT(*) AS occurrences
        FROM multibet.grandes_ganhos
        WHERE game_image_url IS NULL OR game_image_url = ''
        GROUP BY game_name
        ORDER BY occurrences DESC
    """)

    # ---------------------------------------------------------------
    # Bonus: verificar se existe mapeamento nao utilizado
    # ---------------------------------------------------------------
    run_query(cur, "BONUS: game_image_mapping entries vs grandes_ganhos game_names", """
        SELECT
            m.game_name AS mapping_game_name,
            m.image_url AS mapping_url,
            COUNT(g.id) AS matches_in_grandes_ganhos,
            COUNT(g.id) FILTER (WHERE g.game_image_url IS NULL OR g.game_image_url = '') AS still_missing
        FROM multibet.game_image_mapping m
        LEFT JOIN multibet.grandes_ganhos g ON LOWER(g.game_name) = LOWER(m.game_name)
        GROUP BY m.game_name, m.image_url
        ORDER BY matches_in_grandes_ganhos DESC
    """)

    cur.close()
    conn.close()
    client.close()
    local_server.close()
    print("\nConexoes encerradas.")


if __name__ == "__main__":
    main()