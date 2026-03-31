"""
Exploracao compacta das tabelas multibet.grandes_ganhos e multibet.game_image_mapping
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


def create_ssh_tunnel(local_port=15433):
    key = paramiko.RSAKey.from_private_key_file(BASTION_KEY)
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(BASTION_HOST, port=BASTION_PORT, username=BASTION_USER, pkey=key, timeout=15)
    transport = client.get_transport()

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
                channel = transport.open_channel("direct-tcpip", (PG_HOST, PG_PORT), addr)
            except Exception:
                local_conn.close()
                continue
            def relay(lc=local_conn, ch=channel):
                while True:
                    r, _, _ = select.select([lc, ch], [], [], 5)
                    if lc in r:
                        data = lc.recv(4096)
                        if not data: break
                        ch.sendall(data)
                    if ch in r:
                        data = ch.recv(4096)
                        if not data: break
                        lc.sendall(data)
                ch.close()
                lc.close()
            threading.Thread(target=relay, daemon=True).start()

    threading.Thread(target=forward_tunnel, daemon=True).start()
    return client, local_server, local_port


def run_query(cur, title, sql):
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")
    cur.execute(sql)
    if cur.description:
        col_names = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        # Calc column widths
        widths = [len(c) for c in col_names]
        str_rows = []
        for row in rows:
            sr = [str(v)[:60] for v in row]
            str_rows.append(sr)
            for i, v in enumerate(sr):
                if i < len(widths):
                    widths[i] = max(widths[i], len(v))
        # Print
        header = " | ".join(c.ljust(widths[i]) for i, c in enumerate(col_names))
        print(f"  {header}")
        print(f"  {'-' * len(header)}")
        for sr in str_rows:
            line = " | ".join(sr[i].ljust(widths[i]) if i < len(widths) else sr[i] for i in range(len(sr)))
            print(f"  {line}")
        print(f"  ({len(rows)} rows)")
    print()
    return rows if cur.description else []


def main():
    print("Conectando ao bastion SSH...")
    client, local_server, local_port = create_ssh_tunnel(15433)
    time.sleep(1)

    conn = psycopg2.connect(
        host="127.0.0.1", port=local_port, dbname=PG_DB,
        user=PG_USER, password=PG_PASS, connect_timeout=15,
    )
    conn.set_session(readonly=True, autocommit=True)
    cur = conn.cursor()
    print("Conectado!\n")

    # 1. Estrutura grandes_ganhos
    run_query(cur, "1. ESTRUTURA: multibet.grandes_ganhos", """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'multibet' AND table_name = 'grandes_ganhos'
        ORDER BY ordinal_position
    """)

    # 2. Amostra 5 linhas (colunas selecionadas)
    run_query(cur, "2. AMOSTRA: grandes_ganhos (5 linhas, colunas chave)", """
        SELECT id, game_name, provider_name,
               CASE WHEN game_image_url IS NOT NULL AND game_image_url <> ''
                    THEN LEFT(game_image_url, 40) || '...'
                    ELSE '(NULL/vazio)'
               END AS image_url_preview,
               win_amount,
               event_time AT TIME ZONE 'America/Sao_Paulo' AS event_time_brt,
               game_slug
        FROM multibet.grandes_ganhos
        ORDER BY id DESC LIMIT 5
    """)

    # 3. Contagens
    run_query(cur, "3. CONTAGEM: total vs missing game_image_url", """
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE game_image_url IS NULL OR game_image_url = '') AS missing,
            COUNT(*) FILTER (WHERE game_image_url IS NOT NULL AND game_image_url <> '') AS has_url,
            ROUND(100.0 * COUNT(*) FILTER (WHERE game_image_url IS NULL OR game_image_url = '') / COUNT(*), 1) AS pct_missing
        FROM multibet.grandes_ganhos
    """)

    # 4. Estrutura game_image_mapping
    run_query(cur, "4. ESTRUTURA: multibet.game_image_mapping", """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'multibet' AND table_name = 'game_image_mapping'
        ORDER BY ordinal_position
    """)

    # 5. Todos registros game_image_mapping (contagem primeiro)
    run_query(cur, "5a. CONTAGEM game_image_mapping", """
        SELECT COUNT(*) AS total FROM multibet.game_image_mapping
    """)

    run_query(cur, "5b. REGISTROS game_image_mapping (game_name, vendor, image_url preview)", """
        SELECT id, game_name, game_name_upper, game_code, vendor_id,
               LEFT(image_url, 50) || '...' AS image_url_preview,
               LEFT(game_slug, 40) AS slug,
               source
        FROM multibet.game_image_mapping
        ORDER BY id
    """)

    # 6. Jogos distintos sem image_url
    run_query(cur, "6. JOGOS DISTINTOS sem game_image_url em grandes_ganhos", """
        SELECT game_name, provider_name, COUNT(*) AS occurrences
        FROM multibet.grandes_ganhos
        WHERE game_image_url IS NULL OR game_image_url = ''
        GROUP BY game_name, provider_name
        ORDER BY occurrences DESC
    """)

    # 7. Bonus: cruzamento
    run_query(cur, "7. BONUS: mapping vs grandes_ganhos (match check)", """
        WITH missing AS (
            SELECT DISTINCT game_name
            FROM multibet.grandes_ganhos
            WHERE game_image_url IS NULL OR game_image_url = ''
        )
        SELECT m.game_name AS missing_game,
               gim.game_name AS has_mapping,
               CASE WHEN gim.game_name IS NOT NULL THEN 'MATCH' ELSE 'NO MATCH' END AS status
        FROM missing m
        LEFT JOIN multibet.game_image_mapping gim ON UPPER(m.game_name) = gim.game_name_upper
        ORDER BY status, m.game_name
    """)

    cur.close()
    conn.close()
    client.close()
    local_server.close()
    print("Conexoes encerradas.")


if __name__ == "__main__":
    main()