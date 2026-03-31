"""
Exploracao compacta (parte 2) - corrigida.
Queries 5b, 6, 7 que falharam no v2.
"""

import paramiko
import psycopg2
import socket
import select
import threading
import time

BASTION_HOST = "34.238.84.114"
BASTION_PORT = 22
BASTION_USER = "ec2-user"
BASTION_KEY  = "C:/Users/NITRO/Downloads/bastion-analytics-key.pem"
PG_HOST = "supernova-db.c8r8mcwe6zq9.us-east-1.rds.amazonaws.com"
PG_PORT = 5432
PG_DB   = "supernova_db"
PG_USER = "analytics_user"
PG_PASS = "Supernova123!"


def create_ssh_tunnel(local_port=15434):
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
                ch.close(); lc.close()
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
        widths = [len(c) for c in col_names]
        str_rows = []
        for row in rows:
            sr = [str(v)[:70] for v in row]
            str_rows.append(sr)
            for i, v in enumerate(sr):
                if i < len(widths):
                    widths[i] = max(widths[i], len(v))
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
    print("Conectando...")
    client, local_server, local_port = create_ssh_tunnel(15434)
    time.sleep(1)
    conn = psycopg2.connect(
        host="127.0.0.1", port=local_port, dbname=PG_DB,
        user=PG_USER, password=PG_PASS, connect_timeout=15,
    )
    conn.set_session(readonly=True, autocommit=True)
    cur = conn.cursor()
    print("Conectado!\n")

    # 5b. Amostra game_image_mapping (primeiros 20 + ultimos 10)
    run_query(cur, "5b. game_image_mapping - AMOSTRA (20 primeiros)", """
        SELECT id, game_name, game_name_upper, provider_game_id, vendor_id,
               LEFT(game_image_url, 55) AS image_url_preview,
               LEFT(game_slug, 45) AS slug,
               source
        FROM multibet.game_image_mapping
        ORDER BY id
        LIMIT 20
    """)

    # 5c. Distribuicao por source e vendor
    run_query(cur, "5c. game_image_mapping - DISTRIBUICAO por source", """
        SELECT source, COUNT(*) AS count
        FROM multibet.game_image_mapping
        GROUP BY source ORDER BY count DESC
    """)

    run_query(cur, "5d. game_image_mapping - DISTRIBUICAO por vendor_id (top 15)", """
        SELECT vendor_id, COUNT(*) AS count
        FROM multibet.game_image_mapping
        GROUP BY vendor_id ORDER BY count DESC LIMIT 15
    """)

    # 6. Jogos distintos sem image_url em grandes_ganhos
    run_query(cur, "6. JOGOS SEM game_image_url em grandes_ganhos", """
        SELECT game_name, provider_name, COUNT(*) AS occurrences
        FROM multibet.grandes_ganhos
        WHERE game_image_url IS NULL OR game_image_url = ''
        GROUP BY game_name, provider_name
        ORDER BY occurrences DESC
    """)

    # 7. Cruzamento: jogos sem imagem vs mapping disponivel
    run_query(cur, "7. CRUZAMENTO: jogos sem imagem vs game_image_mapping", """
        WITH missing AS (
            SELECT DISTINCT game_name, provider_name
            FROM multibet.grandes_ganhos
            WHERE game_image_url IS NULL OR game_image_url = ''
        )
        SELECT m.game_name AS missing_game,
               m.provider_name,
               gim.game_name AS mapping_game_name,
               LEFT(gim.game_image_url, 50) AS mapping_url,
               CASE WHEN gim.game_name IS NOT NULL THEN 'MATCH FOUND' ELSE 'NO MAPPING' END AS status
        FROM missing m
        LEFT JOIN multibet.game_image_mapping gim ON UPPER(m.game_name) = gim.game_name_upper
        ORDER BY status, m.game_name
    """)

    # 8. Bonus: distribuicao game_image_url por provider em grandes_ganhos
    run_query(cur, "8. BONUS: grandes_ganhos - distribuicao por provider_name", """
        SELECT provider_name,
               COUNT(*) AS total,
               COUNT(*) FILTER (WHERE game_image_url IS NOT NULL AND game_image_url <> '') AS has_img,
               COUNT(*) FILTER (WHERE game_image_url IS NULL OR game_image_url = '') AS missing_img
        FROM multibet.grandes_ganhos
        GROUP BY provider_name
        ORDER BY total DESC
    """)

    # 9. Verificar se ha game_image_url com problemas (double slash, etc)
    run_query(cur, "9. BONUS: game_image_url patterns em grandes_ganhos", """
        SELECT
            COUNT(*) FILTER (WHERE game_image_url LIKE '%//%//%') AS double_slash_count,
            COUNT(*) FILTER (WHERE game_image_url NOT LIKE 'https://%') AS non_https_count,
            COUNT(*) FILTER (WHERE game_image_url LIKE '%.webp') AS webp_count,
            COUNT(*) FILTER (WHERE game_image_url NOT LIKE '%.webp' AND game_image_url IS NOT NULL AND game_image_url <> '') AS non_webp_count
        FROM multibet.grandes_ganhos
    """)

    cur.close()
    conn.close()
    client.close()
    local_server.close()
    print("Conexoes encerradas.")


if __name__ == "__main__":
    main()