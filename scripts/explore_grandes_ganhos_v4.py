"""
Quick check: encoding of the missing game name + search in mapping.
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


def create_ssh_tunnel(local_port=15435):
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


def main():
    client, local_server, local_port = create_ssh_tunnel(15435)
    time.sleep(1)
    conn = psycopg2.connect(
        host="127.0.0.1", port=local_port, dbname=PG_DB,
        user=PG_USER, password=PG_PASS, connect_timeout=15,
    )
    conn.set_session(readonly=True, autocommit=True)
    cur = conn.cursor()

    # Check the raw bytes of the missing game name
    print("=== Missing game name details ===")
    cur.execute("""
        SELECT id, game_name, provider_name, game_slug,
               LENGTH(game_name) AS name_len,
               encode(game_name::bytea, 'hex') AS hex_name
        FROM multibet.grandes_ganhos
        WHERE game_image_url IS NULL OR game_image_url = ''
    """)
    for row in cur.fetchall():
        print(f"  id={row[0]}")
        print(f"  game_name={row[1]!r}")
        print(f"  provider={row[2]}")
        print(f"  slug={row[3]}")
        print(f"  len={row[4]}")
        print(f"  hex={row[5]}")

    # Search for similar names in mapping
    print("\n=== Similar games in mapping (ZEUS%) ===")
    cur.execute("""
        SELECT id, game_name, game_name_upper, provider_game_id, vendor_id,
               LEFT(game_image_url, 60) AS url_preview
        FROM multibet.game_image_mapping
        WHERE game_name_upper LIKE 'ZEUS%'
        ORDER BY game_name
    """)
    for row in cur.fetchall():
        print(f"  id={row[0]} | name={row[1]} | upper={row[2]} | game_id={row[3]} | vendor={row[4]}")
        print(f"    url={row[5]}")

    # Also search for HADES
    print("\n=== Similar games in mapping (%HADES%) ===")
    cur.execute("""
        SELECT id, game_name, game_name_upper, provider_game_id, vendor_id,
               LEFT(game_image_url, 60) AS url_preview
        FROM multibet.game_image_mapping
        WHERE game_name_upper LIKE '%HADES%'
        ORDER BY game_name
    """)
    for row in cur.fetchall():
        print(f"  id={row[0]} | name={row[1]} | upper={row[2]} | game_id={row[3]} | vendor={row[4]}")
        print(f"    url={row[5]}")

    # Check all grandes_ganhos rows for this game
    print("\n=== All grandes_ganhos rows for this game ===")
    cur.execute("""
        SELECT id, game_name, win_amount, event_time AT TIME ZONE 'America/Sao_Paulo' AS brt,
               game_image_url IS NOT NULL AND game_image_url <> '' AS has_img
        FROM multibet.grandes_ganhos
        WHERE game_name LIKE '%HADES%' OR game_name LIKE '%hades%'
        ORDER BY id
    """)
    for row in cur.fetchall():
        print(f"  id={row[0]} | name={row[1]} | R${row[2]} | {row[3]} | has_img={row[4]}")

    cur.close()
    conn.close()
    client.close()
    local_server.close()
    print("\nDone.")


if __name__ == "__main__":
    main()