"""
Monitor Multiverso — visualização local dos snapshots Anti-Abuse
=================================================================
Lê o JSON mais recente da pasta reports/ e exibe um painel clean no terminal.
Não consulta BigQuery, não envia Slack — só leitura local.

Uso:
    python monitor_multiverso.py              # lê snapshot mais recente
    python monitor_multiverso.py --watch      # atualiza a cada 60s
    python monitor_multiverso.py --watch 30   # atualiza a cada 30s
    python monitor_multiverso.py --all        # mostra BAIXO também
"""

import os
import sys
import json
import glob
import time
import argparse
from datetime import datetime

# ─── CONFIG ────────────────────────────────────────────────────────────────────

REPORTS_DIR = os.path.join(os.path.dirname(__file__), "reports")
TOP_N       = 20

RISK_COLOR = {
    "ALTO":  "\033[91m",   # vermelho
    "MEDIO": "\033[93m",   # amarelo
    "BAIXO": "\033[92m",   # verde
}
RESET = "\033[0m"
BOLD  = "\033[1m"
DIM   = "\033[2m"

# ─── HELPERS ────────────────────────────────────────────────────────────────────

def latest_snapshot() -> str | None:
    """Retorna o caminho do JSON mais recente em reports/."""
    files = glob.glob(os.path.join(REPORTS_DIR, "anti_abuse_*.json"))
    return max(files, key=os.path.getmtime) if files else None


def load_snapshot(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def clear():
    os.system("cls" if sys.platform == "win32" else "clear")


# ─── DISPLAY ────────────────────────────────────────────────────────────────────

def display(data: dict, show_all: bool, snapshot_path: str):
    jogadores = data.get("jogadores", [])
    gerado_em = data.get("gerado_em", "—")
    total     = data.get("total_jogadores", 0)
    alto      = data.get("alto_risco", 0)
    medio     = data.get("medio_risco", 0)
    baixo     = total - alto - medio

    agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

    print(f"\n{BOLD}{'═'*78}{RESET}")
    print(f"{BOLD}  MONITOR ANTI-ABUSE — CAMPANHA MULTIVERSO{RESET}")
    print(f"  Snapshot: {gerado_em}  |  Atualizado: {agora}")
    print(f"{'═'*78}{RESET}")

    # ── Resumo ──────────────────────────────────────────────────────────────────
    print(f"\n  {BOLD}RESUMO{RESET}")
    print(f"  {'Jogadores ativos:':<22} {total}")
    print(f"  {'Risco ALTO:':<22} {RISK_COLOR['ALTO']}{BOLD}{alto}{RESET}")
    print(f"  {'Risco MÉDIO:':<22} {RISK_COLOR['MEDIO']}{medio}{RESET}")
    print(f"  {'Risco BAIXO:':<22} {DIM}{baixo}{RESET}")

    # ── Filtro ──────────────────────────────────────────────────────────────────
    if show_all:
        lista = jogadores
    else:
        lista = [j for j in jogadores if j.get("risk_level") in ("ALTO", "MEDIO")]

    if not lista:
        print(f"\n  {RISK_COLOR['BAIXO']}Nenhum jogador suspeito no snapshot.{RESET}\n")
        print(f"{'═'*78}\n")
        return

    top = lista[:TOP_N]

    # ── Tabela ──────────────────────────────────────────────────────────────────
    print(f"\n  {BOLD}TOP {TOP_N} JOGADORES SUSPEITOS{RESET}")
    print(f"  {'─'*76}")
    print(f"  {'#':>3}  {'USER ID':>12}  {'NIVEL':<6}  {'SCORE':>5}  {'APOSTADO':>10}  {'P&L':>10}  FLAGS")
    print(f"  {'─'*76}")

    for i, j in enumerate(top, 1):
        nivel   = j.get("risk_level", "?")
        cor     = RISK_COLOR.get(nivel, "")
        uid     = int(j.get("user_id", 0))
        score   = int(j.get("risk_score", 0))
        wagered = float(j.get("total_wagered", 0))
        pnl     = float(j.get("pnl", 0))
        flags   = str(j.get("flags", "-"))[:38]
        pnl_str = f"R${pnl:>8,.0f}"
        pnl_col = "\033[91m" if pnl > 0 else "\033[92m"  # vermelho se ganhou, verde se perdeu

        print(
            f"  {i:>3}  {uid:>12}  {cor}{nivel:<6}{RESET}  {BOLD}{score:>5}{RESET}"
            f"  R${wagered:>8,.0f}  {pnl_col}{pnl_str}{RESET}  {DIM}{flags}{RESET}"
        )

    if len(lista) > TOP_N:
        print(f"\n  {DIM}... e mais {len(lista) - TOP_N} jogadores com risco (use --all para ver todos){RESET}")

    # ── Detalhe flags top 5 ALTO ─────────────────────────────────────────────
    altos = [j for j in lista if j.get("risk_level") == "ALTO"][:5]
    if altos:
        print(f"\n  {BOLD}DETALHE — TOP 5 ALTO RISCO{RESET}")
        print(f"  {'─'*76}")
        for j in altos:
            uid     = int(j.get("user_id", 0))
            score   = int(j.get("risk_score", 0))
            bets    = int(j.get("total_bets", 0))
            speed   = j.get("avg_seconds_between_bets", 0)
            games   = int(j.get("unique_fortune_games", 0))
            bonuses = int(j.get("total_campaign_bonuses", 0))
            max_rep = int(j.get("max_times_same_bonus", 0))
            flags   = str(j.get("flags", "-"))
            wagered = float(j.get("total_wagered", 0))
            print(f"\n  {RISK_COLOR['ALTO']}{BOLD}  user_id: {uid}  |  score: {score}{RESET}")
            print(f"     Flags:          {BOLD}{flags}{RESET}")
            print(f"     Apostas:        {bets} bets  |  Speed: {float(speed):.2f}s entre bets")
            print(f"     Fortune games:  {games}/6  |  Total apostado: R${wagered:,.2f}")
            print(f"     Bônus campanha: {bonuses} total  |  Max mesma quest: {max_rep}x")

    print(f"\n  {'─'*76}")
    snap_file = os.path.basename(snapshot_path)
    print(f"  {DIM}Fonte: reports/{snap_file}{RESET}")
    print(f"{'═'*78}\n")


# ─── ENTRY POINT ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Monitor Anti-Abuse Multiverso")
    parser.add_argument("--watch", nargs="?", const=60, type=int, metavar="SEGUNDOS",
                        help="Atualiza automaticamente a cada N segundos (padrão: 60)")
    parser.add_argument("--all", action="store_true", help="Mostra BAIXO risco também")
    args = parser.parse_args()

    while True:
        path = latest_snapshot()

        if args.watch:
            clear()

        if not path:
            print(f"\n  Nenhum snapshot encontrado em: {REPORTS_DIR}")
            print("  Rode o bot primeiro: python pipelines/anti_abuse_multiverso.py --json\n")
        else:
            data = load_snapshot(path)
            display(data, show_all=args.all, snapshot_path=path)

        if not args.watch:
            break

        print(f"  Atualizando em {args.watch}s... (Ctrl+C para sair)\n")
        try:
            time.sleep(args.watch)
        except KeyboardInterrupt:
            print("\n  Monitor encerrado.\n")
            break


if __name__ == "__main__":
    main()
