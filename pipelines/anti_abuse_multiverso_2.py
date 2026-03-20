"""
Anti-Abuse Bot — Campanha Multiverso
=====================================
Monitora comportamentos suspeitos nos 6 jogos Fortune (PG Soft)
durante a campanha de Challenges/Quests da MultiBet.

Roda a cada 5 minutos via BigQuery (Smartico).
Output: lista de jogadores com score de risco + flags + alerta no Slack.

Uso:
    python pipelines/anti_abuse_multiverso.py              # roda uma vez
    python pipelines/anti_abuse_multiverso.py --loop       # roda a cada 5 min
    python pipelines/anti_abuse_multiverso.py --hours 48   # janela de analise
    python pipelines/anti_abuse_multiverso.py --loop --json # loop + exporta JSON

Configuracao no .env:
    SLACK_WEBHOOK_MULTIVERSO=https://hooks.slack.com/services/SEU_WORKSPACE/SEU_CANAL/SEU_TOKEN
    Canal Slack: #risco-multiverso

Próximo passo sugerido (a alinhar com Raphael/CRM):
    - Usar API Smartico para taguear usuarios ALTO risco com "fraud_high_multiverso"
    - Automation Center reage à tag: pausar entrega de bonus / notificar CRM
    - Isso elimina a necessidade de revisão manual para casos óbvios
"""

import sys
import os
import io
import time
import json
import signal
import logging
import argparse
import requests
from datetime import datetime, timezone, timedelta

# Graceful shutdown — permite terminar o ciclo atual antes de parar
_shutdown_requested = False

def _handle_signal(signum, frame):
    global _shutdown_requested
    _shutdown_requested = True
    logging.getLogger("anti_abuse").info("Sinal de parada recebido — finalizando apos ciclo atual...")

signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)

# Fix encoding no Windows (cp1252 nao suporta caracteres especiais)
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import pandas as pd
from dotenv import load_dotenv

# Garante que o diretório raiz do projeto esteja no path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from db.bigquery import query_bigquery

load_dotenv()

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

# Smartico smr_game_id dos 6 jogos Fortune (PG Soft)
FORTUNE_GAMES = {
    45838245: "Fortune Tiger",
    45815847: "Fortune Dragon",
    45708862: "Fortune Rabbit",
    45967080: "Fortune Mouse",
    45846458: "Fortune Ox",
    45804835: "Fortune Snake",
}

GAME_IDS_SQL = ", ".join(str(gid) for gid in FORTUNE_GAMES.keys())

# Thresholds da campanha (valores em BRL)
QUEST_THRESHOLDS = [150, 300, 500]  # girar R$150, R$300, R$500

# Data de início da promo (para calcular dormência e conta nova)
# 13/03/2026 às 17h BRT = 20h UTC
PROMO_START = "2026-03-13 20:00:00"

# Journey IDs dos 18 bônus da campanha Multiverso (Smartico Automation Center)
# Fonte: screenshots do Automation Center, 06-07/03/2026
# Máximo legítimo: 1 entrega por quest por jogo = 18 bônus por jogador
# Se COUNT > 1 para o mesmo journey_id → bônus duplicado = fraude
CAMPAIGN_JOURNEY_IDS = {
    # Fortune Tiger
    1951086: "Tiger_Q1", 1951242: "Tiger_Q2", 1951326: "Tiger_Q3",
    # Fortune Rabbit
    1954378: "Rabbit_Q1", 1954382: "Rabbit_Q2", 1954386: "Rabbit_Q3",
    # Fortune Snake
    1954451: "Snake_Q1",  1954456: "Snake_Q2",  1954460: "Snake_Q3",
    # Fortune Mouse
    1954402: "Mouse_Q1",  1954406: "Mouse_Q2",  1954410: "Mouse_Q3",
    # Fortune Dragon
    1954438: "Dragon_Q1", 1954442: "Dragon_Q2", 1954446: "Dragon_Q3",
    # Fortune Ox
    1954390: "Ox_Q1",     1954394: "Ox_Q2",     1954398: "Ox_Q3",
}
CAMPAIGN_JOURNEY_IDS_SQL = ", ".join(str(jid) for jid in CAMPAIGN_JOURNEY_IDS.keys())
MAX_LEGIT_BONUSES = 18  # 6 jogos à 3 quests

# Campo de ligação em j_bonuses → Journey ID
# ATENÇÃO: confirmar com query de diagnóstico abaixo se o campo é "journey_id"
# Para verificar: SELECT * FROM `smartico-bq6.dwh_ext_24105.j_bonuses` LIMIT 1
BONUS_JOURNEY_FIELD = "source_product_ref_id"  # confirmado: campo Journey ID em j_bonuses

# Scoring — apenas sinais de FRAUDE:
#   1. Conta criada após o início da promo (não deveria participar)
#   2. Automação/bot completando wagering
#   3. Bônus duplicado (mesma quest entregue mais de uma vez)
#   4. Aposta mínima sistemática completando quest (exploração intencional)
SCORING = {
    "new_account":    {"points": 45},               # conta criada APÓS o início da promo
    "bot_speed":      {"threshold": 1.0, "points": 30},  # < 1s entre apostas = automação
    "bonus_repeated": {"points": 55},               # mesma quest entregue >1x — fraude direta
    "quest_min_bet":  {"max_avg_bet": 0.60, "points": 20},  # avg_bet <= R$0,60 completando quest — exploração intencional
}

# Calibração quest_min_bet:
#   CONTA_POS_PROMO + QUEST_MIN_BET: 45+20 = 65 -> ALTO
#   QUEST_MIN_BET sozinho:               20 = BAIXO (nao alerta — jogador legítimo pode apostar pouco)
#   BOT_SPEED + QUEST_MIN_BET:       30+20 = MEDIO (sinal duplo, mas sem conta nova)

# Calibração:
#   Bonus repetido (sozinho):             55    =  55 → ALTO (fraude direta)
#   Conta pós-promo + bot:            45+30    =  75 → ALTO
#   Bonus repetido + conta pós-promo: 55+45    = 100 → ALTO (caso grave)
#   Conta pós-promo (sozinha):            45    = MEDIO
#   Bot (sozinho):                        30    = BAIXO (sinal isolado, não alerta)
RISK_THRESHOLD_MEDIUM = 35
RISK_THRESHOLD_HIGH   = 55

# Intervalo do loop (segundos)
LOOP_INTERVAL = 300  # 5 minutos

# Janela de análise padrão (horas)
DEFAULT_HOURS = 24

# Slack Incoming Webhook (configura no .env)
SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK_MULTIVERSO", "")

# Só envia pro Slack se tiver pelo menos N jogadores ALTO risco
SLACK_MIN_HIGH_RISK = 1

# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("anti_abuse")

# ---------------------------------------------------------------------------
# QUERIES
# ---------------------------------------------------------------------------

def query_user_bets(hours: int) -> pd.DataFrame:
    """
    Agrega apostas por jogador nos 6 jogos Fortune dentro da janela de horas.
    Retorna métricas comportamentais + número de challenges (quests3) completados.
    """
    sql = f"""
    WITH bets AS (
        -- Apostas com saldo real nos 6 jogos Fortune dentro da janela
        SELECT
            user_id,
            event_time,
            casino_last_bet_amount_real,
            casino_last_bet_game_name
        FROM `smartico-bq6.dwh_ext_24105.tr_casino_bet`
        WHERE casino_last_bet_game_name IN ({GAME_IDS_SQL})
          AND event_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
          AND casino_last_bet_amount_real > 0
    ),

    -- Wagering por jogo por usuário (para detectar quests completadas)
    wagered_per_game AS (
        SELECT
            user_id,
            casino_last_bet_game_name,
            SUM(casino_last_bet_amount_real) AS wagered_this_game
        FROM bets
        GROUP BY user_id, casino_last_bet_game_name
    ),

    -- Quantos challenges cada user completou até a quest3 (>= R$500)
    challenges_completed AS (
        SELECT
            user_id,
            COUNTIF(wagered_this_game >= 500) AS games_quest3_completed,
            COUNTIF(wagered_this_game >= 300) AS games_quest2_completed,
            COUNTIF(wagered_this_game >= 150) AS games_quest1_completed
        FROM wagered_per_game
        GROUP BY user_id
    ),

    user_stats AS (
        SELECT
            b.user_id,

            -- Volume geral
            COUNT(*) AS total_bets,
            SUM(b.casino_last_bet_amount_real) AS total_wagered,
            AVG(b.casino_last_bet_amount_real) AS avg_bet,

            -- Velocidade (segundos médios entre apostas — sinal de bot)
            SAFE_DIVIDE(
                TIMESTAMP_DIFF(MAX(b.event_time), MIN(b.event_time), SECOND),
                NULLIF(COUNT(*) - 1, 0)
            ) AS avg_seconds_between_bets,

            -- Diversidade de jogos Fortune
            COUNT(DISTINCT b.casino_last_bet_game_name) AS unique_fortune_games,

            -- Janela de atividade
            MIN(b.event_time) AS first_bet,
            MAX(b.event_time) AS last_bet,
            TIMESTAMP_DIFF(MAX(b.event_time), MIN(b.event_time), MINUTE) AS active_minutes

        FROM bets b
        GROUP BY b.user_id
        HAVING COUNT(*) >= 10  -- mínimo de 10 apostas pra avaliar
    )

    SELECT
        s.*,
        COALESCE(c.games_quest3_completed, 0) AS games_quest3_completed,
        COALESCE(c.games_quest2_completed, 0) AS games_quest2_completed,
        COALESCE(c.games_quest1_completed, 0) AS games_quest1_completed
    FROM user_stats s
    LEFT JOIN challenges_completed c ON c.user_id = s.user_id
    ORDER BY s.total_wagered DESC
    """
    log.info(f"Consultando apostas nos Fortune games (ultimas {hours}h)...")
    return query_bigquery(sql)


def query_user_registration() -> pd.DataFrame:
    """
    Retorna a data de cadastro real de cada usuário via j_user.core_registration_date.
    Filtra apenas contas criadas a partir do início da promo — são as únicas que interessam
    para o flag CONTA_POS_PROMO (conta criada após a promo não deveria participar).
    Mais preciso que o proxy de primeiro login anterior.
    """
    sql = f"""
    SELECT
        user_id,
        core_registration_date AS registration_date
    FROM `smartico-bq6.dwh_ext_24105.j_user`
    WHERE core_registration_date >= TIMESTAMP('{PROMO_START}')
    """
    log.info("Consultando cadastros pos-promo (j_user.core_registration_date)...")
    return query_bigquery(sql)


def query_user_wins(hours: int) -> pd.DataFrame:
    """
    Agrega ganhos por jogador nos 6 jogos Fortune.
    Permite calcular P&L (profit/loss) durante a promo.
    """
    sql = f"""
    SELECT
        user_id,
        COUNT(*) AS total_wins,
        SUM(casino_last_win_amount_real) AS total_won,
        MAX(casino_last_win_amount_real) AS max_win,
        MAX(casino_last_win_multiplier_real) AS max_multiplier
    FROM `smartico-bq6.dwh_ext_24105.tr_casino_win`
    WHERE casino_last_bet_game_name IN ({GAME_IDS_SQL})
      AND event_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
      AND casino_last_win_amount_real > 0
    GROUP BY user_id
    """
    log.info("Consultando wins nos Fortune games...")
    return query_bigquery(sql)


def query_withdrawals(hours: int) -> pd.DataFrame:
    """
    Busca saques aprovados no período.
    Usado pra detectar saque imediato após completar quest.
    """
    sql = f"""
    SELECT
        user_id,
        COUNT(*) AS total_withdrawals,
        SUM(acc_last_withdrawal_amount) AS total_withdrawn,
        MAX(event_time) AS last_withdrawal_time
    FROM `smartico-bq6.dwh_ext_24105.tr_acc_withdrawal_approved`
    WHERE event_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
      AND acc_last_withdrawal_amount > 0
    GROUP BY user_id
    """
    log.info("Consultando saques aprovados...")
    return query_bigquery(sql)


def query_campaign_participants() -> dict:
    """
    Retorna métricas dos jogadores que optaram/entraram na campanha Multiverso.
    'Entrou na campanha' = recebeu ao menos 1 bônus dos 18 journey IDs desde PROMO_START.

    Usado nos alertas Slack no lugar de 'jogadores ativos nos Fortune games',
    que incluia qualquer jogador — mesmo sem ter optado pela campanha.

    Retorna:
        total_campaign_users   : participantes únicos da campanha
        total_campaign_wagered : total apostado nos Fortune games por esses users desde PROMO_START
        first_entry_time       : quando o 1o jogador entrou na campanha
    """
    sql = f"""
    -- Participantes: receberam ao menos 1 bônus dos 18 journey IDs da campanha
    WITH campaign_users AS (
        SELECT DISTINCT user_id
        FROM `smartico-bq6.dwh_ext_24105.j_bonuses`
        WHERE {BONUS_JOURNEY_FIELD} IN ({CAMPAIGN_JOURNEY_IDS_SQL})
          AND fact_date >= TIMESTAMP('{PROMO_START}')
    )
    SELECT
        -- Total de participantes da campanha
        (SELECT COUNT(*) FROM campaign_users) AS total_campaign_users,

        -- Total apostado nos Fortune games pelos participantes desde o início da promo
        (
            SELECT COALESCE(SUM(b.casino_last_bet_amount_real), 0)
            FROM `smartico-bq6.dwh_ext_24105.tr_casino_bet` b
            WHERE b.casino_last_bet_game_name IN ({GAME_IDS_SQL})
              AND b.event_time >= TIMESTAMP('{PROMO_START}')
              AND b.casino_last_bet_amount_real > 0
              AND b.user_id IN (SELECT user_id FROM campaign_users)
        ) AS total_campaign_wagered,

        -- Quando o primeiro jogador entrou na campanha (para contexto da janela)
        (
            SELECT MIN(fact_date)
            FROM `smartico-bq6.dwh_ext_24105.j_bonuses`
            WHERE {BONUS_JOURNEY_FIELD} IN ({CAMPAIGN_JOURNEY_IDS_SQL})
              AND fact_date >= TIMESTAMP('{PROMO_START}')
        ) AS first_entry_time
    """
    log.info("Consultando participantes da campanha Multiverso (j_bonuses)...")
    df = query_bigquery(sql)

    if df.empty or df.iloc[0].get("total_campaign_users", 0) is None:
        return {"total_campaign_users": 0, "total_campaign_wagered": 0.0, "first_entry_time": None}

    row = df.iloc[0]
    return {
        "total_campaign_users":   int(row.get("total_campaign_users", 0) or 0),
        "total_campaign_wagered": float(row.get("total_campaign_wagered", 0) or 0),
        "first_entry_time":       row.get("first_entry_time"),
    }


def query_bonus_claims(hours: int) -> pd.DataFrame:
    """
    Busca entregas de bônus da campanha Multiverso por usuário.

    Filtra pelos 18 Journey IDs da campanha (6 jogos à 3 quests).
    Detecta dois padrões de abuso:
      1. total_campaign_bonuses > 18  → recebeu mais do que o máximo legítimo
      2. max_times_same_bonus > 1     → mesma quest entregue mais de uma vez (fraude direta)

    ATENÇÃO: o campo de join com o Journey ID é definido em BONUS_JOURNEY_FIELD.
    Se a query falhar, rode a diagnóstica abaixo para verificar as colunas disponíveis:
        SELECT * FROM `smartico-bq6.dwh_ext_24105.j_bonuses` LIMIT 1
    """
    sql = f"""
    WITH campaign_bonuses AS (
        -- Filtra só os bônus dos 18 journeys da campanha Multiverso
        SELECT
            user_id,
            {BONUS_JOURNEY_FIELD} AS journey_id,
            COUNT(*) AS times_received
        FROM `smartico-bq6.dwh_ext_24105.j_bonuses`
        WHERE {BONUS_JOURNEY_FIELD} IN ({CAMPAIGN_JOURNEY_IDS_SQL})
          AND fact_date >= TIMESTAMP('{PROMO_START}')
        GROUP BY user_id, {BONUS_JOURNEY_FIELD}
    )
    SELECT
        user_id,
        SUM(times_received)         AS total_campaign_bonuses,
        MAX(times_received)         AS max_times_same_bonus,
        COUNTIF(times_received > 1) AS repeated_quest_count,
        MAX(times_received) > 1     AS has_repeated_bonus
    FROM campaign_bonuses
    GROUP BY user_id
    """
    log.info("Consultando bonus claims da campanha Multiverso...")
    return query_bigquery(sql)


# ---------------------------------------------------------------------------
# SCORING
# ---------------------------------------------------------------------------

def calculate_risk_score(row: pd.Series) -> tuple:
    """
    Calcula score de fraude para a Campanha Multiverso.
    Retorna (score, [flags]).

    Sinais monitorados:
        1. Conta criada após o início da promo (não deveria participar)
        2. Velocidade de aposta < 1s (automação/bot)
        3. Aposta mínima sistemática completando quest (exploração intencional)
        4. Bônus duplicado (mesma quest entregue mais de uma vez)
    """
    score = 0
    flags = []

    # 1. Conta criada APÓS o início da promo — não deveria ter acesso às quests
    reg_date = row.get("registration_date")
    if pd.notna(reg_date) and str(reg_date) != "0":
        score += SCORING["new_account"]["points"]
        flags.append("CONTA_POS_PROMO")

    # 2. Velocidade anormal (bot automatizando wagering)
    speed = row.get("avg_seconds_between_bets")
    if pd.notna(speed) and float(speed) > 0:
        speed = float(speed)
        if speed < SCORING["bot_speed"]["threshold"]:
            score += SCORING["bot_speed"]["points"]
            flags.append(f"BOT_SPEED_{speed:.1f}s")

    # 3. Aposta mínima sistemática completando quest — exploração intencional da campanha
    avg_bet = float(row.get("avg_bet", 0))
    quest1_completed = int(row.get("games_quest1_completed", 0))
    if avg_bet > 0 and avg_bet <= SCORING["quest_min_bet"]["max_avg_bet"] and quest1_completed >= 1:
        score += SCORING["quest_min_bet"]["points"]
        flags.append(f"QUEST_MIN_BET_R${avg_bet:.2f}")

    # 4. Bônus duplicado — mesma quest entregue mais de uma vez (fraude direta)
    max_same = int(row.get("max_times_same_bonus", 0))
    total_campaign = int(row.get("total_campaign_bonuses", 0))
    if max_same > 1:
        score += SCORING["bonus_repeated"]["points"]
        flags.append(f"BONUS_REPETIDO_x{max_same}")
    elif total_campaign > MAX_LEGIT_BONUSES:
        score += SCORING["bonus_repeated"]["points"]
        flags.append(f"BONUS_EXCESSO_{total_campaign}")

    return score, flags


def classify_risk(score: int) -> str:
    """Classifica o nível de risco baseado no score."""
    if score >= RISK_THRESHOLD_HIGH:
        return "ALTO"
    elif score >= RISK_THRESHOLD_MEDIUM:
        return "MEDIO"
    else:
        return "BAIXO"


# ---------------------------------------------------------------------------
# MAIN PIPELINE
# ---------------------------------------------------------------------------

# Cache: lista de user_ids com cadastro pós-promo (não muda durante a campanha)
_cache_profile = None  # Optional[pd.DataFrame]

# Controle de envio do "tudo limpo" — envia a cada 30 min (6 ciclos de 5 min)
CLEAN_NOTIFY_EVERY = 6   # ciclos
_clean_cycle_count = 0

# Acumulador de jogadores ALTO risco — persiste entre ciclos
# Guarda {user_id: {score, flags, ...}} para que cada alerta mostre TODOS os
# jogadores já detectados, não apenas os novos do ciclo atual.
_all_high_risk = {}  # dict[int, dict]


def run_analysis(hours: int) -> pd.DataFrame:
    """
    Executa a análise completa:
    1. Busca dados de apostas, wins, saques e bônus
    2. Cruza tudo por user_id
    3. Calcula score de risco
    4. Retorna DataFrame ordenado por score

    Queries estáticas (pré-promo) são cacheadas em memória para evitar
    chamadas desnecessárias ao BigQuery a cada ciclo de 5 minutos.
    """
    global _cache_profile

    df_bets = query_user_bets(hours)

    if df_bets.empty:
        log.info("Nenhuma aposta encontrada nos Fortune games no periodo.")
        return pd.DataFrame()

    df_wins        = query_user_wins(hours)
    df_withdrawals = query_withdrawals(hours)
    df_bonus       = query_bonus_claims(hours)

    # Query estática: cadastros pós-promo (lista não muda durante a campanha)
    if _cache_profile is None:
        log.info("Populando cache de cadastros pos-promo (j_user)...")
        _cache_profile = query_user_registration()

    df_profile = _cache_profile

    # Merge: bets <- wins
    df = df_bets.copy()
    if not df_wins.empty:
        df = df.merge(df_wins, on="user_id", how="left")
    else:
        df["total_wins"] = 0
        df["total_won"] = 0.0
        df["max_win"] = 0.0
        df["max_multiplier"] = 0.0

    # Merge: bets <- withdrawals
    if not df_withdrawals.empty:
        df = df.merge(df_withdrawals, on="user_id", how="left")
        df["has_withdrawal"] = df["total_withdrawals"].notna() & (df["total_withdrawals"] > 0)
    else:
        df["has_withdrawal"] = False
        df["total_withdrawals"] = 0
        df["total_withdrawn"] = 0.0

    # Merge: bets <- cadastros pós-promo
    # registration_date != NaN/0 → conta foi criada após PROMO_START
    if not df_profile.empty:
        df = df.merge(df_profile, on="user_id", how="left")
    else:
        df["registration_date"] = pd.NaT

    # Merge: bets <- bonus claims (campanha Multiverso)
    if not df_bonus.empty:
        df = df.merge(df_bonus, on="user_id", how="left")
    else:
        df["total_campaign_bonuses"] = 0
        df["max_times_same_bonus"]   = 0
        df["repeated_quest_count"]   = 0
        df["has_repeated_bonus"]     = False

    # Preenche NaN
    df = df.fillna(0)

    # Calcula P&L (profit/loss)
    df["pnl"] = df["total_won"].astype(float) - df["total_wagered"].astype(float)

    # Calcula score de risco e flags
    scores_flags = df.apply(calculate_risk_score, axis=1)
    df["risk_score"] = scores_flags.apply(lambda x: x[0])
    df["flags"] = scores_flags.apply(lambda x: ", ".join(x[1]) if x[1] else "-")
    df["risk_level"] = df["risk_score"].apply(classify_risk)

    # Ordena por score (mais suspeitos primeiro)
    df = df.sort_values("risk_score", ascending=False).reset_index(drop=True)

    return df


# ---------------------------------------------------------------------------
# SLACK
# ---------------------------------------------------------------------------

def send_slack_alert(df: pd.DataFrame, hours: int, df_alto_override=None, campaign_summary: dict = None):
    """
    Envia alerta no Slack quando há jogadores com risco ALTO.
    Usa Block Kit para mensagem rica com tabela de suspeitos.

    Se df_alto_override for passado, usa essa lista (acumulada) em vez
    de filtrar apenas os ALTO do ciclo atual.
    campaign_summary: dict com total_campaign_users e total_campaign_wagered (quem optou na campanha).
    """
    if not SLACK_WEBHOOK:
        log.warning("SLACK_WEBHOOK_MULTIVERSO nao configurado no .env — alerta Slack ignorado.")
        return

    df_alto = df_alto_override if df_alto_override is not None else df[df["risk_level"] == "ALTO"].copy()
    df_medio = df[df["risk_level"] == "MEDIO"].copy()

    if df_alto.empty:
        return  # sem alertas — "tudo limpo" é enviado pelo main() 1x/hora

    now = datetime.now(timezone(timedelta(hours=-3)))  # BRT

    # Usa métricas da campanha (quem optou) se disponível, senao cai no total geral
    if campaign_summary:
        total_players = campaign_summary["total_campaign_users"]
        total_wagered = campaign_summary["total_campaign_wagered"]
        players_label = "Na campanha"
        wagered_label = "Total apostado (campanha)"
    else:
        total_players = len(df)
        total_wagered = df["total_wagered"].astype(float).sum()
        players_label = "Total ativos"
        wagered_label = "Total apostado"

    # Tabela compacta — top 20 no bloco preformatted (limite ~3000 chars do Slack)
    TOP_TABLE = 20
    linhas = []
    linhas.append(f"{'ID':>12}  {'SCORE':>5}  {'TOTAL':>9}  {'P&L':>9}  FLAGS")
    linhas.append("-" * 70)

    for _, row in df_alto.head(TOP_TABLE).iterrows():
        user_id = int(row["user_id"])
        score   = int(row["risk_score"])
        total_w = float(row["total_wagered"])
        pnl     = float(row["pnl"])
        flags   = str(row["flags"])[:35]
        linhas.append(f"{user_id:>12}  {score:>5}  R${total_w:>7,.0f}  R${pnl:>7,.0f}  {flags}")

    if len(df_alto) > TOP_TABLE:
        linhas.append(f"... +{len(df_alto) - TOP_TABLE} jogadores ALTO risco")

    tabela = "\n".join(linhas)

    # Links clicáveis — top 20, ~80 chars cada = ~1600 chars total (dentro do limite)
    SMARTICO_BASE = "https://drive-6.smartico.ai/24105#/users"
    links_text = "\n".join(
        f"• <{SMARTICO_BASE}/{int(row['user_id'])}/show/cj_user_state_advance|{int(row['user_id'])}>"
        f"  [{int(row['risk_score'])}]  {str(row['flags'])[:40]}"
        for _, row in df_alto.head(20).iterrows()
    )

    # Monta mensagem Block Kit
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f":rotating_light: ANTI-ABUSE MULTIVERSO — {now.strftime('%d/%m/%Y %H:%M')} BRT"
            }
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Risco ALTO:*\n:red_circle: {len(df_alto)} jogadores"},
                {"type": "mrkdwn", "text": f"*Risco MÉDIO:*\n:large_yellow_circle: {len(df_medio)} jogadores"},
                {"type": "mrkdwn", "text": f"*{players_label}:*\n{total_players} jogadores"},
                {"type": "mrkdwn", "text": f"*{wagered_label}:*\nR$ {total_wagered:,.2f}"},
                {"type": "mrkdwn", "text": f"*Janela analisada:*\nÚltimas {hours}h"},
                {"type": "mrkdwn", "text": f"*Jogos monitorados:*\n6 Fortune (PG Soft)"},
            ]
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*:warning: Jogadores com Risco ALTO (top 20):*"
            }
        },
        {
            "type": "rich_text",
            "elements": [
                {
                    "type": "rich_text_preformatted",
                    "elements": [{"type": "text", "text": tabela}]
                }
            ]
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*:link: Abrir perfis no Smartico:*\n{links_text}"
            }
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": ":information_source: Score alto = mais sinais de fraude acumulados. Avaliar o conjunto de flags, não só o número."
                }
            ]
        }
    ]

    payload = {"blocks": blocks}

    try:
        resp = requests.post(
            SLACK_WEBHOOK,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        if resp.status_code == 200:
            log.info(f"Alerta Slack enviado: {len(df_alto)} jogadores ALTO risco")
        else:
            log.error(f"Slack retornou status {resp.status_code}: {resp.text}")
    except requests.RequestException as e:
        log.error(f"Falha ao enviar alerta Slack: {e}")


def send_slack_clean(df: pd.DataFrame, hours: int, campaign_summary: dict = None):
    """
    Envia mensagem no Slack confirmando que nenhum suspeito foi encontrado.
    Chamada 1x por hora pelo loop principal para dar visibilidade de que o bot está vivo.
    campaign_summary: dict com total_campaign_users e total_campaign_wagered (quem optou na campanha).
    """
    if not SLACK_WEBHOOK:
        return

    now = datetime.now(timezone(timedelta(hours=-3)))

    # Usa métricas da campanha (quem optou) se disponível, senao cai no total geral
    if campaign_summary:
        total_players = campaign_summary["total_campaign_users"]
        total_wagered = campaign_summary["total_campaign_wagered"]
        players_label = "Na campanha"
    else:
        total_players = len(df) if df is not None and not df.empty else 0
        total_wagered = df["total_wagered"].astype(float).sum() if total_players > 0 else 0.0
        players_label = "Jogadores ativos"

    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f":white_check_mark: *ANTI-ABUSE MULTIVERSO — {now.strftime('%d/%m/%Y %H:%M')} BRT*\n"
                    f"Nenhum jogador suspeito detectado até o momento.\n"
                    f"{players_label}: *{total_players}* | Total apostado: *R$ {total_wagered:,.2f}* | Janela: últimas {hours}h"
                )
            }
        }
    ]

    payload = {"blocks": blocks}
    try:
        resp = requests.post(
            SLACK_WEBHOOK,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        if resp.status_code == 200:
            log.info("Slack clean: nenhum suspeito — status enviado.")
        else:
            log.error(f"Slack retornou status {resp.status_code}: {resp.text}")
    except requests.RequestException as e:
        log.error(f"Falha ao enviar status limpo no Slack: {e}")


# ---------------------------------------------------------------------------
# OUTPUT
# ---------------------------------------------------------------------------

def print_report(df: pd.DataFrame, hours: int, campaign_summary: dict = None):
    """Imprime relatório clean e direto."""
    now = datetime.now(timezone(timedelta(hours=-3)))  # BRT
    header = f"\n{'='*70}"
    header += f"\n  ANTI-ABUSE MULTIVERSO — {now.strftime('%d/%m/%Y %H:%M')} BRT"
    header += f"\n  Janela: ultimas {hours}h | Jogos: 6 Fortune (PG Soft)"
    header += f"\n{'='*70}"
    print(header)

    if df.empty:
        print("\n  Nenhuma atividade detectada nos Fortune games.\n")
        return

    # Resumo geral
    total_players = len(df)
    high_risk = len(df[df["risk_level"] == "ALTO"])
    medium_risk = len(df[df["risk_level"] == "MEDIO"])
    total_wagered = df["total_wagered"].astype(float).sum()
    total_bets = df["total_bets"].astype(int).sum()

    print(f"\n  RESUMO:")
    print(f"  Jogadores ativos:  {total_players}")
    print(f"  Total apostado:    R$ {total_wagered:,.2f}")
    print(f"  Total de apostas:  {total_bets:,}")

    # Turnover da campanha (missoes): total apostado pelos participantes desde PROMO_START
    if campaign_summary and campaign_summary.get("total_campaign_users", 0) > 0:
        camp_users   = campaign_summary["total_campaign_users"]
        camp_wagered = campaign_summary["total_campaign_wagered"]
        print(f"\n  MISSOES (turnover da campanha, desde inicio):")
        print(f"  Participantes:     {camp_users}")
        print(f"  Turnover total:    R$ {camp_wagered:,.2f}")

    print(f"\n  RISCO ALTO:        {high_risk}")
    print(f"  RISCO MEDIO:       {medium_risk}")

    # Lista de alertas (ALTO + MEDIO)
    df_risk = df[df["risk_level"].isin(["ALTO", "MEDIO"])].copy()

    if df_risk.empty:
        print(f"\n  SEM ALERTAS -- nenhum jogador com comportamento suspeito.\n")
        return

    print(f"\n  {'-'*68}")
    print(f"  ALERTAS ({len(df_risk)} jogadores com risco)")
    print(f"  {'-'*68}")

    # Mostra top 30 por score (senão fica enorme)
    top_n = 30
    df_show = df_risk.head(top_n)

    for _, row in df_show.iterrows():
        risk_tag = "[ALTO]" if row["risk_level"] == "ALTO" else "[MEDIO]"
        user_id = int(row["user_id"])
        score = int(row["risk_score"])
        avg_bet = float(row["avg_bet"])
        total_w = float(row["total_wagered"])
        bets = int(row["total_bets"])
        games = int(row["unique_fortune_games"])
        pnl = float(row["pnl"])
        flags = row["flags"]

        print(f"\n  {risk_tag} user_id: {user_id}  |  SCORE: {score}")
        print(f"     Apostas: {bets} | Avg: R${avg_bet:.2f} | Total: R${total_w:,.2f} | P&L: R${pnl:,.2f}")
        print(f"     Jogos Fortune: {games}/6 | Flags: {flags}")

        if row.get("has_withdrawal", False):
            print(f"     >> SACOU R${float(row.get('total_withdrawn', 0)):,.2f} durante o periodo")

    if len(df_risk) > top_n:
        print(f"\n  ... e mais {len(df_risk) - top_n} jogadores com risco (ver CSV)")

    print(f"\n  {'-'*68}")
    print(f"  Proxima execucao em 5 minutos...")
    print(f"{'='*70}\n")


def export_json(df: pd.DataFrame):
    """Exporta resultado em JSON com timestamp."""
    now = datetime.now(timezone(timedelta(hours=-3)))
    os.makedirs("reports", exist_ok=True)
    filename = f"reports/anti_abuse_{now.strftime('%Y%m%d_%H%M')}.json"

    cols = [
        "user_id", "risk_score", "risk_level", "flags",
        "total_bets", "avg_bet", "total_wagered", "pnl",
        "avg_seconds_between_bets", "unique_fortune_games",
        "has_withdrawal", "total_withdrawn",
        "total_campaign_bonuses", "max_times_same_bonus",
        "first_bet", "last_bet",
    ]
    existing_cols = [c for c in cols if c in df.columns]

    records = df[existing_cols].copy()
    # Converte timestamps para string (JSON não serializa datetime nativo)
    for col in ["first_bet", "last_bet"]:
        if col in records.columns:
            records[col] = records[col].astype(str)

    output = {
        "gerado_em": now.strftime("%Y-%m-%d %H:%M:%S BRT"),
        "total_jogadores": len(df),
        "alto_risco": int((df["risk_level"] == "ALTO").sum()),
        "medio_risco": int((df["risk_level"] == "MEDIO").sum()),
        "jogadores": records.to_dict(orient="records"),
    }

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2, default=str)

    log.info(f"Relatorio exportado: {filename}")


# ---------------------------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Anti-Abuse Bot — Campanha Multiverso")
    parser.add_argument("--loop",  action="store_true", help="Rodar em loop a cada 5 min")
    parser.add_argument("--hours", type=int, default=DEFAULT_HOURS, help="Janela de analise em horas")
    parser.add_argument("--json",  action="store_true", help="Exportar JSON a cada execucao")
    args = parser.parse_args()

    log.info("Anti-Abuse Multiverso iniciado")
    log.info(f"Jogos monitorados: {list(FORTUNE_GAMES.values())}")
    log.info(f"Janela de analise: {args.hours}h | Loop: {args.loop}")

    while True:
        try:
            # Métricas de quem optou/entrou na campanha (janela: desde PROMO_START)
            # Usado nos alertas Slack no lugar de "jogadores ativos nos Fortune games"
            campaign_summary = query_campaign_participants()

            df = run_analysis(args.hours)
            print_report(df, args.hours, campaign_summary=campaign_summary)

            if args.json and not df.empty:
                export_json(df)

            global _clean_cycle_count, _all_high_risk
            if not df.empty:
                # Acumula jogadores ALTO risco (atualiza score/flags se mudaram)
                high_risk_now = df[df["risk_level"] == "ALTO"]
                new_ids = []
                for _, row in high_risk_now.iterrows():
                    uid = int(row["user_id"])
                    if uid not in _all_high_risk:
                        new_ids.append(uid)
                    _all_high_risk[uid] = {
                        "user_id": uid,
                        "risk_score": int(row["risk_score"]),
                        "flags": str(row["flags"]),
                        "total_wagered": float(row["total_wagered"]),
                        "pnl": float(row["pnl"]),
                    }

                total_accumulated = len(_all_high_risk)

                if len(high_risk_now) >= SLACK_MIN_HIGH_RISK or (new_ids and total_accumulated >= SLACK_MIN_HIGH_RISK):
                    if new_ids:
                        log.warning(f"⚠ {len(new_ids)} NOVO(S) jogador(es) ALTO risco: {new_ids}")
                    log.warning(f"⚠ Total acumulado ALTO risco: {total_accumulated} — enviando Slack...")
                    # Monta DataFrame acumulado para o alerta
                    df_accumulated = pd.DataFrame(list(_all_high_risk.values()))
                    df_accumulated["risk_level"] = "ALTO"
                    send_slack_alert(df, args.hours, df_alto_override=df_accumulated, campaign_summary=campaign_summary)
                    _clean_cycle_count = 0
                else:
                    _clean_cycle_count += 1
                    if _clean_cycle_count >= CLEAN_NOTIFY_EVERY:
                        log.info("30min sem novos suspeitos — enviando status limpo no Slack...")
                        send_slack_clean(df, args.hours, campaign_summary=campaign_summary)
                        _clean_cycle_count = 0

        except Exception as e:
            log.error(f"Erro na execucao: {e}", exc_info=True)

        if not args.loop or _shutdown_requested:
            break

        log.info(f"Aguardando {LOOP_INTERVAL}s ate proxima execucao...")
        time.sleep(LOOP_INTERVAL)

        if _shutdown_requested:
            break

    log.info("Anti-Abuse Multiverso finalizado.")


if __name__ == "__main__":
    main()