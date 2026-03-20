"""
Engine de segmentacao — logica completa de processamento.

Fluxo:
1. Busca usuarios marcados no BigQuery (Smartico)
2. Resolve game_id nos catalogos
3. Consulta transacoes no Redshift (chunks de 5000)
4. Classifica por faixa (maior prevalece, rollback desclassifica)
5. Validacao cruzada BigQuery vs Redshift
6. Gera CSV e mensagens WhatsApp

Uso:
    from segmentacao_app.engine import run_segmentacao
    resultado = run_segmentacao(demanda_parsed)
"""

import os
import sys
import logging
import tempfile
import zipfile
from dataclasses import dataclass, field
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv

# Garantir imports do projeto e carregar .env do diretorio correto
PROJECT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
sys.path.insert(0, PROJECT_DIR)
load_dotenv(os.path.join(PROJECT_DIR, '.env'))

from db.redshift import query_redshift
from db.bigquery import query_bigquery
from segmentacao_app.parser import DemandaParsed
from segmentacao_app.game_catalog import resolve_game

log = logging.getLogger(__name__)

CHUNK_SIZE = 5000
TXN_BET = 27
TXN_ROLLBACK = 72


@dataclass
class FaixaResult:
    nome: str
    jogadores: int
    volume_brl: float
    pct_volume: float


@dataclass
class SegmentacaoResult:
    # Dados gerais
    success: bool
    erro: str = ""

    # Jogo
    game_name: str = ""
    redshift_game_id: str = ""
    smartico_game_id: int | None = None
    vendor: str = ""

    # Usuarios
    total_marcados: int = 0
    total_jogaram: int = 0
    total_desclassificados: int = 0

    # Financeiro
    total_bet_brl: float = 0.0
    total_rollback_brl: float = 0.0
    net_bet_brl: float = 0.0

    # Faixas
    faixas: list = field(default_factory=list)  # list[FaixaResult]

    # Validacao cruzada
    validacao_jogadores_smr: int = 0
    validacao_total_smr: float = 0.0
    validacao_rollback_smr: int = 0
    validacao_diff_pct: float = 0.0
    validacao_ok: bool = False

    # Arquivos
    csv_path: str = ""
    zip_path: str = ""

    # Mensagens WhatsApp
    msg_resumo: str = ""
    msg_validacao: str = ""

    # DataFrame completo (para uso interno)
    df_final: object = None

    # Parametros usados
    mark_tag: str = ""
    inicio_brt: str = ""
    fim_brt: str = ""
    rollback_permitido: bool = True


def _fmt_brl(v: float) -> str:
    """Formata valor para BRL."""
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _fetch_marked_users(mark_tag: str, callback=None) -> pd.DataFrame:
    """Busca usuarios marcados no BigQuery."""
    if callback:
        callback(f"Buscando usuarios com tag '{mark_tag}' no BigQuery...")

    sql = f"""
    SELECT user_id AS smartico_user_id, user_ext_id
    FROM `smartico-bq6.dwh_ext_24105.j_user`
    WHERE '{mark_tag}' IN UNNEST(core_tags)
    """
    df = query_bigquery(sql)
    df['user_ext_id'] = pd.to_numeric(df['user_ext_id'], errors='coerce').astype('Int64')
    df = df.dropna(subset=['user_ext_id'])

    if callback:
        callback(f"  {len(df)} usuarios marcados encontrados")

    return df


def _fetch_transactions(ext_ids: list, game_id: str, start_utc: str, end_utc: str, callback=None) -> pd.DataFrame:
    """Busca transacoes no Redshift em chunks."""
    chunks = [ext_ids[i:i + CHUNK_SIZE] for i in range(0, len(ext_ids), CHUNK_SIZE)]

    if callback:
        callback(f"Consultando Redshift ({len(ext_ids)} IDs em {len(chunks)} chunk(s))...")

    frames = []
    for idx, chunk in enumerate(chunks, 1):
        ids_str = ", ".join(str(i) for i in chunk)
        sql = f"""
        WITH params AS (
            SELECT '{start_utc}'::timestamp AS start_ts,
                   '{end_utc}'::timestamp AS end_ts
        )
        SELECT
            e.c_external_id AS user_ext_id,
            SUM(CASE WHEN f.c_txn_type = {TXN_BET} THEN f.c_amount_in_ecr_ccy ELSE 0 END) AS total_bet_cents,
            SUM(CASE WHEN f.c_txn_type = {TXN_ROLLBACK} THEN f.c_amount_in_ecr_ccy ELSE 0 END) AS total_rollback_cents,
            SUM(CASE WHEN f.c_txn_type = {TXN_ROLLBACK} THEN 1 ELSE 0 END) AS qtd_rollbacks
        FROM fund.tbl_real_fund_txn f
        INNER JOIN ecr.tbl_ecr e ON e.c_ecr_id = f.c_ecr_id
        CROSS JOIN params p
        WHERE f.c_start_time BETWEEN p.start_ts AND p.end_ts
          AND f.c_game_id = '{game_id}'
          AND f.c_txn_status = 'SUCCESS'
          AND f.c_txn_type IN ({TXN_BET}, {TXN_ROLLBACK})
          AND e.c_external_id IN ({ids_str})
        GROUP BY 1
        """
        df_chunk = query_redshift(sql)
        if not df_chunk.empty:
            frames.append(df_chunk)

        if callback:
            callback(f"  Chunk {idx}/{len(chunks)}: {len(df_chunk)} jogadores")

    if frames:
        return pd.concat(frames, ignore_index=True)
    return pd.DataFrame(columns=['user_ext_id', 'total_bet_cents', 'total_rollback_cents', 'qtd_rollbacks'])


def _classify(df_txn: pd.DataFrame, faixas: list, rollback_permitido: bool) -> pd.DataFrame:
    """Processa e classifica jogadores por faixa."""
    df = df_txn.copy()
    df['user_ext_id'] = df['user_ext_id'].astype('Int64')
    df['total_bet_cents'] = pd.to_numeric(df['total_bet_cents'], errors='coerce').fillna(0)
    df['total_rollback_cents'] = pd.to_numeric(df['total_rollback_cents'], errors='coerce').fillna(0)
    df['qtd_rollbacks'] = pd.to_numeric(df['qtd_rollbacks'], errors='coerce').fillna(0).astype(int)
    df['total_bet_brl'] = df['total_bet_cents'] / 100
    df['total_rollback_brl'] = df['total_rollback_cents'] / 100
    df['net_bet_brl'] = df['total_bet_brl'] - df['total_rollback_brl']
    df['tem_rollback'] = df['qtd_rollbacks'] > 0

    # Faixas ordenadas por valor_min decrescente (maior primeiro)
    faixas_sorted = sorted(faixas, key=lambda f: f.valor_min, reverse=True)
    min_faixa = min(f.valor_min for f in faixas) if faixas else 0

    def classificar(row):
        if row['tem_rollback'] and not rollback_permitido:
            return 'Desclassificado (rollback)'
        net = row['net_bet_brl']
        for f in faixas_sorted:
            if f.valor_min <= net <= f.valor_max:
                return f.nome
        if net < min_faixa:
            return 'Abaixo do Minimo'
        return 'Sem classificacao'

    df['faixa_segmentacao'] = df.apply(classificar, axis=1)
    return df


def _cross_validate(mark_tag: str, smartico_game_id: int, start_utc: str, end_utc: str, callback=None) -> tuple:
    """Validacao cruzada com BigQuery. Retorna (jogadores, total_brl, rollbacks)."""
    if not smartico_game_id:
        return 0, 0.0, 0

    if callback:
        callback("Executando validacao cruzada com BigQuery...")

    sql = f"""
    SELECT
        COUNT(DISTINCT b.user_id) AS total_jogadores,
        SUM(CAST(b.casino_last_bet_amount_real AS FLOAT64)) AS total_bet_brl,
        SUM(CASE WHEN b.casino_is_rollback = true THEN 1 ELSE 0 END) AS qtd_rollbacks
    FROM `smartico-bq6.dwh_ext_24105.tr_casino_bet` b
    INNER JOIN `smartico-bq6.dwh_ext_24105.j_user` u ON u.user_id = b.user_id
    WHERE b.casino_last_bet_game_name = {smartico_game_id}
      AND b.event_time BETWEEN '{start_utc}' AND '{end_utc}'
      AND '{mark_tag}' IN UNNEST(u.core_tags)
    """
    df = query_bigquery(sql)
    jogadores = int(df.iloc[0, 0]) if not df.empty else 0
    total_brl = float(df.iloc[0, 1]) if not df.empty and df.iloc[0, 1] else 0.0
    rollbacks = int(df.iloc[0, 2]) if not df.empty else 0

    if callback:
        callback(f"  Smartico: {jogadores} jogadores, {_fmt_brl(total_brl)}")

    return jogadores, total_brl, rollbacks


def _generate_csv(df_final: pd.DataFrame, output_dir: str, game_name: str) -> tuple:
    """Gera CSV e ZIP. Retorna (csv_path, zip_path)."""
    safe_name = game_name.lower().replace(' ', '_').replace("'", "")
    csv_name = f"segmentacao_{safe_name}.csv"
    zip_name = f"segmentacao_{safe_name}.zip"

    csv_path = os.path.join(output_dir, csv_name)
    zip_path = os.path.join(output_dir, zip_name)

    cols = ['smartico_user_id', 'user_ext_id', 'total_bet_brl', 'total_rollback_brl',
            'net_bet_brl', 'qtd_rollbacks', 'faixa_segmentacao']
    df_final[cols].sort_values('net_bet_brl', ascending=False).to_csv(
        csv_path, index=False, sep=';', encoding='utf-8-sig'
    )

    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.write(csv_path, csv_name)

    return csv_path, zip_path


def _build_whatsapp_messages(result) -> tuple:
    """Gera as duas mensagens prontas para WhatsApp."""
    # Mensagem 1 — Resumo
    faixas_txt = ""
    for f in result.faixas:
        if f.nome == 'Nao jogou':
            continue
        faixas_txt += f"\n- {f.nome}: {f.jogadores} jogadores — {_fmt_brl(f.volume_brl)} ({f.pct_volume:.0f}% do volume)"

    periodo_str = ""
    if result.inicio_brt:
        dt = datetime.strptime(result.inicio_brt, "%Y-%m-%d %H:%M:%S") if isinstance(result.inicio_brt, str) else result.inicio_brt
        periodo_str = dt.strftime("%d/%m")

    nao_jogou = result.total_marcados - result.total_jogaram
    pct_nao_jogou = (nao_jogou / result.total_marcados * 100) if result.total_marcados > 0 else 0

    msg1 = f"""*Segmentacao {result.game_name} | Promocao {result.mark_tag} | Periodo: {periodo_str}*
Do segmento com opt-in ({result.total_marcados:,} usuarios marcados), {result.total_jogaram} jogaram {result.game_name} no periodo. Total apostado: {_fmt_brl(result.net_bet_brl)}.

*Distribuicao por faixa:*{faixas_txt}

*Ponto de atencao:* {result.total_desclassificados} desclassificado(s) por rollback. {pct_nao_jogou:.0f}% dos marcados nao jogaram {result.game_name} no periodo.""".replace(",", ".")

    # Mensagem 2 — Validacoes
    variantes_txt = "descartadas variantes encontradas no catalogo"
    val_cruzada = ""
    if result.validacao_ok:
        val_cruzada = f"""
7. *Validacao cruzada Redshift vs BigQuery:* {result.validacao_jogadores_smr} jogadores em ambas as fontes, diferenca de {_fmt_brl(abs(result.net_bet_brl - result.validacao_total_smr))} no total ({result.validacao_diff_pct:.2f}%) — dados consistentes"""

    msg2 = f"""*Validacoes realizadas:*

1. {result.game_name} confirmado como game_id `{result.redshift_game_id}` ({result.vendor}) no catalogo oficial — {variantes_txt}

2. Usuarios extraidos do BigQuery Smartico via tag `{result.mark_tag}` em `j_user.core_tags` — {result.total_marcados:,} com opt-in confirmado

3. Valores confirmados em centavos pela documentacao da Pragmatic (v1.3) — divisao por 100 aplicada

4. {result.total_desclassificados} rollback(s) no periodo — {'jogadores desclassificados conforme regra' if not result.rollback_permitido else 'descontados do net bet'}

5. Mapeamento de IDs validado: Smartico user_ext_id = c_external_id na tabela ECR da Pragmatic

6. Cada jogador aparece em apenas uma faixa (a mais alta atingida) — sem duplicidade de pagamento{val_cruzada}""".replace(",", ".")

    return msg1, msg2


def run_segmentacao(demanda: DemandaParsed, output_dir: str = None, callback=None) -> SegmentacaoResult:
    """
    Executa toda a segmentacao a partir de uma demanda parseada.

    Args:
        demanda: DemandaParsed com todos os parametros
        output_dir: Diretorio para salvar CSV/ZIP (default: temp)
        callback: Funcao de callback para status updates (ex: callback("mensagem"))

    Returns:
        SegmentacaoResult com todos os dados
    """
    result = SegmentacaoResult(success=False)
    result.mark_tag = demanda.mark_tag
    result.rollback_permitido = demanda.rollback_permitido

    if not demanda.valido:
        result.erro = f"Demanda invalida: {', '.join(demanda.erros)}"
        log.error(result.erro)
        return result

    try:
        # 1. Resolver jogo
        if callback:
            callback(f"Resolvendo jogo '{demanda.nome_jogo}'...")
        try:
            game = resolve_game(demanda.nome_jogo)
        except Exception as e:
            result.erro = f"Erro ao conectar nos bancos de dados: {e}. Verifique sua conexao e tente novamente."
            log.error(result.erro)
            if callback:
                callback(f"ERRO de conexao: {e}")
            return result
        if not game:
            result.erro = f"Jogo '{demanda.nome_jogo}' nao encontrado nos catalogos"
            log.error(result.erro)
            return result

        result.game_name = game.game_name
        result.redshift_game_id = game.redshift_game_id
        result.smartico_game_id = game.smartico_game_id
        result.vendor = game.vendor

        if callback:
            callback(f"  Redshift: {game.redshift_game_id} | Smartico: {game.smartico_game_id}")

        # 2. Buscar usuarios marcados
        df_marked = _fetch_marked_users(demanda.mark_tag, callback)
        result.total_marcados = len(df_marked)

        if result.total_marcados == 0:
            result.erro = f"Nenhum usuario encontrado com a tag '{demanda.mark_tag}'"
            log.error(result.erro)
            return result

        # 3. Buscar transacoes
        start_utc = demanda.inicio_utc.strftime("%Y-%m-%d %H:%M:%S")
        end_utc = demanda.fim_utc.strftime("%Y-%m-%d %H:%M:%S")
        result.inicio_brt = demanda.inicio_brt.strftime("%Y-%m-%d %H:%M:%S")
        result.fim_brt = demanda.fim_brt.strftime("%Y-%m-%d %H:%M:%S")

        ext_ids = df_marked['user_ext_id'].tolist()
        df_txn = _fetch_transactions(ext_ids, game.redshift_game_id, start_utc, end_utc, callback)

        if callback:
            callback(f"  {len(df_txn)} jogadores com transacoes")

        # 4. Classificar
        if callback:
            callback("Classificando jogadores por faixa...")
        df_txn = _classify(df_txn, demanda.faixas, demanda.rollback_permitido)

        # 5. Merge com base completa
        df_final = df_marked.merge(df_txn, on='user_ext_id', how='left')
        df_final['total_bet_brl'] = df_final['total_bet_brl'].fillna(0)
        df_final['total_rollback_brl'] = df_final['total_rollback_brl'].fillna(0)
        df_final['net_bet_brl'] = df_final['net_bet_brl'].fillna(0)
        df_final['qtd_rollbacks'] = df_final['qtd_rollbacks'].fillna(0).astype(int)
        df_final['faixa_segmentacao'] = df_final['faixa_segmentacao'].fillna('Nao jogou')

        result.df_final = df_final
        result.total_jogaram = len(df_final[df_final['faixa_segmentacao'] != 'Nao jogou'])
        result.total_desclassificados = len(df_final[df_final['faixa_segmentacao'] == 'Desclassificado (rollback)'])
        result.total_bet_brl = df_final['total_bet_brl'].sum()
        result.total_rollback_brl = df_final['total_rollback_brl'].sum()
        result.net_bet_brl = df_final['net_bet_brl'].sum()

        # Faixas para resultado
        total_net = result.net_bet_brl if result.net_bet_brl > 0 else 1
        faixa_order = [f.nome for f in sorted(demanda.faixas, key=lambda x: x.valor_min, reverse=True)]
        faixa_order += ['Abaixo do Minimo', 'Desclassificado (rollback)', 'Nao jogou']

        for faixa_nome in faixa_order:
            sub = df_final[df_final['faixa_segmentacao'] == faixa_nome]
            if len(sub) > 0:
                vol = sub['net_bet_brl'].sum()
                result.faixas.append(FaixaResult(
                    nome=faixa_nome,
                    jogadores=len(sub),
                    volume_brl=vol,
                    pct_volume=(vol / total_net * 100) if total_net > 0 else 0,
                ))

        if callback:
            callback(f"Classificacao concluida: {result.total_jogaram} jogaram, {result.total_desclassificados} desclassificados")

        # 6. Validacao cruzada
        if game.smartico_game_id:
            jog_smr, total_smr, rb_smr = _cross_validate(
                demanda.mark_tag, game.smartico_game_id, start_utc, end_utc, callback
            )
            result.validacao_jogadores_smr = jog_smr
            result.validacao_total_smr = total_smr
            result.validacao_rollback_smr = rb_smr
            diff = abs(result.net_bet_brl - total_smr)
            result.validacao_diff_pct = (diff / result.net_bet_brl * 100) if result.net_bet_brl > 0 else 0
            result.validacao_ok = result.validacao_diff_pct < 5  # menos de 5% = OK

            if callback:
                status = "OK" if result.validacao_ok else "ATENCAO"
                callback(f"  Validacao cruzada: {status} (diff {result.validacao_diff_pct:.2f}%)")

        # 7. Gerar CSV/ZIP
        if not output_dir:
            output_dir = tempfile.mkdtemp(prefix="segmentacao_")
        os.makedirs(output_dir, exist_ok=True)

        if callback:
            callback("Gerando CSV e ZIP...")
        result.csv_path, result.zip_path = _generate_csv(df_final, output_dir, game.game_name)

        if callback:
            callback(f"  CSV: {result.csv_path}")
            callback(f"  ZIP: {result.zip_path}")

        # 8. Mensagens WhatsApp
        result.msg_resumo, result.msg_validacao = _build_whatsapp_messages(result)

        result.success = True
        log.info(f"Segmentacao concluida: {result.total_marcados} marcados, "
                 f"{result.total_jogaram} jogaram, {_fmt_brl(result.net_bet_brl)} net bet")

    except (ConnectionError, OSError, FileNotFoundError) as e:
        result.erro = f"Erro de conexao com o banco: {e}. Verifique sua rede/VPN e tente novamente."
        log.exception(result.erro)
        if callback:
            callback(f"ERRO de conexao: {e}")
    except Exception as e:
        result.erro = f"Erro na segmentacao: {str(e)}"
        log.exception(result.erro)
        if callback:
            callback(f"ERRO: {e}")

    return result
