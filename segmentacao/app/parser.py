"""
Parser de demandas de segmentacao de promocoes.

Recebe o texto padrao enviado pelo CRM/James e extrai:
- Tag de marcacao (mark user)
- Nome do jogo
- Periodo (inicio/fim em BRT e UTC)
- Faixas com valores min/max
- Regra de rollback

Uso:
    from segmentacao_app.parser import parse_demanda
    result = parse_demanda(texto)
"""

import re
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta

log = logging.getLogger(__name__)


@dataclass
class Faixa:
    nome: str
    valor_min: float
    valor_max: float  # float('inf') para "ou mais"


@dataclass
class DemandaParsed:
    mark_tag: str
    nome_jogo: str
    inicio_brt: datetime
    fim_brt: datetime
    inicio_utc: datetime
    fim_utc: datetime
    faixas: list  # list[Faixa]
    rollback_permitido: bool
    erros: list = field(default_factory=list)  # problemas no parsing

    @property
    def valido(self) -> bool:
        return len(self.erros) == 0 and self.mark_tag and self.nome_jogo and self.faixas


def _extrair_mark_tag(texto: str) -> str | None:
    """Extrai a tag de marcacao (mark user: XXXX)."""
    # Padroes: "mark user: TAG", "mark user:TAG", "marcados com mark user: TAG"
    m = re.search(r'mark\s*user\s*:\s*(\S+)', texto, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return None


def _extrair_nome_jogo(texto: str) -> str | None:
    """Extrai o nome do jogo a partir do texto."""
    # Padrao: "no jogo XXXX no periodo" ou "no jogo XXXX no período"
    m = re.search(r'no\s+jogo\s+(.+?)\s+no\s+per[ií]odo', texto, re.IGNORECASE)
    if m:
        return m.group(1).strip()

    # Fallback: "jogo XXXX"
    m = re.search(r'jogo\s+(.+?)(?:\s+no\s+|\s+entre|\s*$)', texto, re.IGNORECASE)
    if m:
        return m.group(1).strip()

    return None


def _extrair_periodo(texto: str) -> tuple:
    """
    Extrai inicio e fim do periodo em BRT e converte para UTC.
    Retorna (inicio_brt, fim_brt, inicio_utc, fim_utc) ou Nones.
    """
    # Padrao: "das 11h do dia 10/03/2026 as 23h59 do dia 10/03/2026"
    # Variantes: "às", "as", "11h", "11h00", "23h59", "23:59"
    padrao = (
        r'(?:das?|a\s+partir\s+das?)\s*'
        r'(\d{1,2})[hH:]?(\d{0,2})\s*'
        r'(?:do\s+dia\s+)?(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})\s*'
        r'[àa]s?\s*'
        r'(\d{1,2})[hH:]?(\d{0,2})\s*'
        r'(?:do\s+dia\s+)?(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})'
    )
    m = re.search(padrao, texto, re.IGNORECASE)
    if not m:
        return None, None, None, None

    h1 = int(m.group(1))
    min1 = int(m.group(2)) if m.group(2) else 0
    d1, mo1, y1 = int(m.group(3)), int(m.group(4)), int(m.group(5))

    h2 = int(m.group(6))
    min2 = int(m.group(7)) if m.group(7) else 59  # "23h" vira 23:59
    d2, mo2, y2 = int(m.group(8)), int(m.group(9)), int(m.group(10))

    try:
        inicio_brt = datetime(y1, mo1, d1, h1, min1, 0)
        # Se hora fim = 23 e minuto = 59, usar 23:59:59
        sec2 = 59 if (h2 == 23 and min2 == 59) else 0
        fim_brt = datetime(y2, mo2, d2, h2, min2, sec2)
    except ValueError:
        return None, None, None, None

    # BRT → UTC (+3h)
    inicio_utc = inicio_brt + timedelta(hours=3)
    fim_utc = fim_brt + timedelta(hours=3)

    return inicio_brt, fim_brt, inicio_utc, fim_utc


def _extrair_faixas(texto: str) -> list:
    """Extrai as faixas de segmentacao do texto."""
    faixas = []

    # Padrao: "Faixa N: Apostas entre R$X a R$Y" ou "Faixa N: Apostas de R$X ou mais"
    linhas = texto.split('\n')
    for linha in linhas:
        linha = linha.strip()

        # Faixa com "ou mais" / "acima" / "a partir"
        m = re.match(
            r'Faixa\s*(\d+)\s*:\s*.*?R\$\s*([\d.,]+)\s*(?:ou\s+mais|acima|a\s+partir)',
            linha, re.IGNORECASE
        )
        if m:
            num = m.group(1)
            val_min = _parse_valor(m.group(2))
            faixas.append(Faixa(nome=f"Faixa {num}", valor_min=val_min, valor_max=float('inf')))
            continue

        # Faixa com range: "R$X a R$Y"
        m = re.match(
            r'Faixa\s*(\d+)\s*:\s*.*?R\$\s*([\d.,]+)\s*a\s*R\$\s*([\d.,]+)',
            linha, re.IGNORECASE
        )
        if m:
            num = m.group(1)
            val_min = _parse_valor(m.group(2))
            val_max = _parse_valor(m.group(3))
            faixas.append(Faixa(nome=f"Faixa {num}", valor_min=val_min, valor_max=val_max))
            continue

        # Faixa com "entre R$X e R$Y"
        m = re.match(
            r'Faixa\s*(\d+)\s*:\s*.*?R\$\s*([\d.,]+)\s*e\s*R\$\s*([\d.,]+)',
            linha, re.IGNORECASE
        )
        if m:
            num = m.group(1)
            val_min = _parse_valor(m.group(2))
            val_max = _parse_valor(m.group(3))
            faixas.append(Faixa(nome=f"Faixa {num}", valor_min=val_min, valor_max=val_max))
            continue

    # Ordenar por valor_min decrescente (faixa mais alta primeiro — pra classificacao)
    faixas.sort(key=lambda f: f.valor_min, reverse=True)
    return faixas


def _parse_valor(s: str) -> float:
    """Converte string de valor para float. Ex: '1.000,00' → 1000.0, '300' → 300.0."""
    s = s.strip()
    # Formato BR: 1.000,99 → remover pontos, trocar virgula por ponto
    if ',' in s:
        s = s.replace('.', '').replace(',', '.')
    return float(s)


def _extrair_rollback(texto: str) -> bool:
    """Verifica se rollback eh permitido. Retorna True se permitido, False se nao."""
    texto_lower = texto.lower()
    if 'não é permitido rollback' in texto_lower or 'nao e permitido rollback' in texto_lower:
        return False
    if 'não permitido rollback' in texto_lower or 'sem rollback' in texto_lower:
        return False
    # Default: permitido (so desconta do net bet, nao desclassifica)
    return True


def parse_demanda(texto: str) -> DemandaParsed:
    """
    Interpreta o texto padrao da demanda de segmentacao.

    Args:
        texto: Texto completo da demanda (colado do WhatsApp/email)

    Returns:
        DemandaParsed com todos os campos extraidos
    """
    erros = []

    mark_tag = _extrair_mark_tag(texto)
    if not mark_tag:
        erros.append("Nao foi possivel extrair a tag de marcacao (mark user)")

    nome_jogo = _extrair_nome_jogo(texto)
    if not nome_jogo:
        erros.append("Nao foi possivel extrair o nome do jogo")

    inicio_brt, fim_brt, inicio_utc, fim_utc = _extrair_periodo(texto)
    if not inicio_brt:
        erros.append("Nao foi possivel extrair o periodo (datas/horarios)")

    faixas = _extrair_faixas(texto)
    if not faixas:
        erros.append("Nao foi possivel extrair as faixas de segmentacao")

    rollback_permitido = _extrair_rollback(texto)

    result = DemandaParsed(
        mark_tag=mark_tag or "",
        nome_jogo=nome_jogo or "",
        inicio_brt=inicio_brt,
        fim_brt=fim_brt,
        inicio_utc=inicio_utc,
        fim_utc=fim_utc,
        faixas=faixas,
        rollback_permitido=rollback_permitido,
        erros=erros,
    )

    if result.valido:
        log.info(f"Demanda parseada: jogo={nome_jogo}, tag={mark_tag}, "
                 f"periodo={inicio_brt} a {fim_brt} BRT, {len(faixas)} faixas, "
                 f"rollback={'permitido' if rollback_permitido else 'desclassifica'}")
    else:
        log.warning(f"Erros no parsing: {erros}")

    return result


if __name__ == "__main__":
    # Teste rapido com demanda real
    texto_teste = """
    Usuarios com opt-in marcados com mark user: GIRE_GANHE_SWEET_110326
    Segmento: https://drive-6.smartico.ai/24105#/j_segment/28799

    Regras:

    O usuario precisa ter feito opt-in e ter a tag de marcacao
    Realizar apostas entre as faixas abaixo no jogo Sweet Bonanza no periodo das 11h do dia 11/03/2026 as 23h59 do dia 11/03/2026 no UTC-03:00.
    Nao e permitido rollback.

    Faixa 1: Apostas entre R$15 a R$49,99
    Faixa 2: Apostas entre R$50 a R$99,99
    Faixa 3: Apostas entre R$100 a R$299,99
    Faixa 4: Apostas de R$300,00 ou mais

    O usuario so pode estar em uma faixa.
    """

    logging.basicConfig(level=logging.INFO)
    result = parse_demanda(texto_teste)
    print(f"\nValido: {result.valido}")
    print(f"Tag: {result.mark_tag}")
    print(f"Jogo: {result.nome_jogo}")
    print(f"Periodo BRT: {result.inicio_brt} a {result.fim_brt}")
    print(f"Periodo UTC: {result.inicio_utc} a {result.fim_utc}")
    print(f"Rollback permitido: {result.rollback_permitido}")
    print(f"Faixas ({len(result.faixas)}):")
    for f in result.faixas:
        print(f"  {f.nome}: R${f.valor_min:.2f} a R${f.valor_max}")
    if result.erros:
        print(f"Erros: {result.erros}")
