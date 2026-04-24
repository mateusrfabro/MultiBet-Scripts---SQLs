# Impacto Financeiro — Apostas Aceitas Apos Encerramento de Eventos

> **Para:** Altenar (via Castrin / Gusta)
> **De:** Squad 3 Intelligence Engine — MultiBet
> **Data:** 13/04/2026
> **Periodo analisado:** 90 dias (13/01/2026 a 12/04/2026)
> **Fonte:** AWS Athena — `vendor_ec2.tbl_sports_book_bets_info` + `tbl_sports_book_bet_details`

---

## 1. O que foi identificado

Jogadores estao realizando apostas em eventos esportivos cujo **horario de fim real** (`c_ts_realend`) ja passou ha mais de 30 minutos. O mercado deveria ter sido fechado pela Altenar quando o evento terminou, mas permaneceu aberto — permitindo que jogadores consultassem o resultado em fontes externas e apostassem no resultado ja conhecido.

**Criterio de deteccao:**
```
Data/hora da aposta (c_created_time) > Data/hora fim real do evento (c_ts_realend) + 30 minutos
```

---

## 2. Numeros de impacto (90 dias)

### Visao geral

| Metrica | Valor |
|---|---|
| **Jogadores com perfil de fraude** | **443** |
| **Apostas pos-evento desses jogadores** | **20.209** |
| **Stake total exposto** | **R$ 12.860.168** |
| **Profit extraido pelos jogadores (WIN)** | **R$ 6.341.384** |
| **Prejuizo liquido (profit - losses)** | **R$ 3.192.986** |
| Refunds operacionais | R$ 104.690 |

### Por faixa de Win Rate

| Faixa Win Rate | Jogadores | Profit extraido | Risco |
|---|---|---|---|
| **>= 95% (certeza de fraude)** | **88** | **R$ 747.222** | CRITICAL |
| 80 - 95% | 44 | R$ 395.473 | HIGH |
| 60 - 80% | 191 | R$ 1.309.967 | HIGH |
| 50 - 60% | 120 | R$ 740.324 | MEDIUM |

> **Nota:** 88 jogadores com Win Rate acima de 95% em apostas pos-evento representam **certeza estatistica de fraude**. Um apostador legitimo nao atinge essa taxa em multiplas apostas.

### Por esporte

| Esporte | Apostas | Jogadores | Profit extraido |
|---|---|---|---|
| **Basquete** | **1.831** | **247** | **R$ 1.580.105** |
| **Futebol** | **4.544** | **273** | **R$ 1.353.046** |
| E-sports | 489 | 42 | R$ 91.700 |
| Hoquei no Gelo | 40 | 29 | R$ 63.315 |
| Volei | 1.387 | 26 | R$ 46.572 |
| Beisebol | 29 | 13 | R$ 32.952 |
| Outros | 283 | 30 | R$ 25.297 |

> **Basquete e Futebol concentram 92% do prejuizo** (R$ 2,93M de R$ 3,19M total).

### Top 15 ligas mais exploradas

| Liga / Torneio | Apostas | Jogadores | Profit |
|---|---|---|---|
| **NCAAB** (basquete universitario EUA) | 469 | 109 | **R$ 546.829** |
| **NBA** | 559 | 86 | **R$ 540.123** |
| Bundesliga | 233 | 85 | R$ 166.780 |
| UEFA Champions League | 174 | 54 | R$ 150.741 |
| NCAA Feminino | 73 | 19 | R$ 101.831 |
| Euroliga (basquete) | 45 | 25 | R$ 92.361 |
| Premier League | 332 | 91 | R$ 78.655 |
| Superliga | 239 | 61 | R$ 75.227 |
| Serie A (Italia) | 276 | 85 | R$ 61.533 |
| Primeira Liga (Portugal) | 185 | 64 | R$ 53.660 |
| LaLiga 2 | 39 | 27 | R$ 49.306 |
| Campeonato Carioca | 137 | 42 | R$ 45.339 |
| Campeonato (generico) | 56 | 25 | R$ 42.662 |
| Liga Europa UEFA | 78 | 31 | R$ 39.973 |
| Taca FA Feminino | 6 | 2 | R$ 37.951 |

> **NCAAB + NBA sozinhos = R$ 1,09M de prejuizo.** Ligas de basquete americano sao as mais exploradas — possivelmente porque terminam em horarios em que o feed de settlement demora mais (madrugada no Brasil).

---

## 3. Top 10 jogadores (maior profit)

| Customer ID | Slips | Stake | Profit | Wins | Bets | Win Rate | Esporte |
|---|---|---|---|---|---|---|---|
| 28046905 | 16 | R$ 94.482 | **R$ 104.273** | 24 | 38 | **63,2%** | Basquete |
| 839571773947643 | 25 | R$ 264.995 | R$ 88.918 | 38 | 68 | 55,9% | Futebol |
| 963631773623158 | 27 | R$ 12.802 | R$ 55.723 | 30 | 58 | 51,7% | Basquete |
| 560981770728395 | 10 | R$ 1.444 | R$ 44.773 | 20 | 30 | **66,7%** | Futebol |
| 216091772408905 | 19 | R$ 77.370 | R$ 42.864 | 30 | 38 | **78,9%** | Basquete |
| 155631766865352 | 246 | R$ 258.874 | R$ 38.336 | 479 | 924 | 51,8% | Futebol |
| 926781771878328 | 3 | R$ 53.330 | R$ 37.221 | 6 | 6 | **100%** | Futebol |
| 251421774346159 | 36 | R$ 106.260 | R$ 34.547 | 64 | 94 | 68,1% | Basquete |
| 726671770920548 | 10 | R$ 63.601 | R$ 33.044 | 10 | 20 | 50,0% | Futebol |
| 118591773760286 | 5 | R$ 18.000 | R$ 32.800 | 10 | 10 | **100%** | Futebol |

---

## 4. Exemplo concreto — como funciona a fraude

### Caso investigado em profundidade (jogadores 764641775223027 e 777971772567301)

Em 04/04/2026, dois jogadores fizeram apostas em jogos de futebol que **ja haviam terminado ha 1-3 horas**:

| Hora aposta | Evento | Fim real | Delay | Stake | Resultado |
|---|---|---|---|---|---|
| 15:24 BRT | Bayer Leverkusen vs Wolfsburg (Bundesliga) | **13:00** | +2h24 | R$ 12.000 | WIN (+R$ 326) |
| 16:32 BRT | Chelsea vs Port Vale (Copa da Inglaterra) | **15:45** | +47min | R$ 8.000 | WIN (+R$ 6.400) |
| 15:18 BRT | Hamburgo vs FC Augsburg (Bundesliga) | **13:00** | +2h18 | R$ 15.000 | REFUND |
| 18:05 BRT | Chelsea vs Port Vale (mesmo jogo) | **15:45** | +2h20 | R$ 15.000 | WIN (+R$ 1.875) |
| 13:37 BRT | Go Ahead Eagles vs Zwolle (Eredivisie) | **11:15** | +2h22 | R$ 15.000 | WIN (+R$ 3.000) |

**O slip da Bundesliga (15:24 BRT) foi liquidado em 1 minuto e 14 segundos** — confirmando que o evento ja estava encerrado quando a aposta foi aceita.

Estes 2 jogadores extrairam **R$ 28.050 em 2 dias** com Win Rate de **100%** nas apostas liquidadas.

> Documentacao completa: `docs/deep_dive_live_delay_764_777.md`

---

## 5. Causa raiz

O feed de dados da Altenar/Sportradar envia sinal de "match end" quando o evento termina. Esse sinal deveria:
1. **Fechar todos os mercados do evento** (impedir novas apostas)
2. Disparar o settlement (liquidar apostas)

**O passo 1 esta falhando em parte dos eventos.** Os mercados ficam abertos apos o fim real, aceitando apostas. O settlement (passo 2) funciona normalmente — o que confirma que os resultados ja sao conhecidos pelo sistema no momento da liquidacao.

**Hipoteses tecnicas:**
- Falha no feed de "match end" de determinadas ligas (principalmente NCAAB/NCAA, basquete europeu)
- Dessincronizacao entre o feed de live e o feed de settlement
- Falta de timeout automatico (se nao receber "match end" apos duracao esperada do esporte, fechar por precaucao)

---

## 6. O que pedimos

### Opcao A — Correcao na Altenar (preferencial)

1. **Timeout de mercado:** Se passaram `duracao_esporte + 30 min` desde o inicio sem sinal de "match end", fechar o mercado automaticamente
2. **Validacao pre-settlement:** Antes de liquidar, checar se `c_ts_realend < c_created_time`. Se a aposta foi feita apos o fim do evento, anular automaticamente
3. **Monitoramento do feed:** Alertar quando um evento e dado como finalizado no Sportradar mas o mercado continua aberto na plataforma
4. **Retorno sobre eventos afetados:** Lista dos eventos nos ultimos 90 dias onde o mercado permaneceu aberto apos o fim real, para cruzamento com nossa deteccao

### Opcao B — Enquanto nao corrigem (deteccao propria)

Ja temos script standalone (`scripts/detect_post_match_bets.py`) rodando no nosso ambiente, que compara `c_created_time` com `c_ts_realend` e gera alerta quando detecta apostas pos-evento. Roda sob demanda para monitoramento.

---

## 7. Arquivos de referencia

| Arquivo | Descricao |
|---|---|
| `output/post_match_bets_90d_raw.csv` | 1,75M linhas — todas as apostas pos-evento (90 dias) |
| `output/post_match_fraudadores_90d.csv` | 443 jogadores filtrados com perfil de fraude |
| `scripts/detect_post_match_bets.py` | Script standalone de deteccao |
| `docs/deep_dive_live_delay_764_777.md` | Investigacao detalhada dos 2 casos com WR 100% |
| `docs/casos_fraude_observados_multibet.md` | Catalogo de todos os tipos de fraude observados |

---

**Squad 3 Intelligence Engine — MultiBet**
**Contato:** Mateus Fabro (analista) | Castrin (Head de Dados)