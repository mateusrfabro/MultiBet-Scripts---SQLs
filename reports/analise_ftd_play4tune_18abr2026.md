# Análise FTD Conversion Play4Tune — 18-19/04/2026

> **🚨 ATUALIZAÇÃO 19/04 10:18 BRT — FARMER VOLTOU EM ESCALA.**
> Sem bloqueio dos prefixos durante a noite, o farmer retornou na janela prevista (01h-05h BRT = 09h-13h PKT) e **completou a sequência de phones**: ontem usou sufixos `1980-1997` (16 contas), hoje madrugada usou `1970-1979` + `1988` (11 contas — preencheu o gap exato).
>
> **Status 19/04 até 10:18 BRT:**
> - 63 cadastros, 21 FTDs, **CV reportado 33,3%** (vs baseline 17,8%)
> - **11 contas no novo batch +92341374** (sufixos 1970-1979 + 1988)
> - 3 contas com `whyknapp.com` (mesmos domínios temp-mail de ontem)
> - 13 de 21 FTDs hoje foram sub-2min (62%)
> - **9 saques completados hoje, todos sem cumprir rollover** (faixa 2-14% do exigido)
> - Conta `m33kwhBFIWQVNN11` (já reportada ontem) **voltou e sacou Rs 2.000** hoje — REPEAT farmer
> - Maior saque: `AH5kr6vsGRUInVH7` Rs 2.260 (phone fora do prefixo — possível farmer paralelo)
>
> **Ação:** Compliance NÃO bloqueou os prefixos como recomendado ontem → ataque escalou. Bloqueio precisa ser feito AGORA antes da janela das 11h-14h BRT.

---



**Demanda:** Castrin (audio 18/04) — entender por que CV FTD subiu de ~22% (normal) para 40% hoje. É natural, fraude, ou "tá liso"?
**Banco:** supernova_bet (Paquistão) | **Moeda:** PKR | **Extração:** 18/04/2026 14:44 BRT

---

## 🚨 INSIGHT CRÍTICO — Bomba-relógio matemática no catálogo (descoberto durante análise)

**Enquanto investigava o farmer, descobri: 27 dos 136 jogos da Play4Tune têm RTP > 100% no catálogo — e o RTP realizado bate com o catálogo. A casa está em prejuízo sistemático nesses jogos.**

| Métrica | Valor |
|---|---|
| Jogos afetados | **27 de 136 (20%)** — todos ativos |
| Turnover histórico nesses jogos | Rs 370.345 (R$ 6.622) |
| Casa pagou em wins | Rs 420.108 (R$ 7.512) |
| **GGR casa** | **-Rs 49.763 (R$ -890) prejuízo** |
| RTP realizado consolidado | **113,44%** (casa paga 13% a mais do que arrecada) |

**Os piores ofensores:**
- **CRASH II** (RTP 122,80%): Rs 156.210 apostado, prejuízo casa Rs -35.618 (R$ -637). Já tem 35 jogadores nele.
- **CASHPOWER MANIA** (115,42%): prejuízo Rs -2.337
- **ORIENTAL RICHES** (132,07%): prejuízo Rs -1.353
- **CAR ROULETTE** (500%!), **THREE DICE** (200%), **BACCARAT** (180%) — pouco jogados ainda, mas matemática explosiva se descobertos

**Por que isso importa pro farmer de hoje:** o batch apostou 23% do turnover em jogos RTP>100% (Crash II, Piggy Bankin, Dice) — **ele já descobriu parcialmente**. Se otimizar (só CAR ROULETTE / THREE DICE), cada conta vira EV+ garantido: dep Rs 200 + bônus Rs 200 → em CAR ROULETTE (RTP 500%) → payout esperado Rs 1.000 → farmer saca Rs 600 (max 3× bônus) = **lucro Rs 400 por conta**.

**Ação urgente (hoje) — passar pro Gabriel Barbosa (CTO):**
1. Auditar com provider **2J Games** por que 27 jogos têm RTP > 100%. Bug de configuração, erro de integração, ou intencional?
2. Enquanto não resolve: **desativar os 10 piores** (CAR ROULETTE, THREE DICE, BACCARAT, COLOR GAME, CRAZY LUDO, LABUBU'S, JIN LOONG, COSMIC BLITZ, ORIENTAL RICHES, LUCKY LOTTERY — RTP > 125%) ou limitar max_bet pra conter exposição.
3. Auditar GGR de todo o catálogo — pode haver mais jogos com RTP > 100% pontual em reviews específicos que estão distorcendo mart.

Este é um **problema estrutural maior que o batch de hoje**. O farmer de hoje é sintoma; o catálogo quebrado é a causa raiz do risco.

---

## Resumo Executivo (o que responder ao Castrin) — revisado pós-auditoria (auditor + risk-analyst)

**NÃO é natural. É bonus farming ORGANIZADO e SOFISTICADO — operação nova, nascendo hoje, com tooling (proxy pool + device cloaking). Classificação: ALERTA P1, score 80/100 (HIGH).**

- CV de hoje: **39,7%** (31 FTD / 78 cadastros) — baseline 9d anteriores: **17,8%**. Salto de **+21,9pp**.
- **17-21 das 78 contas** são parte do batch (batch real maior que os 16 phones sequenciais). O **core batch** = **16 contas com phones sequenciais `+923413741980 a 1997`** (operadora Zong/PK). **Mais 1 conta `+923047208563` fora do prefixo usa o mesmo domínio descartável** (`wetuns.com`) — é do mesmo farmer.
- **Zero histórico:** prefixo `+9234137419xx` e domínios `wetuns.com`/`whyknapp.com` têm **ZERO cadastros nos 15 dias anteriores**. Hoje: 16 + 7 contas. **É nascimento de operação nova.**
- **Infraestrutura sofisticada:** as 16 contas do prefixo usam **13 IPs distintos** — não é bot de 1 máquina, é **pool de proxy/VPN** (provável SIM farm + anti-detect browser + residential proxy).
- **100% dos 16 ativaram o Welcome Bonus** (100%, rollover 75x), depositaram o **mínimo (Rs 200-300)**, jogaram 1-4 jogos só pra liberar rollover, e **6 já sacaram**.
- Entre **todos os 31 FTDs de hoje**: 21 ativaram bônus, **10 já sacaram** (32%), 71% sem UTM (vs 38% ontem).
- **Gap de compliance identificado:** a conta `sZ8M2Jn3BBryYd31` teve **3 saques FAILED de Rs576 sem `reviewed_by` preenchido** — gateway rejeitou mas ninguém do compliance revisou. Regulatório PK exige SAR (Suspicious Activity Report).
- Nenhum desses 78 cadastros tem `affiliate_code`, `affiliate_id` ou `referred_by` preenchido — **tráfego não identificado**.
- **Concentração em jogos RTP alto:** RTP médio do batch = **96,03%** vs plataforma 91,19% = **+4,84pp**. Farmer escolheu Fortune Tiger (99,64%), Crash II (122%), Piggy Bankin (100,33%), Dice (116%) — jogador comum não conhece RTP, farmer escolhe por RTP pra minimizar perda no rollover. **Smoking gun adicional.**
- **Impacto financeiro REALIZADO hoje: +Rs 729 (+R$ 13) a favor da casa.** O rollover 75× segurou o dano: farmer depositou Rs 4.200, recebeu Rs 4.200 de bônus, apostou Rs 21.187 (turnover), sacou Rs 3.471. GGR bruto do batch = Rs 2.924. **O prejuízo ainda é pequeno — mas o padrão é replicável e escalável.**
- **Ataque AINDA EM ANDAMENTO:** última atividade do batch foi há **15min** (conta `8x1rJh3nGkvjIlRZ` jogando casino agora). Ação precisa ser HOJE, não amanhã.
- **CV orgânico hoje (removendo batch) = 23,4%** — totalmente dentro do baseline (17,8%). **100% do "crescimento" reportado é artefato do farmer.** Sem o batch, o dia era normal. Qualquer decisão de marketing que pegue o 39,7% estaria errada.
- **Cadência do farmer:** picos em **12h BRT (20h PKT)** e **02-05h BRT (10-13h PKT)** = horário comercial + após-almoço no Paquistão. Gap médio entre cadastros = 44min (não é bot instantâneo — é humano operando ou bot com delay). **Próxima janela prevista:** amanhã 02-05h BRT e 11-14h BRT.
- **Farmer é NOVO:** zero ocorrências do prefixo `+92341374198x/199x` e dos domínios `wetuns.com`/`whyknapp.com` em 16 dias de histórico anterior. **Operação nascendo hoje.** Prefixo já quase saturado (16/20 slots usados com gaps em 1988/1989) → amanhã ele deve migrar pra prefixo vizinho (`+9234137420xx`).
- **6 das 17 contas já são lucrativas pro farmer** (sacaram mais do que depositaram, lucro de Rs 200-400 cada). O rollover 75× não é suficiente contra os jogos RTP > 100% (ver insight crítico acima).

**Onde esses caras estão clicando:** NÃO veio dos canais pagos oficiais (Meta/Facebook/Instagram responderam por só 9 dos 31 FTDs hoje — vs 26 ontem). **22 dos 31 FTDs são totalmente sem UTM** (link direto, orgânico, ou compartilhado em grupo fechado).

**Depositaram E jogaram:** sim — todos os 31 tiveram aposta. **MAS** turnover médio é Rs 2.515 (US$ 9), rounds=4.768 em 31 contas (≈150/conta). Perfil "cumprir rollover mínimo e sacar", não de jogador real.

**Ação recomendada (hoje + 48h):**
1. **HOJE (preventivo):** Bloquear cadastros com prefixo `+9234137419xx` e preventivamente `+9234137420xx` (alerta no gateway — preferir "flag for review" antes de blacklist dura, pois é prefixo Zong legítimo).
2. **HOJE:** Bloquear domínios temp-mail no formulário de cadastro: `wetuns.com`, `whyknapp.com` + lista pública disposable-email-domains (ex: Maildrop, EmailOnDeck, MohMal).
3. **HOJE:** Investigar as 3 `WITHDRAW FAILED Rs576` da conta `sZ8M2Jn3BBryYd31` sem `reviewed_by` — entender o motivo do gateway (AVS? velocity? limite?) e fechar gap de compliance AML/KYC.
4. **48h:** Revisar Welcome Bonus — (a) KYC pré-saque, (b) elevar min deposit, (c) 1 bônus por phone/device (não por conta).
5. **Roadmap (pedir ao Gabriel/CTO):** capturar **device fingerprint** no registro — hoje só temos `transactions.ip_address` (provavelmente IP do backend AWS); sem fingerprint, bloqueio é sempre reativo.
6. **Compliance PK:** formalizar SAR (Suspicious Activity Report) — obrigação regulatória local.

**O salto não sustenta.** Amanhã CV deve cair porque o batch não se repete — a não ser que o farmer abra outra sequência.

---

## Evidências

### 1. Série histórica (cohort CV FTD)

| Data | Cadastros | FTDs (cohort) | CV | FTDs evento |
|------|-----------|---------------|-----|-------------|
| 07/04 | 16 | 3 | 18,8% | 0 |
| 08/04 | 163 | 26 | 16,0% | 25 |
| 09/04 | 152 | 16 | 10,5% | 17 |
| 10/04 | 101 | 9 | 8,9% | 9 |
| 11/04 | 39 | 9 | 23,1% | 9 |
| 12/04 | 100 | 29 | 29,0% | 27 |
| 13/04 | 112 | 21 | 18,8% | 23 |
| 14/04 | 150 | 24 | 16,0% | 22 |
| 15/04 | 214 | 28 | 13,1% | 26 |
| 16/04 | 176 | 33 | 18,8% | 34 |
| 17/04 | 188 | 42 | 22,3% | 42 |
| **18/04** | **78** | **31** | **39,7%** | **38** |

**Baseline 9d:** 17,8%. Hoje: **+21,9pp**.

### 2. Batch +92341374xxx (smoking gun)

16 contas com phones sequenciais `+92341374198[0-7]` e `+92341374199[0-7]`, cadastradas ao longo do dia (02:41-14:24 BRT). Todas ativaram o Welcome Bonus. Detalhamento:

| username | phone | dep (Rs) | saque (Rs) | bônus | jogos distintos |
|---|---|---|---|---|---|
| JC5uOMwD9xuffKpr | +923413741980 | 300 | 0 | 1 | 1 |
| hYpZ9HdypO3pLtcD | +923413741981 | 300 | 600 | 1 | 1 |
| uuZSMBdC60xapNgG | +923413741982 | 300 | 500 | 1 | 4 |
| RRMSn3Or8IpStzUe | +923413741983 | 300 | 0 | 1 | 1 |
| WnElZAi9fZx00cp6 | +923413741984 | 200 | 0 | 1 | 1 |
| sZ8M2Jn3BBryYd31 | +923413741985 | 200 | 576 | 1 | 1 |
| EloHJCp5U0mcw2kd | +923413741986 | 200 | 535 | 1 | 1 |
| 8x1rJh3nGkvjIlRZ | +923413741987 | 200 | 0 | 1 | 2 |
| zAJVKFxQyQ5R7WPK | +923413741990 | 200 | 0 | 1 | 1 |
| 5vVJYHLDfX42xuvf | +923413741991 | 200 | 0 | 1 | 1 |
| 7TTzOwgLox6DeWnV | +923413741992 | 200 | 0 | 1 | 1 |
| MHXlrTznEKz8FwpN | +923413741993 | 200 | 0 | 1 | 1 |
| SOCVpCHYaqKsMG5F | +923413741994 | 300 | 0 | 1 | 2 |
| g0CWBJJbQtObGsZq | +923413741995 | 300 | 0 | 1 | 2 |
| KZ9JwZEeukWH3Ume | +923413741996 | 300 | 700 | 1 | 1 |
| 7afqeya9n1nJ0rl4 | +923413741997 | 200 | 560 | 1 | 2 |

**Padrão clássico de farmer:**
- Phones adquiridos em bloco de uma mesma operadora (prefixo 4G Zong Pakistan)
- Emails de serviços temp mail (`wetuns.com`, `whyknapp.com`)
- Usernames gerados por script (16 chars alfanuméricos random)
- Depósitos iguais ao min do bônus
- Saque assim que libera rollover

### 3. Origem (UTMs) — 18/04 vs 17/04

| Source | 18/04 | 17/04 | 16/04 |
|--------|-------|-------|-------|
| (null / sem UTM) | **22** (71%) | 16 (38%) | 7 (21%) |
| fb | 4 | 21 | 15 |
| ig | 5 | 5 | 10 |
| th | 0 | 2 | 1 |

**Hoje o % de FTDs sem UTM quase dobrou** vs ontem (71% vs 38%) → tráfego entrou fora dos canais trackeados (link direto compartilhado, grupo fechado, ou link adulterado).

### 4. Outros sinais de fraude (reforçados pela validação do risk-analyst)

- **Tempo registro → FTD:** mediana **1,4min** | 21 de 31 (68%) fizeram FTD em menos de 2min após cadastro (bot ou humano seguindo checklist).
- **Valores de depósito:** 300 (13 contas) ou 200 (10 contas) = 74% no mínimo do bônus.
- **Saques:** 10 contas de 31 (32%) já executaram saque COMPLETED; **1 conta (`sZ8M2Jn3BBryYd31`) teve 3 saques FAILED de Rs 576 sem `reviewed_by` preenchido** — gap de compliance AML/KYC a investigar.
- **Affiliate:** zero contas com `affiliate_code` ou `referred_by` — não veio de afiliado cadastrado.
- **Batch maior que 16:** fora do prefixo `+9234137419xx`, a conta `utl2FFfrQR7Qj6` (phone `+923047208563`) usa o mesmo `wetuns.com` e tem mesmo perfil (dep Rs300) — pertence ao mesmo farmer. Total real: ~17-21 contas.
- **Pool de IPs (proxy/VPN):** as 16 contas do prefixo vieram de **13 IPs distintos** — não é bot de uma máquina, é operação com tooling (SIM farm + residential proxy + anti-detect browser). Risco mais sofisticado que farmer amador.
- **Histórico zero:** prefixo `+9234137419xx` e domínios `wetuns.com`/`whyknapp.com` têm **zero cadastros** em 15 dias anteriores. Nasceu hoje = operação nova, risco de escalar amanhã com prefixos vizinhos (`+9234137420xx`).
- **Phone malformado:** conta `qm5QAiZ3uAyDYw` com phone `+92454559782` (11 dígitos em vez de 12) + dep Rs300 / saque Rs1000 (3× dep) — investigar.
- **Fingerprint gap validado:** em 32.083 transactions das últimas 24h, **`user_agent` = 100% NULL**. Sem user-agent/device_id, bloqueio só consegue usar IP (que nem é do cliente) + phone + email. Daí a necessidade urgente de fingerprint estrutural.
- **Teste interno do dev descartado:** zero `reviewed_by` preenchido nessas contas, zero `ADJUSTMENT_*`. Não é teste operacional. É farmer externo.

### 5. Bônus ativados

- Hoje: 21 ACTIVE + 10 CANCELLED (total 31 = todos os FTDs pegaram).
- Welcome Bonus 100% com rollover 75x e min dep 200 PKR é exatamente o alvo desse tipo de farmer.

---

## Como interpretar

| Hipótese | Score | Por quê |
|---|---|---|
| Natural (campanha nova performando) | **Baixo** | Canais pagos caíram em volume. 71% sem UTM. Perfil de jogo == rollover mínimo. |
| Tráfego orgânico viralizado | **Baixo** | Sem pico de sessions/visitas fora do padrão. Sem hashtag/share específico. |
| **Bonus abuse em batch (1 ator)** | **ALTO** | 16 phones sequenciais, emails descartáveis, dep fixo, saque rápido. Clássico. |
| Fraude de pagamento (chargeback) | **Baixo** | Todos DEPOSIT estão COMPLETED, não FAILED nem reviewed_by. Gateway aceitou. |
| KPI errado (erro de cálculo) | **Baixo** | Reproduzi com cohort e com event, ambos dão ~40% (39,7% e 48,7%). Número confirmado. |

---

## Fonte de dados

- **Script principal:** `scripts/analise_ftd_conversion_play4tune.py`
- **Script validação (risk-analyst):** `scripts/risk_validate_batch_18abr.py`
- **Banco:** `supernova_bet` (PostgreSQL 15.14)
- **Tabelas:** `users`, `transactions`, `casino_user_game_metrics`, `user_marketing_events`, `user_sessions`, `bonus_activations`
- **Filtro test users:** UNION (heurística + lógica dev manual adjustment), 72 contas excluídas. Whitelist DP/SQ aplicada (4 usuários reais).
- **Timezone:** banco UTC → BRT na apresentação. Hoje em BRT = hoje em PKT (alinhado até meia-noite BRT/22h PKT).
- **Validação squad:** auditor (GO com 2 ressalvas aplicadas) + risk-analyst (ENDOSSO FORTE, score 80/100 HIGH, ALERTA P1) — 18/04/2026 15:00 BRT.

---

## Glossário

- **FTD (First Time Deposit):** primeiro depósito da vida do usuário, com status COMPLETED.
- **CV cohort:** % dos cadastros de um dia que já fizeram FTD (independe de quando o FTD aconteceu).
- **CV evento:** FTDs que ocorreram no dia / cadastros desse dia (fluxo intraday).
- **Welcome Bonus:** Play4Tune tem bônus 100% no 1º dep, rollover 75x, min dep 200 PKR, max saque 3x.
- **Rollover:** apostas exigidas antes de sacar o bônus. 75x é agressivo.
- **Bonus farming:** criação em massa de contas para capturar o valor do welcome bonus.
- **temp mail / disposable email:** email descartável (wetuns.com, whyknapp.com etc) usado por farmers.
