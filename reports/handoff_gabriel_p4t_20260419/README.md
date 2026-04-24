# Play4Tune — Possível bonus farming organizado (handoff Gabriel/CTO)

**De:** Mateus (Analytics) | **Para:** Gabriel Barbosa (CTO)
**Data:** 19/04/2026 10:40 BRT
**Banco:** `supernova_bet` (PostgreSQL — Play4Tune Paquistão)

---

## 1. Contexto rápido

Castrin pediu análise da CV de FTD da Play4Tune ontem (18/04) — pulou de ~22% pra 40%. Investiguei e achei evidências de bonus farming organizado. Hoje (19/04 madrugada) o farmer voltou na janela que eu previ e completou a sequência de phones.

**Não é fraude de pagamento** (depósitos legítimos, gateway aprovou). É exploração sistemática do Welcome Bonus 100%.

## 2. Resumo do CSV anexo (`contas_suspeitas_play4tune.csv`)

28 contas suspeitas identificadas:
- 28 fizeram FTD
- 9 já sacaram
- Depósitos totais: **Rs 6,400**
- Saques totais (já saídos): **Rs 5,130**

Critério de inclusão: phones no prefixo `+923413741970-1997` (sequencial, 27 contas) + 1 conta com mesmo padrão de email descartável fora do prefixo (`utl2FFfrQR7Qj6qi`).

### Colunas do CSV (separador `;`)

| Coluna | O que é |
|---|---|
| `id`, `username`, `phone`, `email` | Identidade da conta |
| `cadastro_brt` | Data/hora do cadastro em BRT |
| `fez_ftd` | SIM/NAO — se a conta fez 1º depósito completo |
| `dep_pkr` / `saq_pkr` | Valores em PKR (depósito completo / saque completo) |
| `turnover_pkr` / `giros` | Quanto apostou e quantas rodadas |
| `jazzcash_destino` / `jazzcash_titular` / `bank_code` | Conta de destino do saque (Jazzcash) |
| `bonus_status` / `bonus_pkr` | Status do Welcome Bonus (ACTIVE / CANCELLED / COMPLETED) |
| `rollover_target` / `rollover_progress` / `rollover_pct` | Quanto deveria apostar pra liberar o bônus / quanto apostou / % cumprido |
| `ip_first_session` | IP da primeira `user_sessions.metadata.ip` (mas ver hipótese 2 abaixo!) |
| `minutos_reg_ate_ftd` | Tempo entre cadastro e 1º depósito |
| `contas_pareadas_session` | Outras contas que compartilharam o mesmo `bets.session_id` |
| `flags` | Tags consolidadas: `phone_seq_batch`, `email_temp_mail`, `ftd_sub_2min`, `jazzcash_farmer_known`, `rollover_baixo_<20%`, `saque_>_dep`, `session_compartilhada` |

## 3. As 3 evidências fortes

**a) Conta Jazzcash sequencial pra saque:**
Os saques saem para 8 contas Jazzcash distintas, mas **5 estão no mesmo prefixo `+92304720850-563`**, e **4 estão no nome "Adeel"** (provável KYC fraco — mesma pessoa abrindo várias Jazzcash).

**b) `bets.session_id` duplicado entre contas:**
4 sessões de jogo aparecem em 2 contas distintas do batch cada (ex: `2031-0-394801-1000795` em `hYpZ9HdypO3pLtcD` + `JC5uOMwD9xuffKpr`). **Aqui preciso da tua confirmação:** esse `session_id` é gerado pelo nosso back-end, ou pelo provider 2J Games? Se é nosso, é prova nuclear. Se é deles, é só correlação de player session deles.

**c) Padrão temporal e técnico:**
- Phones sequenciais `+923413741970-1997`
- Tempo médio reg→FTD: 1,4min (mediana), 68% sub-2min
- Cadastros em janelas comerciais PK (10-13h e 19-22h horário PK)
- Emails em domínios temp-mail (`wetuns.com`, `whyknapp.com`) — zero histórico desses domínios em 16 dias antes
- Zero ação de admin (`admin_audit_logs`) sobre as contas → **não é teste interno**
- Zero match na lógica oficial de test users (role + ADJUSTMENT + reviewed_by) → **escapam do filtro padrão**

## 4. As 3 hipóteses que eu não consegui validar (preciso da tua opinião)

### Hipótese 1 — Falha de enforcement do rollover

Em toda a plataforma, **121 de 122 saques de contas com bônus ativado foram feitos com `bonus_activations.rollover_progress < rollover_target`** (após filtro test = 83 de 84 reais).

A regra 75× está cadastrada em `bonus_programs.rollover_multiplier=75` (active=true), mas o cashout não bloqueia. Comportamento observado:

```
17:01  DEPOSIT     Rs 200      → saldo REAL=200, BONUS=0
17:01  BONUS_CREDIT Rs 200     → saldo REAL=200, BONUS=200 (carteira separada)
17:03  Bets de Rs 5 (×11)      → saldo REAL=576 (teve sorte num hit)
17:06  WITHDRAW FAILED         (sistema rejeitou com bônus ACTIVE)
17:06  BONUS_DEBIT Rs 200      (sistema CANCELOU o bônus)
17:08  WITHDRAW COMPLETED Rs 576 ✅
```

**Pergunta técnica:** isso é decisão de produto (non-sticky bonus, padrão de mercado) ou gap de gate no endpoint WITHDRAW? Não consegui inferir só lendo o schema.

### Hipótese 2 — IP em `user_sessions.metadata.ip` não serve para fingerprint

Universo todo de jogadores reais tem IP AWS US (744 users em `3.x.x.x`, 628 em `44.x.x.x`, 519 em `54.x.x.x`, etc.). Devemos estar atrás de CloudFront/CDN e o `metadata.ip` que persistimos é IP do edge AWS, não do cliente real.

Confirmação: rodei a tua plataforma toda e essas faixas AWS aparecem em massa, não só no batch. **IP por si só não é evidência de fraude.**

**Pedido:** dá pra capturar `X-Forwarded-For` (ou `True-Client-IP` se for CloudFront) e persistir em `user_sessions.metadata.real_ip`? Hoje sem isso, qualquer regra de bloqueio por IP é cega.

Bônus: `transactions.user_agent` está **100% NULL** em 32.083 tx das últimas 24h. Vale capturar também.

### Hipótese 3 — 27 jogos com RTP > 100% no catálogo

Achei rodando a análise: `casino_games` tem 27 jogos ativos com `rtp > 100`, e o RTP realizado em `casino_user_game_metrics` bate com o catalogado. Casa tem **prejuízo histórico de Rs 49.763 (R$ 890)** nesses jogos. Top:
- `CAR ROULETTE` rtp=500%
- `THREE DICE` rtp=200%
- `BACCARAT` rtp=180,61%
- `CRASH II` rtp=122,80% — **Rs -35.618 prejuízo** (35 jogadores ativos)
- `PIGGY BANKIN` rtp=100,33%

**Pergunta:** provider 2J Games está enviando `rtp` correto (com algum significado que eu não conheço — multiplier vs return-to-player) ou tem erro de configuração? Se for erro, todo esse catálogo está sangrando.

## 5. Sugestões (sem pressão — bola tua decidir)

1. **Investigar gate do `WITHDRAW`:** o endpoint deveria checar `bonus_activations.status='COMPLETED'` ou `rollover_progress >= rollover_target` antes de aprovar. Provavelmente esse gate não existe.
2. **Captura de fingerprint real:** `X-Forwarded-For` em `user_sessions.metadata.real_ip` + `user_agent` em `transactions.user_agent`.
3. **Auditar com 2J Games** os 27 jogos com RTP > 100%.
4. **Política de bônus:** 1 ativação por phone/device em vez de por conta.
5. **Bloqueio preventivo Compliance** (já alinhei com Castrin):
   - Phones cadastro: `+923413741970-1997` (faixa do batch)
   - Jazzcash destino: `+92304720850-563`
   - Domínios email: `wetuns.com`, `whyknapp.com`

## 6. Como reproduzir tudo

Scripts em `c:/Users/NITRO/OneDrive - PGX/Projetos - Super Nova/MultiBet/scripts/` (todos rodam via `db/supernova_bet.py`):

| Script | O que faz |
|---|---|
| `analise_ftd_conversion_play4tune.py` | Análise inicial CV FTD + UTMs |
| `entender_saque_sem_rollover.py` | Fluxo transacional da conta `sZ8M2Jn3BBryYd31` |
| `revalidar_rollover_sem_teste.py` | Aplica filtro test e recalcula 83/84 |
| `fingerprint_smoking_gun.py` | Confirma Jazzcash sequencial + session_id |
| `monitor_live_ftd_p4t.py` | Monitor live (rodar a qualquer hora) |

Report consolidado: `reports/analise_ftd_play4tune_18abr2026.md`

---

Fico à disposição pra reunião de 15-30min se quiser discutir. Não tenho certeza das 3 hipóteses acima — daí o pedido pra você validar com a tua experiência.

Mateus
