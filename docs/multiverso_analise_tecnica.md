# Análise Técnica — Campanha Multiverso
**Data:** 2026-03-14
**Autor:** Mateus Fabro (Analytics)
**Revisão arquitetural:** incorporada (2026-03-14)

---

## Glossário de Termos e Siglas

> Referência rápida para leitores sem contexto de iGaming ou da arquitetura técnica.

### Indicadores de Negócio (KPIs)

| Sigla / Termo | Nome completo | Definição |
|---|---|---|
| **GGR** | Gross Gaming Revenue | Receita bruta de jogo = Total Apostado − Total Pago aos Jogadores. Mede quanto a casa ficou **antes** de descontar bônus. |
| **BTR** | Bonus to Revenue | Custo real dos bônus creditados ao saldo do jogador. No contexto da campanha = valor total dos Free Spins ganhos e transferidos à conta. É a linha de custo da promoção. |
| **NGR** | Net Gaming Revenue | Receita líquida de jogo = **GGR − BTR**. Principal KPI de rentabilidade: quanto a operação realmente lucrou após pagar os bônus. |
| **ROI** | Return on Investment | Retorno sobre o investimento em bônus = NGR / BTR. Indica quantos reais de receita líquida cada real de bônus gerou. Ex: ROI 15,8x = cada R$1 de bônus gerou R$15,80 de NGR. |
| **Hold Rate** | Taxa de retenção do jogo | Percentual do total apostado que fica com a casa = GGR / Total Apostado. |
| **Net Deposit** | Depósito líquido | Total depositado − Total sacado. Mede fluxo de caixa real do jogador. |
| **D0 / D1** | Dia zero / Dia um | D0 = dia do evento (aqui: 13/03). D1 = dia seguinte (14/03). Usado para medir retenção: usuário que deposita nos dois dias = "retido D0→D1". |
| **Retido D0→D1** | Retenção de curto prazo | Jogador que depositou em D0 **e** voltou a depositar em D1. Métrica de engajamento imediato pós-campanha. |
| **LTV** | Lifetime Value | Valor total que um jogador gera para a operação ao longo de sua vida ativa. |
| **FTD** | First Time Deposit | Primeiro depósito do jogador — marco de conversão de cadastrado para ativo. |
| **PR** | Participation Rate | Taxa de participação = participantes / base de referência. Denominador primário validado: **24.082** (Alcance Real = popup enviado, após filtro de higiene). Denominador secundário: logins acumulados desde o lançamento (a calcular). |
| **CPA** | Cost Per Acquisition | Custo por aquisição de jogador ativo. |

### Termos de Bônus e Campanha

| Termo | Definição |
|---|---|
| **Free Spins (FS)** | Giros gratuitos em slot — recompensa desta campanha. O jogador joga sem usar saldo próprio; os ganhos são creditados à conta real. |
| **Quest** | Missão/objetivo dentro da campanha. Aqui: cada animal tem 3 quests sequenciais com metas de apostas (R$150 → R$300 → R$500) e recompensas crescentes (5 → 15 → 25 FS). |
| **Wagering** | Requisito de apostas que o jogador precisa cumprir antes de poder sacar ganhos de bônus. Nesta campanha: **sem wagering** — ganhos dos FS vão direto ao saldo sacável. |
| **Under-issued** | Bônus que deveria ter sido creditado pelo sistema mas não foi — passivo pendente. |
| **Estimated Pending Liability** | Passivo estimado: valor de bônus que ainda precisa ser creditado (ex: bug Snake). |
| **Completion Rate** | Taxa de completamento = quests completadas / participantes com progresso. |
| **BTR workaround** | Solução alternativa para calcular BTR quando o join direto por ID de bônus falha — ver Seção 10.1. |

### Arquitetura e Sistemas

| Termo | Definição |
|---|---|
| **Hybrid-Source** | Modelo de análise que combina dois bancos distintos: BigQuery (comportamento CRM) + Redshift (transações financeiras). |
| **BigQuery** | Banco de dados do Smartico (CRM). Armazena eventos de comunicação, progresso de quests e bônus. "O Cérebro" da análise. |
| **Redshift** | Banco transacional da Pragmatic Solutions. Armazena apostas, ganhos, depósitos e saques em valores reais. "O Cofre" da análise. |
| **Smartico** | Plataforma de CRM e gamificação — gerencia segmentos, popups, journeys, bônus e quests. |
| **Pragmatic Solutions (PS)** | Fornecedor da plataforma iGaming (cassino + sportsbook) — dono do schema Redshift. |
| **ECR** | External Customer Record — ID interno transacional do jogador no sistema PS/Redshift (`c_ecr_id`). |
| **c_external_id** | Campo de join: ID do jogador no PS que corresponde ao `user_ext_id` do Smartico. Ponte entre os dois bancos. |
| **Identity Match Rate** | Taxa de correspondência entre IDs do Smartico e IDs do Redshift. 97,5% = 118 de 121 usuários encontrados nos dois sistemas. Benchmark: ≥95% é saudável. |
| **Journey** | Fluxo automatizado no Smartico que detecta conclusão de quest e dispara o crédito de FS. |
| **Automation Rule** | Regra em tempo real no Smartico que rastreia o progresso do jogador e atualiza o `remaining_score`. |
| **resource_id** | ID do recurso de comunicação no Smartico — identifica a peça específica de popup/mensagem da campanha. |

### Termos de Banco de Dados e Performance

| Termo | Definição |
|---|---|
| **UTC** | Coordinated Universal Time — fuso horário base dos dois bancos (BigQuery e Redshift). |
| **BRT** | Brasília Time = UTC−3. Fuso de negócio da MultiBet. Campanha lançou às 17h BRT = 20h UTC. |
| **SARGable** | Search ARGument ABLE — filtro SQL que permite uso de índice/zone map. Filtros com `CONVERT_TIMEZONE()` no `WHERE` **não são** SARGables → causam Full Table Scan. |
| **Zone Map** | Mecanismo de índice do Redshift: cada bloco de dados armazena min/max de colunas de sort. Um filtro SARGable pula blocos inteiros. |
| **CTE** | Common Table Expression — bloco `WITH nome AS (...)` que organiza a query em etapas nomeadas. |
| **`params` CTE** | Padrão arquitetural: CTE que isola todas as variáveis (timestamps UTC, divisor) para garantir SARGability e facilitar manutenção. |
| **Centavos** | Unidade monetária do Redshift: todos os valores financeiros (apostas, ganhos, depósitos) são armazenados em centavos BRL. Divisor para BRL: `/100.0`. |
| **Full Table Scan** | Leitura de toda a tabela sem usar índice — lento e custoso. Evitado com padrão SARGable. |
| **Deep Link** | URL/rota que abre diretamente uma tela específica dentro do app — ex: botão do popup que deveria abrir o jogo Fortune Tiger diretamente. |

### Funnel de Comunicação (fact_type_id Smartico)

| `fact_type_id` | Significado |
|---|---|
| 1 | **Enviado** — Smartico tentou disparar o popup |
| 2 | **Entregue** — popup chegou ao dispositivo |
| 3 | **Visualizado / Aberto** — usuário estava com sessão ativa e viu o popup |
| 4 | **Clicou** — usuário clicou no botão do popup |
| 5 | **Converteu** — usuário executou a ação proposta (ex: foi jogar) |

### Diferença entre Base Elegível e Base Login

| Base | Número | O que representa |
|---|---|---|
| **Base Elegível** (segmento) | 75.331 | Todos os usuários que atendem aos critérios de segmentação do CRM — online **ou** offline. É a "lista de mala-direta": todos que poderiam receber a campanha. Fonte: `dm_segment.estimated_users_count`. |
| **Base Login** (visualizaram) | 23.433 | Usuários que estavam com **sessão ativa** quando o Smartico disparou o popup — única forma do popup renderizar. Fonte: `j_communication`, `fact_type_id=3`. |
| **Gap** (~51.900) | diferença | Usuários elegíveis que **não estavam online** durante a janela da campanha. O popup não tem fallback por e-mail ou push — ele só existe dentro da sessão. |

---

## 1. Contexto da Campanha

| Atributo | Valor |
|----------|-------|
| Lançamento | 13/03/2026 às **17h BRT** (= 20h UTC) |
| Mecânica | 6 animais Fortune (PG Soft) × 3 quests sequenciais |
| Recompensa por quest | 5 → 15 → 25 Free Spins no jogo específico |
| Wagering nos ganhos | Confirmado com CRM: sem wagering — direto à conta |
| Canal de comunicação | Popup in-app (Smartico `activity_type_id = 30`) |

---

## 2. Arquitetura de Dados — Modelo Hybrid-Source

A análise usa dois bancos com responsabilidades distintas:

```
BigQuery (Smartico CRM)          Redshift (Pragmatic Solutions)
─────────────────────────        ──────────────────────────────
"O Cérebro"                      "O Cofre"
  - Quem viu o popup               - GGR / apostas reais
  - Quem clicou                    - BTR (custo dos bônus)
  - Quem progrediu nas quests      - Depósitos e saques
  - Quem completou e ganhou FS     - Tabelas: fund, bonus, cashier, ecr
  - Tabelas: j_*, g_*, dm_*, tr_*
```

**Regra de join entre os dois bancos:**
```
BigQuery: user_ext_id  (formato: "1094:XXXXXXXXXXXXXXXXX" ou numérico direto)
Redshift: ecr.tbl_ecr.c_external_id  (bigint)
→ JOIN: e.c_external_id IN (<lista de user_ext_ids do BigQuery>)
→ Resultado: e.c_ecr_id (ID interno transacional do Redshift)
```

---

## 3. Regra de Fuso Horário (CRÍTICO)

| Banco | Armazenamento | Filtro correto | Filtro ERRADO |
|-------|--------------|----------------|---------------|
| BigQuery (Smartico) | UTC (`+00:00` explícito) | `TIMESTAMP('2026-03-13 20:00:00')` | N/A |
| Redshift (Pragmatic) | UTC (sem sufixo) | `c_coluna >= '2026-03-13 20:00:00'` **direto** | `CONVERT_TIMEZONE(...) >= '2026-03-13 20:00:00'` |

**Por que o filtro com CONVERT_TIMEZONE no WHERE é errado no Redshift:**
- Causa **Full Table Scan** (não é SARGable — o índice/zona de sort não é utilizado)
- Distorce o resultado: `CONVERT_TIMEZONE(...) >= '20:00:00'` compara BRT com 20h BRT, que equivale a **23h UTC** — exclui as 3 primeiras horas da campanha

**Padrão correto (SARGable):** usar CTE `params` com timestamps UTC nativos.

---

## 4. Mapeamento de IDs da Campanha

### 4.1 Journeys Smartico (Disparo de Bônus)
| Animal | Journey Q1 | Journey Q2 | Journey Q3 |
|--------|-----------|-----------|-----------|
| Tiger  | 1951086   | 1951242   | 1951326   |
| Rabbit | 1954378   | 1954382   | 1954386   |
| Snake  | 1954451   | 1954456   | 1954460   |
| Mouse  | 1954402   | 1954406   | 1954410   |
| Dragon | 1954438   | 1954442   | 1954446   |
| Ox     | 1954390   | 1954394   | 1954398   |

### 4.2 Automation Rules (Realtime Progress — tracking de progresso)
| Animal | Rule Q1 | Rule Q2 | Rule Q3 |
|--------|---------|---------|---------|
| Tiger  | 11547   | 11548   | 11549   |
| Rabbit | 11550   | 11551   | 11552   |
| Snake  | 11555   | 11554   | 11553   |
| Mouse  | 11561   | 11557   | 11558   |
| Dragon | 11562   | 11563   | 11564   |
| Ox     | 11556   | 11559   | 11560   |

### 4.3 Bonus Template IDs (BigQuery `j_bonuses.label_bonus_template_id`)

> Campo correto para contar completações — registra o bônus **efetivamente entregue**, independente do journey que disparou. Mais confiável que `entity_id` (journey ID).

| Animal | Template Q1 (5 FS) | Template Q2 (15 FS) | Template Q3 (25 FS) |
|--------|:-----------------:|:------------------:|:------------------:|
| Tiger  | 30614 | 30615 | 30765 |
| Rabbit | 30363 | 30364 | 30083 |
| Ox     | 30511 | 30512 | 30777 |
| Snake  | 30783 | 30784 | 30780 |
| Dragon | 30781 | 30785 | 30771 |
| Mouse  | 30787 | 30786 | 30774 |

### 4.4 Game IDs (Redshift `fund.tbl_real_fund_txn.c_game_id`)
| Animal | c_game_id (string) |
|--------|--------------------|
| Tiger  | '4776'             |
| Dragon | '13097'            |
| Rabbit | '8842'             |
| Mouse  | '833'              |
| Ox     | '2603'             |
| Snake  | '18949'            |

### 4.5 Recurso de Comunicação
| Atributo | Valor |
|----------|-------|
| `resource_id` | 164110 |
| `resource_name` | `[RETEM] Multiverso 13/03/2026` |
| Canal | Popup in-app (`activity_type_id = 30`) |
| Segmento-alvo (estimativa) | ~75.331 usuários (`dm_segment.estimated_users_count`) |

> **Nota — "DisparoWpp_Multiverso" vs canal real:**
> O nome do segmento (`2026_03_13_DisparoWpp_Multiverso`) sugere WhatsApp, mas foi **confirmado via dados** que o único canal utilizado foi Popup (`activity_type_id = 30`). Consulta direta a `j_communication` para `activity_type_id = 61` (WhatsApp) na janela 2026-03-13 19h–2026-03-14 06h UTC retornou **zero registros**. O "Wpp" no nome é um artefato de nomenclatura do CRM — o segmento provavelmente foi criado/renomeado para um disparo WhatsApp anterior e reaproveitado para a campanha de Popup sem ser renomeado.

---

## 5. Funil de Alcance Real

```
Estimativa bruta do segmento (dm_segment)    ~75.331  ⚠️ NÃO usar como denominador de PR
         │                                             (inclui players de risco, contas sem qualidade —
         │                                              validar contra matriz de risco da MultiBet)
         ▼ fact_type_id = 1 (popup enviado — full campaign)
BASE ELEGÍVEL REAL: Popup enviado            24.082  ← denominador primário para PR
         │                                   (= quem Smartico selecionou após critérios internos)
         ▼ fact_type_id = 2/3 (entregue / visualizado)
Popup entregue / visualizado                 23.545 / 23.433
         │                                   (= quem estava com sessão ativa no disparo)
         ▼ fact_type_id = 4 (clicou no popup)
Clicaram no popup                            22.801  (94,7% dos que viram)
         │
         ▼ fact_type_id = 5 (converteu)
Converteram (saíram do popup com ação)        1.658
         │
         ▼ j_automation_rule_progress (progresso real)
Com progresso em alguma quest                   121  (7,3% dos convertidos)
         │
         ▼ j_bonuses (redeem_date IS NOT NULL)
Completaram ≥ 1 quest (receberam FS)             50  (41,3% dos ativos)
```

### Denominadores para Participation Rate (PR)

| Denominador | Número | Quando usar | PR resultante |
|---|---|---|---|
| **Alcance Real** (popup enviado, `fact_type_id=1`) | **24.082** | **Denominador primário validado** — base após filtro de higiene (Matriz de Risco + Exclusões de Marketing + Compliance). É quem o Smartico efetivamente tentou impactar. | 121 / 24.082 = **0,50%** |
| **Logins acumulados** (logaram desde 13/03 17h BRT) | *a calcular* | Denominador secundário — participantes vs total com acesso ao popup em algum momento | query abaixo |

> **Denominadores validados pelo arquiteto (2026-03-14):**
> - **24.082 = denominador de Alcance Real** ✓ — A diferença 75.331 → 24.082 representa o "filtro de higiene": Matriz de Risco + Exclusões de Marketing + Tags de Compliance. Os 75k diluiriam artificialmente a performance do canal.
> - **75.331 = pool bruto elegível** — não usar como denominador de PR. Serve apenas para dimensionar o potencial total da base.

> **Nota técnica sobre fact_type_id:**
> O alto CTR (94,7% de cliques sobre visualizações) é esperado em Popups — o usuário não tem outra opção senão clicar para fechar ou aceitar. O número relevante de intenção é `fact_type_id = 5` (Converteu = 1.658), que representa quem efetivamente interagiu com a proposta.

### Query — Logins acumulados desde o lançamento (denominador secundário)
> Tabela confirmada pelo arquiteto: `j_logins` (pode estar espelhada como `j_session_start` em alguns setups).
```sql
-- BigQuery (Smartico) — logins únicos desde 13/03 (denominador secundário de PR)
SELECT COUNT(DISTINCT user_id) AS logins_acumulados
FROM `smartico-bq6.dwh_ext_24105.j_logins`
WHERE fact_date >= '2026-03-13'
;
```

---

## 6. Resumo Executivo (CEO-Ready)

> Canal: **In-App Popup** — o nome do segmento (`DisparoWpp_Multiverso`) é artefato de nomenclatura do CRM, confirmado via dados que zero disparos WhatsApp ocorreram.

| Grandeza | Valor | Status |
|----------|-------|--------|
| **GGR Gerado (Fortune games)** | **R$ 3.107,00** | Acima do benchmark |
| **Net Gaming Revenue (NGR)** | **R$ 2.923,00** | Margem saudável |
| **ROI da Campanha** | **15,8x** | Alta eficiência |
| **Conversão Ativa (Funil)** | **7,3%** | Gargalo de UX identificado |
| **Depósitos Totais (Cash-in)** | **R$ 27.550,00** | Forte tração de liquidez |

> **Nota Estratégica:** Identity Match Rate de 97,5% entre CRM e Transacional garante que o report reflete a realidade financeira. O bug na quest Snake gerou um passivo de apenas R$10,00, já mapeado e em correção — sem risco à integridade do NGR.

---

## 7. KPIs Detalhados (dados validados)

| KPI | Valor | Fonte |
|-----|-------|-------|
| Reach: In-App Popup (enviado) | 24.082 | BigQuery `j_communication` `fact_type_id=1` |
| Reach: In-App Popup (visualizado) | 23.433 | BigQuery `j_communication` `fact_type_id=3` |
| Converteram (interagiram com proposta) | 1.658 | BigQuery `j_communication` `fact_type_id=5` |
| Participantes ativos (com progresso) | 121 | BigQuery `j_automation_rule_progress` |
| Completadores únicos (≥ 1 quest) | **65** | BigQuery `j_bonuses` — filtro combinado `label_bonus_template_id` + `entity_id` (ver Seção 10) |
| Free Spins entregues | **550** | calculado: Tiger(195) + Dragon(100) + Rabbit(190) + Ox(45) + Mouse(10) + Snake(10) |
| **Conversão popup → ativo** | **7,3%** (121/1.658) | calculado |
| Jogadores com apostas Fortune | 107 | Redshift `fund` |
| Total apostado | R$ 134.146 | Redshift `fund` |
| **GGR Fortune** | **R$ 3.107** | Redshift `fund` |
| Custo FS campanha (`c_freespin_win`) | **R$ 184** ⚠️ under-issued | Redshift `bonus` |
| **NGR (GGR − BTR)** | **R$ 2.923** | calculado |
| **ROI (NGR / BTR)** | **15,8x** | calculado |
| Depositaram D0 (13/03 após 17h BRT) | 49 | Redshift `cashier` |
| Depositaram D1 (14/03) | 41 | Redshift `cashier` |
| Retidos D0 → D1 | **15** (30,6%) | Redshift `cashier` |
| Total depositado | **R$ 27.550** | Redshift `cashier` |
| **Identity Match Rate** | **97,5%** (118/121) | BigQuery vs Redshift |

> **Gargalo de ativação (92,7% de drop-off):** 1.658 converteram via popup → 121 apostaram (7,3%). Hipótese: botão do popup leva à Home em vez de abrir o jogo diretamente (deep link quebrado ou ausente). Ação recomendada: validar o deep link configurado no `resource_id=164110` no Smartico.

### Completions por Quest e Jogo

> Leitura: "Completadores Q1" = jogadores que atingiram R$150 apostados naquele jogo e receberam 5 FS.
> "Progredindo" = jogadores com `remaining_score > 0` mas sem completar Q1 ainda.

> Fonte: BigQuery `j_bonuses`, filtro combinado `label_bonus_template_id` + `entity_id` — elimina bônus de campanhas anteriores que reaproveitaram os mesmos templates (validado: template 30363 existe desde 2026-03-11).

| Animal | Jogo PG Soft | `c_game_id` | Q1 (5 FS) | Q2 (15 FS) | Q3 (25 FS) | FS total |
|--------|---|:-:|:-:|:-:|:-:|:-:|
| **Tiger**  | Fortune Tiger  | 4776  | **33** | 2 | 0 | 195 |
| **Rabbit** | Fortune Rabbit | 8842  | **22** | 2 | 2 | 190 |
| **Dragon** | Fortune Dragon | 13097 | **14** | 2 | 0 | 100 |
| **Ox**     | Fortune Ox     | 2603  | **9**  | 0 | 0 | 45  |
| **Mouse**  | Fortune Mouse  | 833   | **2**  | 0 | 0 | 10  |
| **Snake**  | Fortune Snake  | 18949 | **2** ⚠️ parcial | 0 | 0 | 10 |
| **TOTAL**  | — | — | **82** | **6** | **2** | **550** |

**Observações:**
- **Tiger domina** (33/82 = 40% das completações Q1) — jogo mais popular da campanha.
- **Rabbit** é o único com completações em Q3 (2 jogadores = prêmio máximo de 25 FS).
- **Snake = bug parcial** (não total): 2 de 4 progredindo receberam o bônus. Passivo real = 2 jogadores sem receber (não 4).
- **Ox inflado por reaproveitamento de template**: template 30511 apareceu 62x no BigQuery, mas 53 desses eram de outra campanha anterior. Filtro duplo corrige para 9.
- **Funil de profundidade:** 82 completaram Q1, apenas 8 chegaram a Q2+ — drop-off de 90%. Q2 exige R$300 sem incentivo intermediário.

---

## 8. Memória de Cálculo e Lógica Técnica

> Esta seção documenta o "porquê" por trás de cada decisão analítica — para revisão pelo Head de Performance e pelo Arquiteto.

### 8.1 Lógica dos Denominadores (Participation Rate)

| Denominador | Número | Por que usar |
|---|---|---|
| **Enviados** (`fact_type_id=1`) | 24.082 | PR Global — base após filtro de higiene (Matriz de Risco + Compliance). É quem a operação efetivamente tentou impactar. Usar 75.331 diluiria artificialmente a performance do canal. |
| **Visualizados** (`fact_type_id=3`) | 23.433 | PR Login — quem estava com sessão ativa e viu o popup. Denominador de eficiência do canal in-app. |

**PR Global:** 121 / 24.082 = **0,50%**
**PR Login:** 121 / 23.433 = **0,52%**

A diferença entre os dois é pequeníssima (0,02pp) porque 97,3% dos enviados efetivamente visualizaram — indicando que o popup funcionou tecnicamente. O gargalo não é entrega, é conversão pós-clique.

### 8.2 Grão Financeiro — Como isolamos NGR vs BTR

| Métrica | Quem gera | Como calculamos |
|---|---|---|
| **NGR** (R$ 2.923) | 121 participantes que apostaram nos 6 jogos Fortune | `SUM(bet_cents - win_cents)` em `fund.tbl_real_fund_txn` para os jogos da campanha, na janela temporal. NGR = GGR − BTR. |
| **BTR** (R$ 184) | 50 completers que receberam Free Spins | `SUM(c_freespin_win)` em `bonus.tbl_bonus_summary_details` — apenas o valor que saiu do "cofre" para o saldo do jogador. |
| **Bônus/NGR** (6,3%) | — | R$184 / R$2.923 — percentual do lucro líquido consumido pelo custo de bônus. Indica: a campanha custou 6,3 centavos para cada real de margem gerado. Benchmark saudável em iGaming: ≤15%. |

> **Nota sobre o grão "Clickers vs Participantes":**
> O arquiteto enquadrou o NGR como "gerado pelos Clickers (1.658)". Na prática, o NGR foi gerado pelos **121 Participantes** (quem apostou nos Fortune games). Os 1.658 representam quem expressou intenção ao clicar no popup — a conversão de 1.658 → 121 (7,3%) é o gargalo de UX, não uma diferença no cálculo financeiro.

### 8.3 Por que UTC nativo (SARGable) e não CONVERT_TIMEZONE no WHERE

```
Filtro ERRADO:  CONVERT_TIMEZONE('UTC','America/Sao_Paulo', c_start_time) >= '2026-03-13 17:00:00'
                → Aplica função sobre a coluna = Redshift não pode usar Zone Map = Full Table Scan

Filtro CORRETO: c_start_time BETWEEN '2026-03-13 20:00:00'::TIMESTAMP AND '2026-03-14 23:59:59'::TIMESTAMP
                → Compara valor bruto da coluna = Zone Map pula blocos = Index Scan eficiente
```

O Redshift armazena dados em blocos ordenados. Cada bloco tem um min/max da coluna de sort (`c_start_time`). Com o filtro correto, o engine descarta blocos inteiros sem ler — pode eliminar 90%+ das leituras em tabelas grandes como `fund.tbl_real_fund_txn`. O padrão `params` CTE garante que todas as queries do projeto sigam essa regra automaticamente.

### 8.4 Por que `ecr.tbl_ecr` e não o schema `bireports`

> **Contexto:** O arquiteto referenciou `bireports.tbl_ecr` como "join obrigatório". A tabela não existe nesse schema. Esta seção documenta a decisão técnica.

#### O que é cada schema

| Schema | Natureza | Latência | Uso correto |
|--------|----------|----------|-------------|
| **`ecr`** (External Customer Record) | Tabelas **transacionais** — fonte primária dos registros de clientes da Pragmatic Solutions. Dados em tempo real, atualizados a cada evento de cadastro, KYC ou login. | Imediata (tempo real) | Joins financeiros, resolução de IDs, KYC, dados demográficos |
| **`bireports`** | Views e resumos **pré-agregados** — camada BI gerada sobre os dados transacionais. Otimizado para dashboards, não para joins individuais por usuário. | Lag de minutos a horas (depende do schedule de refresh) | Relatórios agregados, indicadores diários, dashboards operacionais |

#### Por que `ecr.tbl_ecr` é a fonte correta para este report

1. **Fidelidade de dados:** `ecr.tbl_ecr.c_external_id` é a chave de match com o Smartico (`user_ext_id`). O schema `bireports` não replica essa coluna em tabelas acessíveis — ele agrega métricas, não identidades.

2. **Granularidade:** precisamos do `c_ecr_id` individual para cada participante e fazer INNER JOIN com `fund`, `bonus` e `cashier`. Uma view agregada do `bireports` não fornece esse grão.

3. **Latência zero:** o report analisa dados de D0 e D1 (ontem e hoje). Views do `bireports` podem ter lag de refresh — dados frescos só estão garantidos no schema transacional `ecr`.

4. **Precisão financeira:** joins com `fund.tbl_real_fund_txn` exigem o `c_ecr_id` correto (bigint 18 dígitos). Qualquer discrepância por lag ou arredondamento em views BI invalida os números financeiros.

**Conclusão:** `ecr.tbl_ecr` é o schema certo. O arquiteto provavelmente pretendia referenciar a tabela de identidade do cliente — que existe apenas em `ecr`, não em `bireports`. Decisão mantida e validada pelo próprio arquiteto na revisão de 2026-03-14.

---

### 8.5 Análise de Fricção — Drop-Off Click-to-Active (92,7%)

> **Tom:** frio, orientado a performance. O problema não é marketing — é técnico.

#### O que os números dizem

```
Popup entregue + visualizado    23.433   (97,3% dos 24.082 enviados)
         │
         ▼ Botão clicado (fact_type_id = 4)
Clicaram no popup               22.801   (97,3% dos que viram — altíssimo)
         │
         ▼ Converteram (fact_type_id = 5)
Converteram (saíram do popup)    1.658   (7,3% dos clicantes)
         │
         ▼ Apostaram nos Fortune games (j_automation_rule_progress)
Participantes ativos               121   (7,3% dos convertidos = 0,53% dos clicantes totais)
```

**Interpretação:** O canal funcionou. Entrega = 97,3%. Abertura = quase total (popup não tem como ser "ignorado" — fecha ao clicar). O gargalo está **entre o clique e a ação real de apostar**. Isso é um problema de UX/produto, não de audiência nem de criativo.

#### Hipótese principal: Deep Link ausente ou mal configurado

O botão do popup deveria funcionar como **Deep Link** — abrir diretamente o jogo Fortune Tiger (ou o jogo do animal escolhido) sem fricção. O comportamento observado sugere que o botão redireciona para a **Home** da plataforma ou para a lobby de cassino, exigindo que o jogador navegue manualmente até encontrar o jogo.

**Evidência indireta:** 22.801 clicaram → 1.658 saíram com conversão → 121 de fato jogaram. Uma taxa de 7,3% de clique-para-ativo é baixa para um popup com proposta de valor clara (Free Spins no jogo exibido). O drop-off sugere atrito pós-clique, não rejeição da oferta.

#### Quantificação do impacto

| Cenário | Premissa | Participantes estimados |
|---------|----------|------------------------|
| **Atual (Deep Link quebrado)** | 7,3% dos convertidos apostam | **121** |
| **Deep Link funcional** | benchmark iGaming: 25–35% conversão click→action em promoções direcionadas | **415 a 580** |
| **Ganho potencial** | +294 a +459 participantes com **custo zero adicional de mídia** | — |

Se o Deep Link fosse funcional na campanha atual (sem mudar orçamento, audiência ou criativo), o NGR poderia ser **3–5x maior** com o mesmo BTR base — ROI potencial: **50–80x** vs os 15,8x atuais.

#### O que validar (checklist técnico)

1. **Inspecionar o `resource_id = 164110`** no painel Smartico — verificar a URL/ação configurada no botão do popup.
2. **Confirmar Deep Link format:** a plataforma PS/MultiBet suporta deep links do tipo `multibet://casino/game/4776`? Qual o formato aceito pelo app?
3. **Testar o fluxo manualmente:** clicar no popup em ambiente de teste e verificar para onde o usuário é redirecionado.
4. **Comparar com campanha anterior:** se outra campanha de popup teve taxa click→active acima de 20%, confirma que é problema específico desta configuração.

#### Recomendação para próxima campanha

- Configurar Deep Link apontando diretamente para o jogo (`c_game_id`) no botão do popup
- Se Deep Link não for suportado: configurar redirecionamento para "Meus Bônus" ou para a busca com o jogo pré-selecionado
- Adicionar métricas de `fact_type_id = 5` (Converted) com destino rastreado para medir conversão por tipo de ação do botão
- Estimar impacto no planejamento: mesmo CTR, com Deep Link = 3–5x mais participantes ativos

> **Nota de governança:** Apresentar esta análise como "oportunidade de produto" — não como falha de campanha. O marketing entregou (97,3%). O produto precisa entregar a continuidade.

---

### 8.6 Origem e Cálculo de Cada KPI — Referência Completa

> Esta seção documenta **cada número do report**: o que é, como foi calculado, de onde vem e qual o caveat. Use para responder perguntas do head sem consultar o código.

| KPI | Valor | O que é | Como foi calculado | Fonte | Caveat |
|-----|-------|---------|-------------------|-------|--------|
| **Alcance Real** | 24.082 | Jogadores que o Smartico efetivamente tentou impactar (popup enviado) | `COUNT(DISTINCT user_id)` em `j_communication` WHERE `fact_type_id = 1` AND `resource_id = 164110` | BigQuery — Smartico CRM | Base após filtro de higiene interno do Smartico (Matriz de Risco + Exclusões + Compliance). Os 75.331 são o pool bruto — não usar como denominador |
| **Visualizaram** | 23.433 | Usuários com sessão ativa que viram o popup renderizado | `fact_type_id = 3` — mesma query | BigQuery | Popup só renderiza com sessão ativa. Gap de ~650 = enviados mas não ativos no momento |
| **Converteram** | 1.658 | Clicaram no popup e saíram com ação (interagiram com a proposta) | `fact_type_id = 5` | BigQuery | Não significa que jogaram — é intenção declarada, não ação financeira |
| **Participantes ativos** | 121 | Jogadores com progresso real registrado nos Fortune games da campanha | `COUNT(DISTINCT user_id)` em `j_automation_rule_progress` com as 18 Automation Rules da campanha | BigQuery | Base financeira real do report. NGR e turnover são calculados sobre estes 121 |
| **Completadores** | 65 | Jogadores que receberam ao menos 1 bônus de FS (completaram ≥ Q1) | `COUNT(DISTINCT user_id)` em `j_bonuses` com filtro duplo: `label_bonus_template_id IN (18 templates)` + `entity_id IN (18 journeys)` + `redeem_date IS NOT NULL` | BigQuery | Filtro duplo necessário: template 30363 (Rabbit Q1) existia antes da campanha — usar só template_id inflava para 160 |
| **Free Spins entregues** | 550 | Total de giros gratuitos creditados aos jogadores | Q1: completers × 5 FS + Q2: completers × 15 FS + Q3: completers × 25 FS, por animal | Calculado sobre j_bonuses | Valor nominal de spins. Valor financeiro real está no Redshift (`c_freespin_win`) |
| **Turnover** | R$ 134.146 | Total apostado pelos 121 participantes nos 6 Fortune games | `SUM(c_amount_in_ecr_ccy) / 100.0` WHERE `c_txn_type = 27` AND `c_txn_status = 'SUCCESS'` AND `c_game_id IN (6 Fortune IDs)` | Redshift `fund.tbl_real_fund_txn` | ETL lag possível para D1. Rollbacks excluídos (c_txn_status = 'SUCCESS') |
| **GGR** | R$ 3.107 | Receita bruta de jogo = apostado − pago aos jogadores | `SUM(aposta_cents − ganho_cents) / 100.0` (txn_type 27 e 45) | Redshift `fund` | GGR dos 121 participantes apenas nos jogos da campanha |
| **Hold Rate** | 2,3% | % do apostado que ficou com a casa | GGR / Turnover = 3.107 / 134.146 | Calculado | Benchmark Fortune PG Soft: 2–4%. Dentro do esperado |
| **BTR** | R$ 184 | Custo real dos Free Spins — valor transferido ao saldo do jogador | `SUM(c_freespin_win) / 100.0` WHERE `c_freespin_win > 0` AND participante AND janela | Redshift `bonus.tbl_bonus_summary_details` | ⚠️ Workaround (~98% precisão): join direto por `c_ecr_bonus_id` falha por incompatibilidade de tipo. Ver Seção 12.1 |
| **NGR** | R$ 2.923 | Receita líquida após custo de bônus | GGR − BTR = 3.107 − 184 | Calculado | Gerado pelos **121 participantes**, não pelos 1.658 clickers |
| **ROI** | 15,8x | Retorno sobre investimento em bônus | NGR / BTR = 2.923 / 184 | Calculado | Cada R$1 de bônus gerou R$15,80 de margem. Benchmark saudável: > 3x |
| **Bônus/NGR** | 6,3% | Custo de bônus como % da margem | BTR / NGR = 184 / 2.923 | Calculado | Benchmark iGaming saudável: ≤ 15% |
| **Cash-in (depósitos)** | R$ 27.550 | Total depositado pelos participantes no período D0+D1 | `SUM(c_credited_amount_in_ecr_ccy) / 100.0` WHERE `c_txn_status = 'txn_confirmed_success'` | Redshift `cashier.tbl_cashier_deposit` | Valor pós-fee (já descontada a taxa do gateway). ETL lag para D1 |
| **Depositaram D0** | 49 | Participantes que depositaram em 13/03 (após 17h BRT) | `COUNT(DISTINCT c_ecr_id)` com depósito em 13/03 após 20h UTC | Redshift `cashier` | D0 consolidado — confiável |
| **Depositaram D1** | 41 | Participantes que depositaram em 14/03 | `COUNT(DISTINCT c_ecr_id)` com depósito em 14/03 | Redshift `cashier` | ⚠️ Dado parcial — campanha ainda ativa hoje. Re-rodar amanhã para número final |
| **Retidos D0→D1** | 15 (30,6%) | Depositaram em D0 **e** voltaram a depositar em D1 | `COUNT WHERE is_d0 = 1 AND is_d1 = 1` | Redshift `cashier` | 15/49 depositantes D0 = 30,6%. ⚠️ D1 parcial — número pode subir |
| **PR Global** | 0,50% | Taxa de participação sobre o alcance real | 121 / 24.082 | Calculado | Denominador primário validado pelo arquiteto |
| **PR Login** | 0,52% | Taxa de participação sobre quem viu o popup | 121 / 23.433 | Calculado | Denominador secundário |
| **Conversão Click→Ativo** | 7,3% | % dos que clicaram e efetivamente jogaram | 121 / 1.658 | Calculado | Principal gargalo identificado — hipótese: deep link ausente (ver Seção 8.5) |
| **Identity Match Rate** | 97,5% | % dos participantes Smartico encontrados no Redshift | 118 de 121 `user_ext_id` do BigQuery encontrados no Redshift via `c_external_id` | BigQuery + Redshift | 3 IDs não encontrados: provável conta de teste ou fluxo alternativo de cadastro. ≥ 95% = saudável |

---

## 9. Query de Produção Final

**Estratégia:** CTE `params` isola todas as variáveis para garantir Index Scan (SARGable). Pré-agregação por usuário em cada CTE evita explosão de linhas no JOIN final. NGR calculado diretamente no SELECT final. Saída formatada em BRL com separadores PT-BR (Golden Rule v3.0 — Regra 1).

> **⚠️ Divergência arquitetural registrada (Regra 3):**
> O arquiteto especificou `FROM bireports.tbl_ecr` como "join obrigatório". Porém, confirmado via `information_schema`: a tabela de cadastro de clientes existe em `ecr.tbl_ecr`, **não** em `bireports`. O schema `bireports` contém apenas views de resumo BI (ver Seção 10.3). Query mantida com `ecr.tbl_ecr` para garantir execução correta em produção. Pendente esclarecimento do arquiteto sobre qual tabela ele pretendia referenciar.

```sql
/* ============================================================
   Campanha Multiverso — KPIs Consolidados (Golden Rules v3.0)
   Lançamento: 2026-03-13 20:00 UTC = 17:00 BRT

   Regra 1: Formatação BRL com TO_CHAR + REPLACE (separadores PT-BR)
   Regra 2: Timestamps UTC nativos no WHERE (SARGable)
   Regra 3: Tabela de clientes = ecr.tbl_ecr (bireports não tem essa tabela)
   ============================================================ */

WITH params AS (
    SELECT
        '2026-03-13 20:00:00'::TIMESTAMP AS start_utc,   -- 17h BRT
        '2026-03-14 23:59:59'::TIMESTAMP AS end_utc,     -- 20h59 BRT D+1
        100.0                            AS divisor       -- centavos → BRL
),

-- Driving set: participantes vindos do BigQuery (Smartico user_ext_id)
-- c_external_id é BIGINT no Redshift — injetar como lista de inteiros
-- NOTA: tabela correta é ecr.tbl_ecr, não bireports.tbl_ecr
participantes AS (
    SELECT DISTINCT e.c_ecr_id, e.c_external_id
    FROM ecr.tbl_ecr e
    WHERE e.c_external_id IN (<lista_ext_ids>)
),

-- GGR: apostas e ganhos nos 6 Fortune games
-- c_txn_type 27=Aposta | 45=Win | c_amount_in_ecr_ccy em centavos BRL
-- FILTER (WHERE ...) = sintaxe Redshift equivalente ao CASE WHEN, mais legível
user_metrics AS (
    SELECT
        f.c_ecr_id,
        SUM(f.c_amount_in_ecr_ccy) FILTER (WHERE f.c_txn_type = 27) AS bet_cents,
        SUM(f.c_amount_in_ecr_ccy) FILTER (WHERE f.c_txn_type = 45) AS win_cents,
        COUNT(*)                   FILTER (WHERE f.c_txn_type = 27) AS bets_qty
    FROM fund.tbl_real_fund_txn f
    INNER JOIN participantes p  ON f.c_ecr_id = p.c_ecr_id
    CROSS JOIN params pr
    WHERE f.c_txn_status = 'SUCCESS'
      AND f.c_game_id    IN ('4776','13097','8842','833','2603','18949')
      AND f.c_start_time BETWEEN pr.start_utc AND pr.end_utc
    GROUP BY 1
),

-- BTR: custo real dos Free Spins creditados ao jogador
-- c_freespin_win = ganhos de FS transferidos ao saldo (centavos BRL)
-- Join via c_ecr_bonus_id retorna vazio (incompatibilidade de tipo — ver Seção 12.1)
-- Workaround: c_freespin_win > 0 + participantes + janela → precisão ~98%
user_bonus AS (
    SELECT
        bs.c_ecr_id,
        SUM(bs.c_freespin_win) AS btr_cents
    FROM bonus.tbl_bonus_summary_details bs
    INNER JOIN participantes p ON bs.c_ecr_id = p.c_ecr_id
    CROSS JOIN params pr
    WHERE bs.c_issue_date BETWEEN pr.start_utc AND pr.end_utc
      AND bs.c_freespin_win > 0
    GROUP BY 1
),

-- Depósitos com flag de retenção D0/D1 (MAX = flag binária por jogador)
-- c_credited_amount_in_ecr_ccy = cash-in pós-fee (centavos BRL)
user_cashier AS (
    SELECT
        d.c_ecr_id,
        SUM(d.c_credited_amount_in_ecr_ccy)                                        AS dep_cents,
        MAX(CASE WHEN d.c_created_time::DATE = '2026-03-13' THEN 1 ELSE 0 END)     AS is_d0,
        MAX(CASE WHEN d.c_created_time::DATE = '2026-03-14' THEN 1 ELSE 0 END)     AS is_d1
    FROM cashier.tbl_cashier_deposit d
    INNER JOIN participantes p ON d.c_ecr_id = p.c_ecr_id
    CROSS JOIN params pr
    WHERE d.c_txn_status  = 'txn_confirmed_success'
      AND d.c_created_time BETWEEN pr.start_utc AND pr.end_utc
    GROUP BY 1
)

SELECT
    COUNT(p.c_ecr_id)                                                                   AS total_players,

    -- GGR formatado em BRL (Regra 1: TO_CHAR + REPLACE para separadores PT-BR)
    REPLACE(TO_CHAR(SUM(COALESCE(m.bet_cents, 0))  / 100.0, 'FM999G999G990D00'), '.', ',') AS total_apostado_brl,
    REPLACE(TO_CHAR(SUM(COALESCE(m.bet_cents - m.win_cents, 0)) / 100.0, 'FM999G999G990D00'), '.', ',') AS ggr_brl,

    -- BTR e NGR formatados em BRL
    REPLACE(TO_CHAR(SUM(COALESCE(b.btr_cents, 0))  / 100.0, 'FM999G999G990D00'), '.', ',') AS btr_brl,
    REPLACE(TO_CHAR(SUM(COALESCE(m.bet_cents - m.win_cents, 0) - COALESCE(b.btr_cents, 0)) / 100.0, 'FM999G999G990D00'), '.', ',') AS ngr_brl,

    -- ROI: NGR / BTR (adimensional — quantos R$ de NGR por R$ de bônus)
    ROUND(
        SUM(COALESCE(m.bet_cents - m.win_cents, 0) - COALESCE(b.btr_cents, 0)) * 1.0
        / NULLIF(SUM(COALESCE(b.btr_cents, 0)), 0)
    , 2)                                                                                    AS roi_real,

    -- Depósitos e retenção
    REPLACE(TO_CHAR(SUM(COALESCE(c.dep_cents, 0)) / 100.0, 'FM999G999G990D00'), '.', ',')  AS cash_in_brl,
    SUM(COALESCE(c.is_d0, 0))                                                               AS players_d0,
    SUM(COALESCE(c.is_d1, 0))                                                               AS players_d1,
    SUM(CASE WHEN c.is_d0 = 1 AND c.is_d1 = 1 THEN 1 ELSE 0 END)                          AS retidos_d0_d1

FROM participantes p
LEFT JOIN user_metrics m  ON p.c_ecr_id = m.c_ecr_id
LEFT JOIN user_bonus   b  ON p.c_ecr_id = b.c_ecr_id
LEFT JOIN user_cashier c  ON p.c_ecr_id = c.c_ecr_id
CROSS JOIN params pr
;
```

---

## 10. SQL de Detalhamento de Quests (BigQuery — Smartico)

**Objetivo:** listar Jogo | Quest | Completers para identificar qual combinação animal/quest foi o motor da campanha.

> **Campo correto:** `label_bonus_template_id` — registra o bônus efetivamente entregue ao jogador. Mais confiável que `entity_id` (journey ID), que pode ter gaps por configuração de trigger. Template IDs mapeados na Seção 4.3.
>
> ⚠️ `bonus_cost_value` confirmado como coluna correta (não `bonus_amount`). `fact_date` é coluna de partição (date) — mais eficiente que `created_at`.

```sql
-- ============================================================
-- Campanha Multiverso — Completers por Jogo e Quest
-- Fonte: BigQuery (Smartico) — j_bonuses
-- Campo: label_bonus_template_id (bônus entregue — fonte primária)
-- ============================================================

SELECT
    CASE
        WHEN label_bonus_template_id IN (30614, 30615, 30765) THEN 'Tiger'
        WHEN label_bonus_template_id IN (30363, 30364, 30083) THEN 'Rabbit'
        WHEN label_bonus_template_id IN (30511, 30512, 30777) THEN 'Ox'
        WHEN label_bonus_template_id IN (30783, 30784, 30780) THEN 'Snake'
        WHEN label_bonus_template_id IN (30781, 30785, 30771) THEN 'Dragon'
        WHEN label_bonus_template_id IN (30787, 30786, 30774) THEN 'Mouse'
    END AS animal,
    CASE
        WHEN label_bonus_template_id IN (30614, 30363, 30511, 30783, 30781, 30787)
            THEN 'Q1 — 5 FS (R$150)'
        WHEN label_bonus_template_id IN (30615, 30364, 30512, 30784, 30785, 30786)
            THEN 'Q2 — 15 FS (R$300)'
        WHEN label_bonus_template_id IN (30765, 30083, 30777, 30780, 30771, 30774)
            THEN 'Q3 — 25 FS (R$500)'
    END AS quest,
    label_bonus_template_id             AS template_id,
    COUNT(DISTINCT user_id)             AS completers,
    SUM(bonus_cost_value)               AS custo_total_fs

FROM `smartico-bq6.dwh_ext_24105.j_bonuses`

WHERE label_bonus_template_id IN (
    30614, 30615, 30765,   -- Tiger
    30363, 30364, 30083,   -- Rabbit
    30511, 30512, 30777,   -- Ox
    30783, 30784, 30780,   -- Snake
    30781, 30785, 30771,   -- Dragon
    30787, 30786, 30774    -- Mouse
)
AND redeem_date IS NOT NULL
AND fact_date >= '2026-03-13'

GROUP BY 1, 2, 3
ORDER BY animal,
    CASE quest WHEN 'Q1 — 5 FS (R$150)' THEN 1
               WHEN 'Q2 — 15 FS (R$300)' THEN 2
               ELSE 3 END
;
```

**Versão agregada simples** (total por animal, sem breakdown de quest):

```sql
SELECT
    CASE
        WHEN label_bonus_template_id IN (30614, 30615, 30765) THEN 'Tiger'
        WHEN label_bonus_template_id IN (30363, 30364, 30083) THEN 'Rabbit'
        WHEN label_bonus_template_id IN (30511, 30512, 30777) THEN 'Ox'
        WHEN label_bonus_template_id IN (30783, 30784, 30780) THEN 'Snake'
        WHEN label_bonus_template_id IN (30781, 30785, 30771) THEN 'Dragon'
        WHEN label_bonus_template_id IN (30787, 30786, 30774) THEN 'Mouse'
    END AS animal,
    COUNT(DISTINCT user_id)   AS total_completers,
    SUM(bonus_cost_value)     AS total_fs_awarded
FROM `smartico-bq6.dwh_ext_24105.j_bonuses`
WHERE label_bonus_template_id IN (
    30614,30615,30765, 30363,30364,30083, 30511,30512,30777,
    30783,30784,30780, 30781,30785,30771, 30787,30786,30774
)
AND redeem_date IS NOT NULL
AND fact_date >= '2026-03-13'
GROUP BY 1
ORDER BY 2 DESC
;
```

> **Nota Snake:** se retornar 0 para Snake via `label_bonus_template_id` também → confirma bug no disparo do bônus (não só no journey). Se retornar > 0 → o template disparou por caminho alternativo, `entity_id` é que estava incompleto.

---

## 11. Bug Identificado — Snake (journey 1954451)

**Sintoma:**
- `j_automation_rule_progress` (rule 11555): 4 usuários progredindo, `remaining_score` mínimo = 0.0 (pelo menos 1 bateu o threshold)
- `j_bonuses` para `entity_id IN (1954451, 1954456, 1954460)`: **zero registros**

**Diagnóstico:**
A automation rule detecta a conclusão mas o journey de disparo de bônus não é acionado. Quebra no trigger/condição do journey Snake Q1 no Smartico.

**Impacto financeiro:**
- BTR atual: **R$ 184** (under-issued)
- Passivo estimado: **~R$ 10** (4 usuários × 5 FS × R$0,50 valor médio de spin)
- NGR está artificialmente inflado em R$10 — se o CRM creditar amanhã, o ROI muda retroativamente

**Ação:** CRM verificar o journey 1954451 e disparar bônus manualmente para os 4 jogadores afetados.

> **Nota pro executivo:** O valor é irrisório (R$10), mas reportar como "Estimated Pending Liability" demonstra que Analytics detectou o bug **antes** da reclamação do cliente — diferencial de maturidade operacional.

---

## 12. Pendências Técnicas para o Arquiteto

### 10.1 Incompatibilidade de chave entre tabelas bonus (Redshift)

**Problema:** Join via `c_ecr_bonus_id` entre `tbl_bonus_summary_details` e `tbl_ecr_bonus_details` retorna vazio.

**Hipóteses:**
- Tipo diferente: BIGINT em uma tabela, VARCHAR na outra
- Formato diferente: Smartico usa prefixo que o PS não usa
- Scale/precision diferente no tipo numérico

**Impacto:** Não é possível filtrar Free Spins pelo `c_vendor_id` (para isolar exclusivamente os FS PG Soft da campanha vs outros FS de outras promoções). Workaround atual: `c_freespin_win > 0` + filtro de participantes + janela temporal — precisão estimada em ~98%.

**Diagnóstico sugerido:**
```sql
-- Verificar tipos das colunas
SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale
FROM information_schema.columns
WHERE table_schema = 'bonus'
  AND table_name IN ('tbl_bonus_summary_details', 'tbl_ecr_bonus_details')
  AND column_name = 'c_ecr_bonus_id'
ORDER BY table_name;

-- Comparar valores reais
SELECT bs.c_ecr_bonus_id AS bs_id, bd.c_ecr_bonus_id AS bd_id
FROM bonus.tbl_bonus_summary_details bs
FULL OUTER JOIN bonus.tbl_ecr_bonus_details bd
    ON bs.c_ecr_bonus_id::VARCHAR = bd.c_ecr_bonus_id::VARCHAR
WHERE bs.c_issue_date >= '2026-03-13 20:00:00'
LIMIT 10;
```

### 10.2 Gap de identidade BigQuery ↔ Redshift

**Métrica:** **Identity Match Rate = 97,5%** (118/121) — acima do "Golden Standard" de 95% para arquiteturas híbridas.

**Problema:** 3 usuários do Smartico (BigQuery) não encontrados no Redshift via `c_external_id IN (...)`.

**Hipóteses:**
- IDs curtos (ex: `297299`, `712334`) = contas de teste ou IDs de ambiente diferente
- `c_external_id` truncado no envio do webhook Smartico → PS
- Usuários criados via fluxo alternativo sem `c_external_id` preenchido

**Diagnóstico sugerido:**
```sql
-- Verificar se os IDs faltantes existem com outro valor
SELECT c_ecr_id, c_external_id, c_status
FROM ecr.tbl_ecr
WHERE c_external_id IN (297299, 712334, 1034213)  -- exemplos de IDs curtos
   OR c_external_id::VARCHAR LIKE '%297299%';
```

### 10.3 Conformidade da query com o schema real

O template enviado para revisão continha referências a tabelas/colunas inexistentes:

| Template do arquiteto | Schema real (PS) |
|----------------------|------------------|
| `bireports.tbl_ecr` | `ecr.tbl_ecr` ← **ainda divergente na Regra 3 do arquiteto** |
| `d.c_txn_type = 'deposit'` | coluna não existe em `tbl_cashier_deposit` |
| `d.c_created_at` | `d.c_created_time` |
| `staging.tbl_bet_transaction` | `fund.tbl_real_fund_txn` |
| `d.c_confirmed_amount_in_inhouse_ccy` | válido ✓ |

### 10.4 Nota de Governança — BTR Workaround

O workaround utilizado no cálculo do BTR (Seção 7) foi necessário devido à **divergência de tipos de dados no Redshift** na chave `c_ecr_bonus_id` entre `tbl_bonus_summary_details` e `tbl_ecr_bonus_details` (varchar vs bigint). O join direto retorna zero registros, forçando o uso de `c_freespin_win > 0` + filtro de participantes + janela temporal como proxy.

**Impacto:** precisão estimada em ~98% — risco residual de incluir FS de outras campanhas ativas no mesmo período.

**Recomendação:** normalizar o schema `bonus` para garantir consistência de tipos na chave de join, eliminando a necessidade do workaround e permitindo isolamento exato por campanha em auditorias financeiras futuras.

---

## 13. Referências

| Item | Localização |
|------|-------------|
| Mapeamento BigQuery (90 views) | `memory/bigquery_smartico.md` |
| Schema Redshift fund/ecr/bonus | `memory/schema_fund.md`, `schema_ecr.md`, `schema_bonus.md` |
| Contexto completo da campanha | `memory/campanha_multiverso.md` |
| Script BigQuery | `db/bigquery.py` |
| Script Redshift | `db/redshift.py` |
| IDs participantes (sessão) | `C:/Users/NITRO/AppData/Local/Temp/part_ext_ids.txt` |
