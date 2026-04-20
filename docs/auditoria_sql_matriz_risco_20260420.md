# Auditoria SQL — Matriz de Risco v2

**Data:** 2026-04-20
**Auditor:** Mateus Fabro (com checklist de 6 BOs do code review do Gusta)
**Escopo:** 21 SQLs de regra (`ec2_deploy/sql/risk_matrix/*.sql`) + pipeline orquestrador + push CRM Smartico
**Pipeline em produção:** cron diário 02:00 BRT (05:00 UTC) na EC2, push Smartico 02:30 BRT, ~155K jogadores, 21 tags.

---

## Resumo executivo

- **SQLs auditados:** 21 / 21
- **Pipelines auditados:** 2 (`ec2_deploy/pipelines/risk_matrix_pipeline.py` e `scripts/risk_matrix_pipeline.py`)
- **Push CRM auditado:** 1 (`scripts/push_risk_matrix_to_smartico.py`)
- **Validação estatística:** 1 (`scripts/validacao_estatistica_risk_matrix.py`)

**Achados por categoria** (ocorrências em SQLs, não bugs únicos):

| BO | Categoria | Ocorrências | Severidade dominante |
|----|-----------|-------------|----------------------|
| 1  | INNER JOIN silencioso | 21 / 21 SQLs | **Crítico** |
| 2  | Full scan Athena | 21 / 21 SQLs | **Médio** (custo) |
| 3a | CAST/tipo divergente no JOIN | 0 direto, 1 via pipeline (user_ext_id ".0") | Baixo |
| 3b | ROW_NUMBER sem tie-breaker | 0 (não usa) | N/A |
| 3c | MAX/MIN string como majoritário | 0 | N/A |
| 4  | JOIN entre CTEs com colunas de tempo diferentes | 2 SQLs (CASHOUT_AND_RUN, REINVEST_PLAYER) | Médio |
| 5  | AVG em distribuição skewed | 4 SQLs (BEHAV_RISK_PLAYER, ENGAGED_PLAYER, RG_ALERT_PLAYER, ZERO_RISK_PLAYER) | Médio |
| 6  | COUNT DISTINCT em CASE WHEN | 0 direto, mas há pivot/OR equivalente no pipeline | Baixo |

**Achados sistêmicos adicionais (fora do checklist):**

- **S1 — Pipeline e deploy divergem** (arquivos `scripts/risk_matrix_pipeline.py` vs `ec2_deploy/pipelines/risk_matrix_pipeline.py` confirmado via `diff -q`: files differ). O código lógico é o mesmo (mesmas funções e SQLs), só paths e comentários mudam. **Risco baixo hoje, mas viola CLAUDE.md §"Consistência entre ambientes".**
- **S2 — POTENCIAL_ABUSER.sql sem filtro de partição na `first_deposit` CTE** (varre `cashier_ec2.tbl_cashier_deposit` desde o início). Custo alto + amplifica BO1.
- **S3 — Cascata de INNER JOINs com `brand` e `WHERE br.label_id IS NOT NULL`** derruba jogadores com `c_partner_id IS NULL` ANTES de chegar ao push Smartico. Dado o filtro `u.c_partner_id IS NOT NULL` já no CTE `users` do CASHOUT_AND_RUN, há redundância, mas nos outros 20 SQLs o INNER só aparece no final.
- **S4 — Pipeline faz LEFT JOIN correto** (`user_base.merge(pivoted, on=["label_id","user_id"], how="left")` — linha 557 do `risk_matrix_pipeline.py`), então quem passou o filtro da base de usuários NÃO é perdido. A perda acontece DENTRO de cada SQL individual, não no merge final.

**Severidade geral:** **MÉDIA-ALTA.**
- Nenhum achado tipo "jogador cai no bucket errado por bug silencioso" (crítico puro) foi encontrado — a cadeia de INNER JOINs é consistente em 21 SQLs, então o viés é SISTÊMICO e igual pra todas as tags. Isso neutraliza parte do risco: um jogador que seria filtrado no JOIN das tags também seria filtrado pelo `user_base` (que tem o mesmo padrão).
- Porém, há **cenários específicos de quebra** (BO4 em CASHOUT_AND_RUN, BO5 em médias skewed, BO2 em `first_deposit` sem filtro) que merecem hotfix.

---

## Tabela por regra (21 linhas)

Legenda:
- `JOIN` = BO1: INNER JOIN silencioso (S=sim presente, N=não, **B**=bloqueante confirmado)
- `Scan` = BO2: janela fixa rolante sem filtro de partição (S/N)
- `Div` = BO3: divergência tipo/formato no JOIN (S/N)
- `CTE` = BO4: JOIN entre CTEs com colunas de tempo diferentes (S/N)
- `AVG` = BO5: AVG em distribuição skewed sem mediana (S/N)
- `CountDist` = BO6: COUNT DISTINCT em CASE WHEN (S/N)
- `Sev` = severidade agregada: **Cr**ítico / **Me**dio / **Ba**ixo

| # | Regra | JOIN | Scan | Div | CTE | AVG | CountDist | Sev | Observação principal |
|---|-------|:---:|:---:|:---:|:---:|:---:|:---:|:---:|----------------------|
| 1 | REGULAR_DEPOSITOR | S | S | N | N | N | N | Ba | `HAVING AVG(qtd_dep)>=3` — média mensal; ok p/ esta regra (ela é sobre média). |
| 2 | PROMO_ONLY | S | S | N | N | N | N | Me | Denominador inclui só depósitos; ratio ok. |
| 3 | ZERO_RISK_PLAYER | S | S | N | N | **S** | N | **Me** | `AVG(deposit)` vs `AVG(cashout)` ± 30% — AVG ultra-skewed em depósitos (alguns 50k, mediana 100). |
| 4 | FAST_CASHOUT | S | S | N | N | N | N | Me | `JOIN cashouts` por user apenas, sem pareamento transacional. Pode flagar quem depositou 50x e sacou 1x num outro dia que por coincidência caiu em ±1h de algum depósito. |
| 5 | SUSTAINED_PLAYER | S | S | N | N | N | N | Ba | Lógica simples; flag correto. |
| 6 | NON_BONUS_DEPOSITOR | S | S | N | N | N | N | Ba | `NOT EXISTS` anti-join — padrão correto. |
| 7 | PROMO_CHAINER | S | S | N | N | N | N | Ba | LEFT JOIN correto entre bonus_days e activity_days. |
| 8 | CASHOUT_AND_RUN | S | S | N | **S** | N | N | **Me** | `bonus_date` (DATE) JOIN `cashout_date` (DATE) com `BETWEEN bu.bonus_date AND bu.bonus_date + 1 DAY` — pode pegar cashout de ANTES do bonus se mesmo dia (truncou pra DATE). |
| 9 | REINVEST_PLAYER | S | S | N | **S** | N | N | Me | `BETWEEN co.c_created_time AND co.c_created_time + 7 DAY` — ok, mas NÃO filtra `d.c_created_time >= start_ts` na `cashier_deposit` (pode trazer depósitos fora da janela pra matchar saque recente). |
| 10 | NON_PROMO_PLAYER | S | S+7d | N | N | N | N | Ba | Janela custa pouco (7d). |
| 11 | ENGAGED_PLAYER | S | S | N | N | **S** | N | Me | `AVG(sessions_count)` entre 3 e 10 — se jogador teve 1 dia com 50 sessões e 89 dias com 0, AVG pode cair na faixa artificialmente (mas median sem). |
| 12 | RG_ALERT_PLAYER | S | S | N | N | **S** | N | **Cr** | Idem 11 — AVG > 10 pode incluir jogador com 1 spike de bot. **Impacta segmentação de jogo responsável (regulatório).** |
| 13 | BEHAV_RISK_PLAYER | S | S | N | N | **S** | N | Me | `STDDEV/AVG > 2.0` — coef de variação, ok conceitualmente. Mas `AVG` no numerador pode mascarar o sinal se 1 saque alto puxar média. |
| 14 | POTENCIAL_ABUSER | S | **SS** | N | N | N | N | **Me** | **`first_deposit` CTE SEM FILTRO TEMPORAL** — varre `cashier_deposit` inteiro. Custo alto + amplifica full scan. |
| 15 | PLAYER_REENGAGED | S | S | N | N | N | N | Ba | `date_diff` entre CTEs com janelas distintas, mas o campo temporal é o mesmo (`c_start_time`). |
| 16 | SLEEPER_LOW_PLAYER | S | S | N | N | N | N | Ba | Thresholds fixos (2-15 dias ativos), ok. |
| 17 | VIP_WHALE_PLAYER | S | S | N | N | N | N | Me | GGR = bets-wins em `fund_ec2`; conforme memory MEMORY.md: rollbacks já não entram por `c_txn_status='SUCCESS'`. Correto. |
| 18 | WINBACK_HI_VAL_PLAYER | S | S | N | N | N | N | Me | Idem VIP, janela recente (30d) separada. |
| 19 | BEHAV_SLOTGAMER | S | S | N | N | N | N | Ba | `casino_bets/total_bets >= 0.70` — ratio, não AVG. |
| 20 | MULTI_GAME_PLAYER | S | S | N | N | N | N | Me | `COUNT DISTINCT c_session_id >= 3` por hora + 10 ocorrências. **⚠ usa `EXTRACT(HOUR FROM t.c_start_time)` em UTC, não BRT** (viola regra do CLAUDE.md sobre timezone, mas não afeta o filtro). |
| 21 | ROLLBACK_PLAYER | S | S | N | N | N | N | Ba | LEFT JOIN correto; COALESCE para dividir por zero. Porém **NÃO filtra `c_txn_status='SUCCESS'` na CTE `rollback_transactions`** — conta rollbacks falhos. |

**S+7d** = janela 7d ao invés de 90d (barato).
**SS** = full scan sem filtro (pior que S).

---

## Top 5 achados críticos

### #1 — RG_ALERT_PLAYER usa AVG em distribuição skewed para flag regulatório

**Arquivo:** `ec2_deploy/sql/risk_matrix/RG_ALERT_PLAYER.sql:53`
**BO:** 5
**Severidade:** **Crítico** (afeta segmentação de Jogo Responsável — tema regulatório)

**Trecho:**
```sql
-- Sessoes por dia por jogador
daily_sessions AS (
  SELECT t.c_ecr_id AS user_id,
         CAST(t.c_start_time AS DATE) AS game_date,
         COUNT(DISTINCT t.c_session_id) AS sessions_count
  FROM fund_ec2.tbl_real_fund_txn t
  WHERE t.c_start_time >= (SELECT start_ts FROM params)
    ...
  GROUP BY t.c_ecr_id, CAST(t.c_start_time AS DATE)
),
avg_sessions AS (
  SELECT user_id,
         AVG(CAST(sessions_count AS DOUBLE)) AS avg_daily_sessions
  FROM daily_sessions
  GROUP BY user_id
),
qualifying AS (
  SELECT user_id FROM avg_sessions
  WHERE avg_daily_sessions > 10.0
)
```

**Diagnóstico:**
Distribuição de sessões/dia é extremamente assimétrica (cauda direita). Um jogador com 1 dia de 80 sessões e 10 dias de 2 sessões tem `AVG = (80+20)/11 ≈ 9` — fica de fora. Mas um jogador com 1 único dia de 11 sessões (e nenhum outro dia) tem `AVG = 11` — entra no RG_ALERT. É exatamente o oposto do que a tag pretende capturar ("10+ sessões/dia sustentado").

Como `avg_sessions` é calculado SÓ sobre dias em que o jogador jogou (não todos os 90d), jogadores esporádicos com sessão única inflada entram; jogadores verdadeiramente viciados com padrão consistente podem ficar de fora.

**Correção sugerida:**
```sql
-- Usar MEDIANA via APPROX_PERCENTILE (Presto/Trino) + mínimo de dias ativos
avg_sessions AS (
  SELECT user_id,
         APPROX_PERCENTILE(CAST(sessions_count AS DOUBLE), 0.5) AS median_daily_sessions,
         COUNT(*) AS active_days
  FROM daily_sessions
  GROUP BY user_id
),
qualifying AS (
  SELECT user_id FROM avg_sessions
  WHERE median_daily_sessions > 10.0
    AND active_days >= 5  -- pelo menos 5 dias pra ser sustentado
)
```

Mesmo problema se aplica a **ENGAGED_PLAYER.sql:51** com `AVG BETWEEN 3 AND 10` — mas severidade Média pois tag positiva (+10) com impacto menor que flag regulatório.

---

### #2 — CASHOUT_AND_RUN: JOIN entre CTEs em datas DATE com `BETWEEN` assimétrico

**Arquivo:** `ec2_deploy/sql/risk_matrix/CASHOUT_AND_RUN.sql:66-71`
**BO:** 4 (JOIN quebrado entre CTEs com conceitos de tempo diferentes)
**Severidade:** **Médio**

**Trecho:**
```sql
bonus_usage AS (
  SELECT DISTINCT b.c_ecr_id AS user_id,
         CAST(b.c_created_time AS DATE) AS bonus_date  -- DATE (00:00 UTC)
  FROM bonus_ec2.tbl_bonus_pocket_txn b ...
),
cashouts AS (
  SELECT c.c_ecr_id AS user_id,
         CAST(c.c_created_time AS DATE) AS cashout_date  -- DATE (00:00 UTC)
  FROM cashier_ec2.tbl_cashier_cashout c ...
),
qualifying AS (
  SELECT DISTINCT bu.user_id
  FROM bonus_usage bu
  JOIN cashouts co ON bu.user_id = co.user_id
   AND co.cashout_date BETWEEN bu.bonus_date AND bu.bonus_date + INTERVAL '1' DAY
  JOIN recent_activity ra ON bu.user_id = ra.user_id
  WHERE ra.last_activity_date <= co.cashout_date + INTERVAL '2' DAY
)
```

**Diagnóstico:**
1. Truncou `c_created_time` para DATE em UTC. Um saque feito às 02:00 BRT = 05:00 UTC aparece no dia seguinte comparado ao bonus local. Tag pode perder pares bonus+saque legítimos ou flagar pares bogus.
2. `BETWEEN bu.bonus_date AND bu.bonus_date + 1 DAY` — como são DATE, isso vira `DATE1 <= cashout_date <= DATE1+1`, ou seja, cashout pode ser ANTES do bonus se fossem eventos no mesmo dia (hora do saque pode ser 10:00 e hora do bonus 18:00 — truncados ambos viram o mesmo DATE e o BETWEEN aceita).
3. Conceitualmente a tag quer "bonus → saque → inatividade". Com DATE UTC, há drift de ±1 dia.

**Correção sugerida:**
Comparar em TIMESTAMP, não DATE:
```sql
qualifying AS (
  SELECT DISTINCT bu.user_id
  FROM bonus_usage bu  -- guarde b.c_created_time cru
  JOIN cashouts co ON bu.user_id = co.user_id
   AND co.cashout_ts > bu.bonus_ts
   AND co.cashout_ts <= bu.bonus_ts + INTERVAL '24' HOUR
  JOIN recent_activity ra ON bu.user_id = ra.user_id
  WHERE ra.last_activity_ts <= co.cashout_ts + INTERVAL '48' HOUR
)
```

---

### #3 — POTENCIAL_ABUSER: `first_deposit` CTE sem filtro temporal (full scan histórico)

**Arquivo:** `ec2_deploy/sql/risk_matrix/POTENCIAL_ABUSER.sql:27-35`
**BO:** 2
**Severidade:** **Médio** (custo + reprocessamento)

**Trecho:**
```sql
first_deposit AS (
  SELECT d.c_ecr_id AS user_id,
         MIN(d.c_created_time) AS first_deposit_time
  FROM cashier_ec2.tbl_cashier_deposit d
  WHERE d.c_txn_status = 'txn_confirmed_success'
    AND d.c_initial_amount > 0
  GROUP BY d.c_ecr_id
),
qualifying AS (
  SELECT user_id
  FROM first_deposit
  WHERE first_deposit_time >= CURRENT_TIMESTAMP - INTERVAL '2' DAY
)
```

**Diagnóstico:**
A CTE `first_deposit` varre TODA a história de `cashier_ec2.tbl_cashier_deposit` (sem WHERE de tempo). Só depois, o `qualifying` filtra para últimos 2 dias.

- Na prática, Presto/Iceberg pode até fazer predicate pushdown parcial, mas `MIN()` por user impede pushdown completo.
- **Custo real:** escanear centenas de milhões de linhas diariamente para flaggar contas criadas nos últimos 2 dias.
- **Conceitualmente errado também:** `first_deposit` é aproximação de "data de criação da conta", mas um jogador que primeiro depositou há 1 ano e refez primeiro depósito ontem (após 1 ano inativo) seria QUALIFICADO incorretamente? Não, porque `MIN` pega o histórico — mas e se o backend faz hard-delete de registros antigos? Depende da retenção.

**Correção sugerida:**
```sql
-- Usar ecr_ec2.tbl_ecr.c_created_time (data real de signup) ao invés de proxy por first_deposit
qualifying AS (
  SELECT u.c_ecr_id AS user_id
  FROM ecr_ec2.tbl_ecr u
  WHERE u.c_created_time >= CURRENT_TIMESTAMP - INTERVAL '2' DAY
    AND u.c_partner_id IS NOT NULL
)
```

Remove o full scan + fica semanticamente correto (conta criada, não primeiro depósito).

---

### #4 — ZERO_RISK_PLAYER: ratio de AVG em distribuição de depósito extremamente skewed

**Arquivo:** `ec2_deploy/sql/risk_matrix/ZERO_RISK_PLAYER.sql:52-58`
**BO:** 5
**Severidade:** **Médio**

**Trecho:**
```sql
qualifying AS (
  SELECT d.user_id
  FROM avg_deposits d
  JOIN avg_cashouts c ON d.user_id = c.user_id
  WHERE d.avg_deposit > 0
    AND ABS(c.avg_cashout - d.avg_deposit) / d.avg_deposit <= 0.30
)
```

**Diagnóstico:**
Em base MultiBet, depósitos têm distribuição long-tail: mediana ~100 BRL, cauda de depósitos de 5k-50k. `AVG(c_initial_amount)` de um jogador com 10 depósitos de 100 e 1 de 10k = 1000. Se ele saca com padrão similar (ticket alto), avg_cashout vira proxy ruim.

Tag "Zero Risk" promete "jogador que evita risco" (score 0 = neutro). Mas pega qualquer jogador com ratio bate-e-pronto, incluindo quem tem 1 par deposit+cashout.

**Correção sugerida:**
```sql
-- Usar mediana + minimo de transacoes
avg_deposits AS (
  SELECT d.c_ecr_id AS user_id,
         APPROX_PERCENTILE(CAST(d.c_initial_amount AS DOUBLE), 0.5) AS median_deposit,
         COUNT(*) AS n_deposits
  FROM ... GROUP BY d.c_ecr_id HAVING COUNT(*) >= 3
),
-- idem para cashouts HAVING COUNT(*) >= 2
qualifying AS (
  SELECT d.user_id
  FROM avg_deposits d JOIN avg_cashouts c ON ...
  WHERE ABS(c.median_cashout - d.median_deposit) / d.median_deposit <= 0.30
)
```

---

### #5 — Push CRM: dedup por diff compara SET de tags (sem tie-breaker), tiers mudam silenciosamente

**Arquivo:** `scripts/push_risk_matrix_to_smartico.py:299`
**BO:** 3 (tangencialmente — é ordenação que faz set()=set() dar igualdade)
**Severidade:** **Médio** (pode deixar de enviar update quando deveria)

**Trecho:**
```python
if set(player.smartico_tags()) != set(prev_player.smartico_tags()):
    changed.append(player)
```

E `smartico_tags()` em linha 145:
```python
return sorted(set(tags))
```

**Diagnóstico:**
A comparação `set() != set()` está correta para detectar mudança no CONJUNTO de tags. Mas:
- **score_norm mudou** (ex: 47 → 53 muda tier Mediano → Bom) → tag RISK_TIER_MEDIANO vira RISK_TIER_BOM → SET muda → update é enviado. OK.
- **score_norm mudou dentro do MESMO tier** (ex: 47 → 49, continua Mediano) → SET permanece igual → NÃO envia update. OK conceitualmente.
- **mas:** o `score_norm` não é enviado como propriedade Smartico nesse flow (só as tags). Então se o CRM quer usar score_norm pra segmentar, vai ficar com valor stale. **Verificar com Raphael se Smartico consome só as tags ou também precisa do score numérico.**

Observação adicional: `pick_canary_user` em `scripts/push_risk_matrix_to_smartico.py:355` usa `random_state=42` — ótimo (reprodutível). Mas `candidates.sample(n=1, random_state=42)` sempre pega O MESMO user em toda execução. Se o canary já foi usado em teste Fase 1 anterior, vai sortear ele de novo. Não é bug — é comportamento esperado pelo seed fixo — mas pode surpreender.

**Correção sugerida:**
- Confirmar com CRM (Raphael) se score_norm entra como propriedade Smartico (além das tags) — se sim, adicionar comparação de score_norm ao diff:
  ```python
  if (set(player.smartico_tags()) != set(prev_player.smartico_tags())
      or abs(player.score_norm - prev_player.score_norm) >= 5):
      changed.append(player)
  ```
- Trocar `random_state=42` em `pick_canary_user` por `random_state=None` ou derivado do timestamp do snapshot, para evitar sempre o mesmo user.

---

## Padrões sistêmicos

### P1 — 21 de 21 SQLs usam janela fixa rolante de 90 dias (BO2)

Todas as CTEs `params` são:
```sql
WITH params AS (
  SELECT CURRENT_TIMESTAMP - INTERVAL '90' DAY AS start_ts,
         CURRENT_TIMESTAMP AS end_ts
)
```

Executando diariamente 02:00 BRT, cada run reprocessa ~90 dias de `fund_ec2.tbl_real_fund_txn` (a maior tabela — ordem de centenas de milhões de linhas). **Não há filtro de partição Iceberg** (lembrando que `dt` não existe como coluna visível — validado empiricamente MEMORY.md). Todos os filtros são por `c_start_time >=`, o que o Iceberg pode ou não resolver via metadata stats dependendo de como a tabela foi particionada.

**Impacto:** custo de Athena alto (cada run escaneia ~1-3 TB acumulado entre 21 queries), mas dado read-only e uso crítico, não é bloqueante. **Problema arquitetural, não de SQLs individuais.**

**Mitigação sugerida (fora do escopo desta auditoria):**
- Mover para pipeline incremental: manter tabela intermediária `multibet.fact_player_daily_features` agregada D-1, rodar só o delta de features do dia.
- Ou: aceitar o custo e documentar no Dashboard de custo AWS.

### P2 — 21 de 21 SQLs têm padrão `users` + `brand` INNER JOIN (BO1)

Todos os 21 arquivos seguem:
```sql
users AS (SELECT ... FROM ecr_ec2.tbl_ecr u JOIN ecr_ec2.tbl_ecr_flags f ON u.c_ecr_id=f.c_ecr_id WHERE f.c_test_user=false),
brand AS (SELECT c_partner_id ... FROM ecr_ec2.tbl_ecr WHERE c_partner_id IS NOT NULL GROUP BY c_partner_id),
...
SELECT ... FROM qualifying q
JOIN users u ON q.user_id = u.user_id           -- INNER
LEFT JOIN brand br ON u.crm_brand_id = br.crm_brand_id
WHERE br.label_id IS NOT NULL                   -- transforma LEFT em INNER
```

**Consequências:**
- **Test users:** INNER JOIN com `users` derruba quem tem `c_test_user=true`. **Correto e desejado.**
- **`c_partner_id IS NULL`:** o LEFT JOIN seguido de `WHERE br.label_id IS NOT NULL` vira um INNER semântico. Jogadores sem partner_id (legado, multibrand mal cadastrado) são perdidos. Dado que `load_user_base()` (linha 200 do pipeline) também filtra `u.c_partner_id IS NOT NULL`, está CONSISTENTE — não há BO1 bloqueante.
- **Atenção:** se algum dia o `tbl_ecr_flags` ficar stale (ex: ETL de flags travar), INNER JOIN vai derrubar jogadores que NÃO têm linha em `tbl_ecr_flags` (silenciosamente). Já aconteceu em outras pipelines (memory: `feedback_test_users_filtro_completo.md`). **Monitorar coverage diário.**

### P3 — INNER JOIN final `JOIN users u ON q.user_id = u.user_id` é redundante

Todo SQL termina com `JOIN users`, mas o `qualifying` já veio de CTEs que só usam `user_id` (não joined com users). Esse último JOIN serve só para:
1. Puxar `u.c_partner_id` (pra mapear label_id).
2. Re-filtrar test users (mas a lista qualifying NUNCA passou por users antes — então se um test user foi qualificado, ele só é removido aqui).

**Consequência:** funciona, mas a lógica está fora de ordem. O ideal seria filtrar test users ANTES de calcular `qualifying` (economia de compute). Não é bug, é oportunidade de otimização.

### P4 — MULTI_GAME_PLAYER usa hora UTC, não BRT (viola regra CLAUDE.md)

**Arquivo:** `ec2_deploy/sql/risk_matrix/MULTI_GAME_PLAYER.sql:31,39`
```sql
EXTRACT(HOUR FROM t.c_start_time) AS game_hour
```

CLAUDE.md obriga BRT:
```sql
EXTRACT(HOUR FROM t.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo')
```

**Impacto prático:** mínimo. A regra é "3+ sessões na mesma HORA" — hora UTC ou BRT, desde que consistente, capta a mesma coisa (sessões simultâneas). Mas viola o contrato da CLAUDE.md e compromete auditorias futuras. Equivalente em BEHAV_RISK_PLAYER.sql:31,35 está CORRETO (usa BRT). Inconsistência entre SQLs do mesmo projeto.

### P5 — ROLLBACK_PLAYER não filtra `c_txn_status` na CTE de rollbacks

**Arquivo:** `ec2_deploy/sql/risk_matrix/ROLLBACK_PLAYER.sql:27-37`
```sql
rollback_transactions AS (
  SELECT t.c_ecr_id AS user_id,
         COUNT(*) AS total_rollbacks, ...
  FROM fund_ec2.tbl_real_fund_txn t
  WHERE t.c_start_time >= (SELECT start_ts FROM params)
    AND t.c_start_time <  (SELECT end_ts FROM params)
    AND t.c_txn_type IN (72, 76, 61, 63, 91, 113)  -- tipos rollback
  -- ⚠ SEM c_txn_status = 'SUCCESS'
  GROUP BY t.c_ecr_id
)
```

Por outro lado `regular_bets` (linhas 40-50) filtra `c_txn_status='SUCCESS'`. Assim, o numerador (rollbacks) conta até rollbacks falhos/abortados, e o denominador só SUCCESS. Razão inflada artificialmente para jogadores com muitas transações flapping.

**Impacto:** potencial false-positive em ROLLBACK_PLAYER (-15 pontos). Dado que score -15 não é catastrófico, mas é bias sistemático em jogadores com padrão de erro técnico.

**Correção:**
```sql
AND t.c_txn_status = 'SUCCESS'  -- adicionar na rollback_transactions
```

### P6 — Pipelines e deploy podem divergir (CLAUDE.md §"Consistência entre ambientes")

`scripts/risk_matrix_pipeline.py` ≠ `ec2_deploy/pipelines/risk_matrix_pipeline.py` (files differ). Revisão manual confirma que só paths/comentários divergem, a lógica é idêntica. Mas a regra da CLAUDE.md é clara: "Pipeline local e deploy devem ser a MESMA versão". **Unificar em um único arquivo** (o deploy-facing) e importar/symlink no outro ambiente.

---

## Conclusões por checklist

- **BO1 (INNER JOIN silencioso):** Presente em 100% dos SQLs, mas o viés é sistêmico e compensado pelo `user_base` usar os mesmos filtros. **Não há perda de usuário específica de tag.**
- **BO2 (Full scan):** Sistêmico em 100%. Custo alto; hotfix em POTENCIAL_ABUSER (achado #3) é o único bloqueante.
- **BO3 (Divergência tipo no JOIN):** Não encontrado em SQL. Há ajuste para ".0" no pipeline Python (`_clean_ext_id`), o que indica que em algum momento user_ext_id foi escrito como float-como-string. Se isso ainda acontece, pode causar mismatch em JOINs futuros.
- **BO4 (CTE com tempos diferentes):** CASHOUT_AND_RUN e REINVEST_PLAYER. Ambos médios.
- **BO5 (AVG skewed):** RG_ALERT_PLAYER (crítico por ser regulatório), ENGAGED_PLAYER, ZERO_RISK_PLAYER, BEHAV_RISK_PLAYER.
- **BO6 (COUNT DISTINCT em CASE WHEN):** Não encontrado nos SQLs. Pipeline Python faz pivot com `aggfunc='first'` — não há risco de soma dupla.

---

## Recomendação de rollout de hotfix

Ordem sugerida por retorno/custo:

1. **RG_ALERT_PLAYER** (Crítico #1) — trocar AVG → APPROX_PERCENTILE(0.5) + min de dias ativos. Impacto regulatório. 30 min.
2. **POTENCIAL_ABUSER** (Médio #3) — trocar proxy first_deposit por `ecr_ec2.tbl_ecr.c_created_time`. Economia de custo + correção semântica. 20 min.
3. **ROLLBACK_PLAYER** (P5) — adicionar `c_txn_status='SUCCESS'` em rollback_transactions. 5 min.
4. **CASHOUT_AND_RUN** (Médio #2) — comparar timestamps em vez de DATE. 30 min.
5. **MULTI_GAME_PLAYER** (P4) — adicionar conversão BRT. 5 min (só compliance com CLAUDE.md).
6. **ZERO_RISK_PLAYER + ENGAGED_PLAYER + BEHAV_RISK_PLAYER** (BO5) — trocar AVG → mediana. 15 min cada.
7. **Pipeline** — unificar scripts/ vs ec2_deploy/ em um só arquivo, outro vira symlink ou import. 20 min.
8. **Scan de 90d rolante** (P1) — arquitetural, avaliar depois. Não hotfix.
