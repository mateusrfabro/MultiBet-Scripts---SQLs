# Auditoria SQL — PCR (Player Credit Rating)

> Data: 20/04/2026
> Auditor: squad/auditor (code review baseado em `memory/feedback_sql_review_checklist_gusta.md`)
> Escopo: pipeline PCR end-to-end (Athena -> Python -> Super Nova DB -> Smartico)
> Objetivo: detectar os 4 padroes de B.O. recorrentes apontados pelo Gusta (20/04) + impactos especificos do rating

---

## Resumo Executivo

| Arquivo | Linhas | B.O. encontrados |
|---|---:|---|
| `pipelines/pcr_pipeline.py` | 541 | 6 (1 Critico, 2 Medios, 3 Baixos) |
| `scripts/pcr_scoring.py` | 463 | 3 (0 Critico, 1 Medio, 2 Baixos) — nota: script legado, CSV-only |
| `scripts/push_pcr_to_smartico.py` | 582 | 3 (1 Critico, 1 Medio, 1 Baixo) |
| `ec2_deploy/deploy_pcr_pipeline.sh` | 162 | 1 (Baixo) |
| `ec2_deploy/run_pcr_pipeline.sh` | 34 | 0 |
| `docs/pcr_player_credit_rating.md` | 247 | 1 (Medio, inconsistencia doc vs codigo) |

**Distribuicao por checklist:**
| # | Categoria | Achados | Severidade max |
|---:|---|---:|---|
| 1 | INNER JOIN silencioso | 2 | Baixo |
| 2 | Full scan Athena / janela fixa | 1 | Baixo |
| 3a | CAST VARCHAR entre tipos/formatos | 1 | Medio |
| 3b | ROW_NUMBER sem tie-breaker | 1 | **Critico** |
| 3c | MAX/MIN(string) como "majoritario" | 1 | **Critico** |
| 4 | CTEs com colunas de tempo diferentes | 0 | — |
| 5 | AVG sem mediana/p90 em distribuicao skewed | 1 | Medio |
| 6 | COUNT DISTINCT CASE WHEN dupla contagem | 0 | — |

**Extras (fora checklist, mas criticos para rating):**
- Filtros c_category NAO aplicados antes do ranking -> players `fraud`/`closed`/`play_user` (11.6% da base) distorcem percentis e ocupam lugar de real_user no Top 1% (S).
- Push ao Smartico envia `PCR_RATING_E` para jogadores `fraud`/`rg_closed` -> tag incorreta em conta bloqueada.
- PVS calculado sobre base heterogenea (casual com 1 deposito vs whale com 200) sem filtro de amostra minima -> rating do casual e matematicamente ruido.

**Veredicto:** **BLOQUEADO**. 2 achados Criticos impedem confianca no rating. Hotfix urgente antes do proximo push Smartico.

---

## 1. INNER JOIN silencioso (drop invisivel)

### 1.1 (Baixo) `pipelines/pcr_pipeline.py:222-233` — subquery `ecr_bi` via LEFT JOIN, mas fonte interna nao-deterministica
O LEFT JOIN em si esta correto (usa LEFT), mas o `ROW_NUMBER() OVER (PARTITION BY c_ecr_id ORDER BY c_category)` dentro da subquery — ver achado 3.2 — pode retornar `NULL` em `c_category` se nao houver registro em `bireports_ec2.tbl_ecr`. Como LEFT JOIN, linha nao e dropada, mas `c_category` vira NULL -> cai no bucket `NULL` da distribuicao no log e nao e filtrada. Impacto: baixo (nao afeta rating), mas mascarado.

**Trecho:**
```sql
LEFT JOIN (
    SELECT c_ecr_id, c_category
    FROM (
        SELECT c_ecr_id, c_category,
               ROW_NUMBER() OVER (PARTITION BY c_ecr_id ORDER BY c_category) AS rn
        FROM bireports_ec2.tbl_ecr
    )
    WHERE rn = 1
) ecr_bi ON m.player_id = ecr_bi.c_ecr_id
```

**Impacto no rating:** nulo direto. Indireto: jogadores sem `c_category` nao sao filtrados da base ativa, podem ser `play_user` (conta demo) entrando no calculo de percentis do PVS.

**Correcao:** adicionar `WHERE ecr_bi.c_category = 'real_user'` na selecao final, ou no minimo excluir `fraud`, `closed`, `rg_closed`, `play_user` (11.6% da base segundo doc secao 7).

### 1.2 (Baixo) `pipelines/pcr_pipeline.py:223` — `LEFT JOIN ps_bi.dim_user` ok, mas external_id pode ser NULL
`player_id` = `ecr_id`. Se o jogador apostou (existe em `fct_player_activity_daily`) mas nao tem linha em `dim_user`, sai com `external_id = NULL`. O pipeline grava a linha mesmo assim (DDL aceita `external_id` nullable), e o push Smartico filtra `WHERE external_id IS NOT NULL` em `scripts/push_pcr_to_smartico.py:119`. Linhas perdidas no push nao sao logadas. Quantificar: rodar `SELECT COUNT(*) FROM pcr_ratings WHERE snapshot_date = MAX(...) AND external_id IS NULL`. Se >1%, investigar.

**Correcao:** acrescentar log no final da extracao:
```python
log.info(f"  -> Sem external_id: {df['external_id'].isna().sum()} ({df['external_id'].isna().mean()*100:.2f}%)")
```

---

## 2. Full scan Athena / janela fixa

### 2.1 (Baixo) `pipelines/pcr_pipeline.py:193-194` — janela fixa 90 dias diaria
```sql
WHERE f.activity_date >= CURRENT_DATE - INTERVAL '{JANELA_DIAS}' DAY
  AND f.activity_date < CURRENT_DATE
```
Isso escaneia **90 dias inteiros de `fct_player_activity_daily` todo dia** — aceitavel porque o PVS depende de janela deslizante (dia N precisa dos dias N-89 a N-1 de novo), mas custa. Nao e full scan (tem filtro), mas **reprocessa 89 dias que ja foram lidos ontem**. Custo marginal recorrente.

**Impacto:** performance/custo Athena. Se `fct_player_activity_daily` for uma tabela de alguns GB/dia, pode virar dezenas de GB scan/dia. Nao afeta rating.

**Mitigacoes possiveis (fora do escopo de hotfix):**
- Materializar agregado incremental em S3/Super Nova, acumular diff e recalcular PVS sobre pre-agregado
- Trocar 90 dias deslizantes para janela fixa por mes (reduz scan mas muda semantica)

**Obs (positivo):** `activity_date < CURRENT_DATE` ja exclui D-0 parcial conforme `feedback_sempre_usar_d_menos_1.md`. Correto.

---

## 3. Divergencia no JOIN / nao-determinismo

### 3.1 (Medio) `pipelines/pcr_pipeline.py:223` — JOIN `m.player_id = u.ecr_id` sem cast, mas sem validacao de tipo
`player_id` vem de `fct_player_activity_daily` (BIGINT no dbt ps_bi) e `ecr_id` do `dim_user` (BIGINT). Se por acaso um dos dois virar VARCHAR (mudanca schema silenciosa do dbt), Athena faz cast implicito e pode perder zeros a esquerda — `ecr_id` tem 18 digitos. Hoje nao ha bug, mas **falta defensive cast**.

**Impacto:** baixo hoje; MEDIO se schema mudar sem o pipeline pegar -> drop silencioso da base inteira (linha vazia) porque join falha.

**Correcao:** acrescentar teste de sanidade no final da extracao:
```python
assert df['external_id'].notna().mean() > 0.95, "Join com dim_user caiu abaixo de 95%"
```

### 3.2 (**CRITICO**) `pipelines/pcr_pipeline.py:226-233` — `ROW_NUMBER() OVER (PARTITION BY c_ecr_id ORDER BY c_category)` nao-deterministico e semanticamente errado

**Trecho:**
```sql
-- Deduplica: pega c_category mais recente por jogador
SELECT c_ecr_id, c_category
FROM (
    SELECT c_ecr_id, c_category,
           ROW_NUMBER() OVER (PARTITION BY c_ecr_id ORDER BY c_category) AS rn
    FROM bireports_ec2.tbl_ecr
)
WHERE rn = 1
```

**3 problemas em um:**

1. **Comentario mente:** diz "pega c_category mais recente" mas o `ORDER BY c_category` ordena pelo **proprio valor da coluna em ordem alfabetica**. Jogador que passou por `real_user -> fraud -> closed` devolve sempre `closed` (C vem antes de R/F alfabeticamente). **Isso e exatamente o B.O. 3c (MIN-string como majoritario)**.

2. **Nao-deterministico em ties:** se um `c_ecr_id` tiver duas linhas com `c_category = 'real_user'`, o ROW_NUMBER sorteia qualquer uma — sem tie-breaker secundario (B.O. 3b). Em `tbl_ecr` isso e menos comum (geralmente 1 linha por ecr), mas se houver multiplas o resultado vai oscilar entre execucoes. **Impacto direto no rating**: em uma rodada o jogador vem como `real_user` e entra no ranking; em outra vem como `fraud` e stakeholder nao confia mais no relatorio.

3. **O mais grave:** a tabela `bireports_ec2.tbl_ecr` tem **uma linha por jogador** (e cadastro, nao fato). O DISTINCT/ROW_NUMBER nao e necessario — e um hack defensivo que introduziu dois bugs. Se por engano um jogador tiver 2 linhas (dado sujo), o pipeline sorteia em vez de alertar.

**Impacto no rating:** **CRITICO**. `c_category` e usado no filtro de negocio (`WHERE c_category = 'real_user'` recomendado na doc secao 7). Se vier `closed` para um jogador real, ele fica de fora do push Smartico e **nao recebe campanha**. Se vier `real_user` para um jogador `fraud`, ele **recebe tag `PCR_RATING_S` e campanha** mesmo estando bloqueado — risco reputacional e compliance.

**Correcao:**
```sql
-- Alternativa A (recomendada): se tbl_ecr e 1-linha-por-jogador, sem dedup
LEFT JOIN bireports_ec2.tbl_ecr ecr_bi ON m.player_id = ecr_bi.c_ecr_id

-- Alternativa B: se houver duplicatas e precisar dedup, ordenar por timestamp
ROW_NUMBER() OVER (
    PARTITION BY c_ecr_id
    ORDER BY c_updated_time DESC NULLS LAST, c_ecr_id DESC  -- tie-breaker
) AS rn
```
Validar empiricamente antes de escolher:
```sql
SELECT c_ecr_id, COUNT(*) FROM bireports_ec2.tbl_ecr
GROUP BY c_ecr_id HAVING COUNT(*) > 1 LIMIT 10;
```

### 3.3 (Medio) `scripts/push_pcr_to_smartico.py:381-389` — `IN ({placeholders})` com variantes string
```python
ext_variants = [ext_id]
if not ext_id.endswith(".0"):
    ext_variants.append(ext_id + ".0")
```
Sinaliza que ha **dois formatos de external_id convivendo na tabela** (com e sem `.0`), provavelmente contaminacao pandas (`float` -> `"12345.0"`). B.O. 3a classico. Ja tem workaround (`_clean_ext_id`), mas o fato de precisar variantes indica que em algum lugar o cast nao foi feito. A DDL de `pcr_ratings` declara `external_id BIGINT`, entao o insert (linha 414 do pipeline via `_safe_int`) ja converte para int — ok para dados novos. Mas se houver historico de versao anterior com varchar, o fallback e valido.

**Impacto:** baixo para dados novos (pipeline atual grava BIGINT). Medio se alguem reintroduzir fonte VARCHAR — cast silencioso perde zeros a esquerda em IDs numericos com padding.

**Correcao:** manter fallback por seguranca, mas adicionar log quando `ext_variants` bater na variante `.0`:
```python
if len(ext_variants) > 1 and cursor.rowcount > 0:
    log.warning("user_ext_id encontrado via variante '.0' — dados legado na base")
```

---

## 4. CTEs com colunas de tempo diferentes

**Nao encontrado.** Pipeline usa uma unica CTE (`player_metrics`) que agrega por `player_id` usando so `activity_date`. JOIN posterior e com dimensoes (dim_user, tbl_ecr) que nao tem tempo. Sem cross-month risk.

---

## 5. AVG em distribuicao skewed (sem mediana/p90)

### 5.1 (Medio) `pipelines/pcr_pipeline.py:111-118` — View `pcr_resumo` reporta so AVG
```sql
ROUND(AVG(ggr_total), 2)       AS ggr_medio,
ROUND(AVG(total_deposits), 2)  AS deposito_medio,
ROUND(AVG(num_deposits), 1)    AS num_dep_medio,
ROUND(AVG(days_active), 1)     AS dias_ativos_medio,
ROUND(AVG(recency_days), 1)    AS recencia_media,
ROUND(AVG(pvs), 2)             AS pvs_medio
```
Para um **rating** baseado em valor, AVG de GGR/deposito em distribuicao fortemente skewed (cauda longa dos whales — doc secao 3 ja confirma: S+A = 12% da base, 80% do GGR positivo) e ruim:
- O `ggr_medio` do rating S vai parecer muito alto porque 2-3 outliers dominam
- O `ggr_medio` do rating D pode ser negativo mas mediana pode ser zero/positiva -> decisao de campanha fica torta

**Obs:** `scripts/pcr_scoring.py:297-314` (o script legado CSV-only) JA TEM `ggr_mediano` no resumo. **O pipeline novo (pcr_pipeline.py) regrediu** — reescreveu o resumo em SQL e perdeu a mediana.

**Impacto no rating:** MEDIO. Nao afeta o rating em si (PVS usa percentil rank — imune a skew), mas afeta **relatorios baseados em `pcr_resumo`** que podem levar o Head a conclusoes erradas sobre valor medio de cada tier.

**Correcao:** adicionar mediana/p90 na view `pcr_resumo` via `approx_percentile` (Postgres 15 tem `percentile_cont`):
```sql
percentile_cont(0.5)  WITHIN GROUP (ORDER BY ggr_total) AS ggr_mediano,
percentile_cont(0.9)  WITHIN GROUP (ORDER BY ggr_total) AS ggr_p90,
percentile_cont(0.5)  WITHIN GROUP (ORDER BY total_deposits) AS deposito_mediano,
percentile_cont(0.9)  WITHIN GROUP (ORDER BY total_deposits) AS deposito_p90,
```

---

## 6. COUNT DISTINCT em CASE WHEN

**Nao encontrado.** Nao ha agrupamento por tier/bucket com CASE WHEN no pipeline — PVS e atribuicao de rating sao calculados em pandas (fora de SQL), entao esse padrao nao se aplica.

---

## 7. Analise do racional matematico (PVS + Rating)

### 7.1 (**CRITICO**) Amostra minima NAO-imposta — PVS aplicado em jogador com 1 deposito tem mesma legitimidade que jogador com 200
**Localizacao:** `pipelines/pcr_pipeline.py:196-199` (HAVING)
```sql
HAVING COALESCE(SUM(f.casino_realbet_count), 0) > 0
    OR COALESCE(SUM(f.sb_realbet_count), 0) > 0
    OR COALESCE(SUM(f.deposit_success_count), 0) > 0
```
Filtro de atividade: **>= 1** (qualquer uma das 3 condicoes). **Nenhum minimo de amostra.**

**Problema racional:** a formula PVS pressupoe que cada componente tem base estatistica significativa:
- `margem_ggr = GGR / turnover_total` — jogador com 1 rodada de R$ 100 e win de R$ 5 tem margem = 0.95. Isso bate com `score_margem` invertido, vira score baixo; parece correto, mas **estatisticamente e ruido** — uma rodada nao define margem.
- `bonus_ratio = bonus_issued / total_deposits` — jogador com 1 bonus de boas-vindas R$ 50 e 1 deposito R$ 10 tem ratio = 5.0 (penalidade maxima -10) quando na verdade e um novo usuario normal.
- `taxa_atividade = days_active / 90` — jogador cadastrado ontem tem 1 dia ativo / 90 = 0.011. Score 1.1.

Resultado: **novos jogadores caem automaticamente no rating E** por construcao da formula, mesmo sendo potencialmente high-value. Nao ha cold-start handling.

**Impacto no rating:** CRITICO. O filtro `feedback_ftd_fontes_decision_tree.md` e `project_cohort` indicam que FTDs recentes (ultimos 7-14 dias) sao justamente quem merece atencao de CRM. Aqui eles viram `PCR_RATING_E` e sao tratados como "engajamento minimo". Push Smartico com tag errada -> campanha inadequada.

**Correcao sugerida (requer decisao do Head):**
- Opcao A: separar ratings — criar tag `PCR_RATING_NEW` para jogadores com `days_active < 14` ou `num_deposits < 3`, nao atribuir E/D/C/B/A/S para eles
- Opcao B: filtro de amostra minima (`HAVING num_deposits + COUNT(rodadas) >= 5`) + relatorio a parte dos excluidos
- Opcao C (minimo): acrescentar coluna `maturity_flag` na tabela indicando base de calculo (1=novato; 2=suficiente)

### 7.2 (Medio) Percentis recalculados toda rodada -> rating ignora evolucao temporal
**Localizacao:** `pipelines/pcr_pipeline.py:325-341`
```python
p25 = result["pvs"].quantile(0.25)
...
p99 = result["pvs"].quantile(0.99)
conditions = [result["pvs"] >= p99, result["pvs"] >= p92, ...]
```
**O rating e relativo a distribuicao do dia** — nao e um corte fixo. Se a base toda piorar amanha (feriado, queda de trafego), o jogador S de ontem com PVS 78 pode virar A hoje **mesmo sem mudar nada no comportamento dele**. Isso e fundamentalmente diferente de credit rating bancario que o produto se propoe a replicar (rating AAA tem criterios absolutos, nao relativos).

**Impacto no rating:** MEDIO. Gera volatilidade artificial na tag Smartico — o `diff_players()` em `push_pcr_to_smartico.py:219` vai detectar "mudou de rating" e empurrar update pra Smartico toda vez que a distribuicao oscilar. Ruido na API, ruido no CRM, CPM de campanha por jogador que na verdade nao mudou.

**Correcao sugerida:**
- Fixar cortes de PVS por N dias (rolling window do percentil), ex: media dos percentis dos ultimos 30 dias, atualiza 1x por mes
- Ou publicar ratings absolutos (bandas fixas de PVS: S >= 80, A >= 65, etc.) calibradas periodicamente

### 7.3 (**CRITICO**) `c_category` existe mas NAO e usado como filtro antes do ranking
**Localizacao:** `pipelines/pcr_pipeline.py:209, 234`
O pipeline extrai `c_category` (fraud, closed, rg_closed, play_user, real_user) mas **nao filtra**. Todos entram no ranking de PVS e recebem um rating E-S. Isso gera 2 bugs:

1. **Distorcao dos percentis:** 11.6% da base (segundo doc secao 7) e nao-real_user. Os percentis P25/P50/P75/P92/P99 sao calculados sobre base contaminada. Se `fraud` tende a ter GGR alto (perde muito por atacado), infla o P99 e o jogador S legitimo pode virar A.

2. **Push Smartico para contas bloqueadas:** `scripts/push_pcr_to_smartico.py:119` filtra somente `external_id IS NOT NULL` e `rating IN ('S','A','B','C','D','E')`. **NAO filtra c_category**. Entao conta `fraud` ou `rg_closed` recebe tag `PCR_RATING_*` no Smartico e pode virar alvo de campanha. Compliance-wise: ruim — jogador que fechou por jogo responsavel (`rg_closed`) nao deveria receber incentivo.

**Correcao (2 niveis):**
- No pipeline (pcr_pipeline.py): filtrar apenas `real_user` **antes do ranking**:
  ```python
  df = df[df["c_category"].isin(["real_user"])].copy()  # antes de calcular PVS
  ```
- No push (push_pcr_to_smartico.py:111-122): adicionar `AND c_category = 'real_user'` na query de snapshot.

### 7.4 (Baixo) Escala doc vs codigo — inconsistencia E-S vs D-AAA
**Localizacao:** `docs/pcr_player_credit_rating.md` varios trechos + `scripts/pcr_scoring.py:455-450`
Doc e script legado falam em **D, C, B, A, AA, AAA** (6 tiers com duas letras). Pipeline novo (`pcr_pipeline.py`) e push Smartico usam **E, D, C, B, A, S** (6 tiers com letra unica). O HTML (`PCR_Player_Credit_Rating_v1.2.html` — nao lido aqui mas mencionado na doc) provavelmente usa a v1.2 (E-S).

**Impacto:** comunicacao. Stakeholder le doc, espera rating AAA, ve no Smartico tag `PCR_RATING_S`. Gera ticket desnecessario.

**Correcao:** atualizar `docs/pcr_player_credit_rating.md` para escala E-S (remover AAA/AA). Ja tem trecho correto na secao 3 mas secoes 11 e legenda falam de AAA.

---

## 8. Top 3 Achados Criticos (priorizados para hotfix)

### Critico #1 — `c_category` nao filtrado antes do PVS e nem no push Smartico
**Arquivos:** `pipelines/pcr_pipeline.py:209,234` + `scripts/push_pcr_to_smartico.py:111-122`

**Diagnostico:** 11.6% da base (fraud, closed, rg_closed, play_user) entra no calculo de percentis e recebe tag PCR_RATING no Smartico. Compliance issue (rg_closed recebendo campanha) + distorcao de ranking de jogadores reais.

**Trecho (pipeline):**
```python
WHERE (u.is_test = false OR u.is_test IS NULL)
# NAO tem AND c_category = 'real_user'
```

**Correcao:**
```sql
-- no SQL da extracao:
WHERE (u.is_test = false OR u.is_test IS NULL)
  AND ecr_bi.c_category = 'real_user'  -- <-- ADICIONAR
```
E no push:
```sql
-- scripts/push_pcr_to_smartico.py:111
SELECT ... FROM multibet.pcr_ratings
WHERE snapshot_date = %s
  AND external_id IS NOT NULL
  AND rating IN ('S','A','B','C','D','E')
  AND c_category = 'real_user'  -- <-- ADICIONAR
```

### Critico #2 — ROW_NUMBER com ORDER BY alfabetico + comentario mentiroso
**Arquivo:** `pipelines/pcr_pipeline.py:226-233`

**Diagnostico:** dedup de `tbl_ecr` por `ORDER BY c_category` (alfabetico) em vez de timestamp. Jogador historicamente `real_user -> fraud -> closed` devolve sempre `closed`. Comentario diz "mais recente" mas e alfabetico. Possivel bug ja em producao.

**Trecho:**
```sql
-- Deduplica: pega c_category mais recente por jogador  <-- MENTE
ROW_NUMBER() OVER (PARTITION BY c_ecr_id ORDER BY c_category) AS rn
```

**Correcao (investigar primeiro):**
```sql
-- 1. Confirmar se tbl_ecr tem duplicatas:
SELECT c_ecr_id, COUNT(*) FROM bireports_ec2.tbl_ecr
GROUP BY c_ecr_id HAVING COUNT(*) > 1 LIMIT 20;

-- 2a. Se NAO tiver duplicatas, remover a subquery:
LEFT JOIN bireports_ec2.tbl_ecr ecr_bi ON m.player_id = ecr_bi.c_ecr_id

-- 2b. Se tiver duplicatas, usar timestamp como ORDER BY:
ROW_NUMBER() OVER (
    PARTITION BY c_ecr_id
    ORDER BY c_updated_time DESC NULLS LAST, c_ecr_id DESC
) AS rn
```

### Critico #3 — Sem amostra minima — jogadores novos (1 deposito, <14 dias) sao classificados junto com whales
**Arquivo:** `pipelines/pcr_pipeline.py:196-199` + formula PVS `pipelines/pcr_pipeline.py:297-307`

**Diagnostico:** HAVING admite jogador com apenas 1 evento. Formula PVS usa ratios (`margem_ggr`, `bonus_ratio`, `taxa_atividade`) que sao matematicamente instaveis com n pequeno. Novo FTD cai automaticamente em rating E e recebe campanha de "engajamento minimo" em vez de "boas-vindas".

**Trecho:**
```sql
HAVING COALESCE(SUM(f.casino_realbet_count), 0) > 0
    OR COALESCE(SUM(f.sb_realbet_count), 0) > 0
    OR COALESCE(SUM(f.deposit_success_count), 0) > 0
```

**Correcao (requer decisao do Head, mas padrao minimo):**
```python
# Separar novatos ANTES do ranking PVS
df_novos = df[(df["days_active"] < 14) | (df["num_deposits"] < 3)].copy()
df_novos["rating"] = "NEW"
df_maduros = df[~df.index.isin(df_novos.index)].copy()
# Calcular PVS so para maduros, publicar tag PCR_RATING_NEW para novos
```
E mapear `RATING_TO_SMARTICO["NEW"] = "PCR_RATING_NEW"` em `push_pcr_to_smartico.py:81`.

---

## 9. Achados secundarios (nao bloqueantes)

| # | Arquivo | Linha | Achado | Severidade |
|---|---|---|---|---|
| 9.1 | `pipelines/pcr_pipeline.py` | 111-118 | View `pcr_resumo` so tem AVG, falta mediana/p90 | Medio |
| 9.2 | `pipelines/pcr_pipeline.py` | 325-341 | Cortes PVS dinamicos geram volatilidade artificial no push Smartico | Medio |
| 9.3 | `docs/pcr_player_credit_rating.md` | varias | Escala D-AAA na doc vs E-S no codigo | Medio |
| 9.4 | `scripts/pcr_scoring.py` | 50-100 | Script legado ainda no repo, mesma falha de c_category (ele nao extrai c_category — melhor deprecar) | Baixo |
| 9.5 | `ec2_deploy/deploy_pcr_pipeline.sh` | 158-159 | Exemplo SQL comentado usa `c_category = 'active'` que nao e valor valido (doc secao 7 lista real_user/closed/fraud/etc.) | Baixo |
| 9.6 | `ec2_deploy/deploy_pcr_pipeline.sh` | 127-130 | `crontab grep -v "pcr_pipeline"` remove linha com esse substring, tecnicamente idempotente mas pode impactar se outro pipeline tiver nome similar | Baixo |

---

## 10. Recomendacao final

Valeu auditar? **Sim, valeu muito.** Encontrei **3 achados Criticos** e 6 Medios/Baixos. O checklist do Gusta capturou o pior deles (3c — ROW_NUMBER ORDER BY alfabetico) que ja esta em producao local e teria ido para EC2. Os outros 2 criticos (`c_category` nao filtrado + sem amostra minima para novatos) sao de design do pipeline, nao do checklist puro SQL, mas derivam da mesma disciplina de review.

**Hotfix urgente:** os 3 Criticos devem ser corrigidos **antes do proximo push Smartico** — o pipeline ja roda local e a doc diz "deploy EC2 pendente, aguardando validacao do Head", entao ainda da tempo de resolver sem rollback de producao. Especificamente o #1 (c_category nao filtrado) e compliance — se chegar ao Smartico tag de campanha para conta `rg_closed`, e problema regulatorio de jogo responsavel.

**Pode esperar?** Nao. O #2 (ROW_NUMBER alfabetico) afeta confiabilidade do rating hoje mesmo no ambiente local — stakeholder que consulta `multibet.pcr_atual` recebe `c_category` potencialmente errado. Corrigir antes de qualquer demo/validacao com Head.

**Ordem sugerida de hotfix:**
1. Critico #1 — filtrar `c_category = 'real_user'` no SQL e no push (2 linhas alteradas, baixo risco)
2. Critico #2 — investigar duplicatas em `tbl_ecr` e escolher alternativa A ou B (1 diagnostico + 1 alteracao)
3. Critico #3 — discussao com Head sobre tratamento de novatos (decisao de produto, 1-2 dias)
4. Medios — mediana na view, escala doc, cortes fixos PVS (pode ir pra sprint normal)

Arquivos relevantes:
- Relatorio: `c:/Users/NITRO/OneDrive - PGX/MultiBet/docs/auditoria_sql_pcr_20260420.md`
- Pipeline auditado: `c:/Users/NITRO/OneDrive - PGX/MultiBet/pipelines/pcr_pipeline.py`
- Push auditado: `c:/Users/NITRO/OneDrive - PGX/MultiBet/scripts/push_pcr_to_smartico.py`
- Script legado (considerar deprecar): `c:/Users/NITRO/OneDrive - PGX/MultiBet/scripts/pcr_scoring.py`
- Deploy: `c:/Users/NITRO/OneDrive - PGX/MultiBet/ec2_deploy/deploy_pcr_pipeline.sh`

## Veredicto

**BLOQUEADO** — 3 achados criticos impedem confianca no rating e geram risco de compliance (push Smartico para `rg_closed`). Hotfix em 2 arquivos (`pipelines/pcr_pipeline.py` + `scripts/push_pcr_to_smartico.py`) resolve os 2 criticos mais urgentes (#1 e #2) com baixo raio de impacto. Critico #3 precisa de decisao de produto com Head antes de codigo.
