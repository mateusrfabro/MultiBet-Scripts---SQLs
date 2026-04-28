# Guia de Reprodução — Segmentação PCR Multibet

> Documento completo para **reproduzir do zero** a segmentação PCR usada na operação Multibet.
> Cole este arquivo no Claude Code do analista junto com os CSVs de snapshot e ele consegue gerar o mesmo output.

---

## 1. O que é o PCR

**PCR (Player Credit Rating)** é um sistema de rating tipo "agência de crédito bancário" (Moody's/S&P) aplicado a iGaming. Cada jogador da operação recebe:

- Um **score numérico PVS (Player Value Score)** de 0 a 100
- Um **rating em letra**: S → A → B → C → D → E + categoria especial **NEW**

Fundamentado em **9 componentes** que ponderam valor financeiro + comportamento + penalização de bônus.

**Para quê:** segmentar a base de jogadores para CRM (régua de bônus, ações por tier, projeção de ganho trimestral). Substitui a antiga segmentação PVS de 6 segmentos nominais (Whale/VIP/Premium/Engajado/Regular/Casual).

---

## 2. Modelo PCR v1.3 (a versão atual)

### 2.1 Componentes do PVS

Cada componente é normalizado por **percentil rank** (0 a 100) sobre toda a base ativa. Depois é multiplicado pelo peso e somado.

| # | Componente | Peso | O que mede |
|---|------------|------|-----------|
| 1 | GGR Total | **+25** | Receita gerada para a casa nos 90d |
| 2 | Depósito Total | +15 | Volume financeiro trazido |
| 3 | Recência | +12 | Quão ativo está hoje (menor recência = melhor) |
| 4 | Margem GGR/Turnover | +10 | Intensidade de jogo |
| 5 | Nº de Depósitos | +10 | Recorrência (forte preditor) |
| 6 | Dias Ativos | +8 | Frequência na plataforma |
| 7 | Mix de Produto | +5 | Misto (casino+sport) pontua mais |
| 8 | Taxa de Atividade | +5 | Consistência (dias ativos ÷ 90) |
| 9 | Dependência de Bônus | **−10** | Penaliza quem só joga com bônus |

Soma máxima teórica: 100 pontos. Na prática a base distribui em torno de 0–75.

### 2.2 Faixas de rating (sobre o PVS percentil)

| Rating | Faixa PVS | Perfil |
|--------|-----------|--------|
| S | Top 1% (≥ P99) | Whale |
| A | 92–99% | VIP |
| B | 75–92% | Em crescimento |
| C | 50–75% | Regular |
| D | 25–50% | Casual |
| E | Bottom 25% | Engajamento mínimo |

### 2.3 Categoria NEW (especial)

Aplica-se **DEPOIS** do rating base ser calculado. Sobrescreve o rating original.

```
eh_new = (tenure_dias < 30) AND (num_deposits < 3)
```

**Critério decidido na rodada de março (Opção D entre 7 alternativas):**
- Tenure < 30 dias = "ainda em janela de onboarding"
- num_deposits < 3 = "ainda não consolidou padrão de depósito"

**Para sair do NEW basta uma das duas:**
- Tenure ≥ 30 dias → vira o rating do PVS dele
- Num_deposits ≥ 3 → vira o rating do PVS dele

A faixa esperada de NEW é **5–10% da base**. Critério mais frouxo (`OR` em vez de `AND`) capturava 87%, o que era inviável.

### 2.4 Filtros obrigatórios

⚠️ **REGRA CRÍTICA**: NÃO filtrar `c_category='real_user'` em análises de segmentação. Mantém-se TODAS as categorias (real_user, play_user, rg_closed, fraud, closed, rg_cool_off) para coerência metodológica entre rodadas. O filtro `real_user` só vale para envio operacional via Smartico (campanhas), não para análise descritiva.

---

## 3. Fontes de Dados

### 3.1 Snapshots locais (entrada)

A operação gera dois CSVs por rodada (geralmente mensal):

#### `pcr_atual_<YYYYMMDDHHMM>.csv`
PCR já calculado pela equipe de dados. Snapshot diário com 90d rolling.

**Formato**: separador `,`, decimal `.` (americano), valores em **R$** (já convertidos).

**Colunas:**
```
snapshot_date, player_id, external_id, rating, pvs, ggr_total, ngr_total,
total_deposits, total_cashouts, num_deposits, days_active, recency_days,
product_type, casino_rounds, sport_bets, bonus_issued, bonus_ratio,
wd_ratio, net_deposit, margem_ggr, ggr_por_dia, affiliate_id,
c_category, registration_date, created_at
```

**Tamanho típico**: ~140k jogadores, ~27 MB.

#### `matriz_risco_<YYYYMMDDHHMM>.csv`
Matriz de Risco v2 — sistema separado (já em produção) que classifica jogadores em comportamento.

**Formato**: separador `,`, decimal `.`, valores em **R$**.

**Colunas:**
```
label_id, user_id, user_ext_id, snapshot_date, score_bruto,
score_norm, classificacao, computed_at
```

**Categorias `classificacao`:** Muito Bom / Bom / Mediano / Ruim / Muito Ruim / Não identificado.

**Join**: `pcr_atual.external_id == matriz_risco.user_ext_id`.

### 3.2 AWS Athena (queries adicionais)

Usado para puxar:
- Top 5 jogos por célula (rating × matriz)
- Top 3 provedores por tier
- Horário/dia dominante por tier
- Intervalo mediano de depósito

**Conexão (pyathena):**
```python
import pyathena
conexao = pyathena.connect(
    aws_access_key_id="...",
    aws_secret_access_key="...",
    region_name="sa-east-1",
    s3_staging_dir="s3://aws-athena-query-results-803633136520-sa-east-1/",
    workgroup="primary",
)
```

⚠️ **NUNCA digitar credenciais manualmente.** Copiar de `Mapeamento/Athena_Mapping.py`.

**Tabelas usadas:**

| Tabela | Para quê |
|--------|----------|
| `ps_bi.fct_casino_activity_daily` | NGR por jogo por jogador (top 5 jogos) |
| `ps_bi.fct_casino_activity_hourly` | Horário e dia dominante por tier |
| `ps_bi.fct_deposits_hourly` | Intervalo entre depósitos, % <24h |
| `ps_bi.dim_game` | Lookup de game_id → nome quando o mapping local não cobre |

### 3.3 Mapping local de jogos

`Pré Análise PCR/game_image_mapping_<data>.csv` — mapeia `provider_game_id` → `game_name` + `vendor_id`.

**Colunas:** `id, game_name, game_name_upper, provider_game_id, vendor_id, game_image_url, game_slug, source, updated_at`.

**Convenção do `game_id` no Athena:** `{provider_game_id}_{room_id}` para jogos live (Bac Bo, Roleta, etc.). Para jogos slot é `{provider_game_id}` direto.

**Quando não encontrar nome no mapping:** consultar `ps_bi.dim_game` (formato pode variar).

---

## 4. Pipeline de Scripts

A rodada completa tem **8 scripts numerados** que devem ser executados em ordem. Cada um lê os outputs do anterior.

```
Estratégia Abril/
├── pcr_atual_<data>.csv             ← entrada (snapshot)
├── matriz_risco_<data>.csv          ← entrada (snapshot)
├── 01_aplicar_modelo_v2.py          → base_v13_apr27.csv
├── 02_athena_top5_jogos.py          → top5_jogos_apr27.json
├── 03_calculos_locais.py            → calculos_locais.json
├── 08_athena_extras.py              → extras_indicadores.json
├── 04_gerar_html.py                 → Estrategia_CRM_por_Rating_PCR.html
├── 05_projecao_atualizada.py        → projecao_abril.json
├── 06_gerar_projecao_html.py        → Projecao_Trimestral_Mai_Jul.html
└── 07_comparativo_html.py           → Comparativo_Marco_vs_Abril.html (opcional)
```

### 4.1 Ordem de execução

```bash
# 1. Aplica modelo v2 (calcula NEW, não filtra real_user)
python 01_aplicar_modelo_v2.py

# 2 e 3 podem rodar em paralelo (não dependem entre si)
python 02_athena_top5_jogos.py    # ~1-2 min de Athena
python 03_calculos_locais.py      # rápido (cálculos pandas)

# 4. Athena extras (horário/dia/intervalo) — pode rodar em paralelo com 2 e 3
python 08_athena_extras.py        # ~1-2 min

# 5. Geração do HTML principal (depende de 03, 02, 08)
python 04_gerar_html.py

# 6 e 7. Projeção (opcional — pode manter da rodada anterior)
python 05_projecao_atualizada.py
python 06_gerar_projecao_html.py

# 8. Comparativo com rodada anterior (opcional)
python 07_comparativo_html.py
```

### 4.2 Função de cada script

#### `01_aplicar_modelo_v2.py`
**O que faz:**
- Lê `pcr_atual_*.csv`
- Aplica regra NEW: `tenure_dias < 30 AND num_deposits < 3`
- Cruza com `matriz_risco_*.csv` por `external_id ↔ user_ext_id`
- Gera `base_v13_apr27.csv` (formato BR `;` e `,`)

**Pontos críticos:**
- NÃO aplicar filtro `c_category='real_user'`
- Calcular tenure: `(snapshot_date - registration_date).dt.days`
- Preencher matriz "Não identificado" para quem não casa no merge

#### `02_athena_top5_jogos.py`
**O que faz:**
- Query Athena: GGR (real_bet − real_win) por (player_id, game_id) em 90d
- Aggregação: top 5 jogos por (rating × classificacao_matriz) ordenados por NGR
- Calcula `top_provedores_por_rating` (top 3 vendors por tier, por bet_count)
- Salva `top5_jogos_apr27.json` com toda a estrutura

**SQL principal:**
```sql
SELECT
  player_id,
  game_id,
  SUM(real_bet_amount_base) - SUM(real_win_amount_base) AS ngr_game,
  SUM(bet_count) AS bets
FROM ps_bi.fct_casino_activity_daily
WHERE activity_date BETWEEN DATE 'YYYY-MM-DD' AND DATE 'YYYY-MM-DD'
  AND bet_count > 0
GROUP BY player_id, game_id
```

⚠️ Janela = 90 dias terminando no `snapshot_date`. Para snapshot 27/04: `2026-01-28` a `2026-04-27`.

#### `03_calculos_locais.py`
**O que faz:**
- Computa tabela consolidada por rating (n, %base, GGR, NGR, ROI, ticket, etc.)
- Cruzamento PCR × Matriz: `% distribuição` (rating linha, classe coluna)
- Cruzamento `% Bônus/NGR` por célula com valores absolutos
- Quick-wins: tamanho de população por ação
- Salva `calculos_locais.json`

**ROI por tier:**
```python
roi_bonus = ggr_total_tier / bonus_issued_tier
```

**% Bônus/NGR por célula:**
```python
pct = (sum(bonus_issued) / sum(ngr_total)) * 100   # por célula (rating × class)
```

#### `08_athena_extras.py`
**O que faz:**
- Horário dominante por tier (bucketing 0-5/6-11/12-17/18-23 → Madrugada/Manhã/Tarde/Noite)
- Dia da semana dominante por tier (`day_of_week(activity_date)`)
- Intervalo mediano entre depósitos por tier (a partir de `fct_deposits_hourly.success_count`)
- % de depósitos com intervalo <24h
- % de jogadores com bônus por tier (computa local)

**Tabela usada para depósitos:**
```sql
SELECT player_id, created_date, created_hour, success_count
FROM ps_bi.fct_deposits_hourly
WHERE created_date BETWEEN ... AND success_count > 0
```

⚠️ Coluna correta é `created_date` (NÃO `activity_date`) e `success_count` (NÃO `deposit_count`).

#### `04_gerar_html.py`
**O que faz:**
- Lê todos os JSONs (`calculos_locais`, `top5_jogos`, `extras_indicadores`)
- Gera HTML executivo único com todas as seções
- Aplica heatmap dourado (lightness HSL inverso ao volume de bônus)
- Inclui blocos estáticos de março (Playbook, Quick-wins, Projeção) com badge

**Seções do HTML:**
1. Resumo Executivo + KPIs headline
2. Tabela Consolidada por Rating (Volume/Valor + Engajamento+Horário+Dia + Top 3 Provedores + Bônus/Depósito/Risco)
3. ROI de Bônus por Tier
4. Playbook (estático março)
5. 5 Quick-wins (estático março)
6. Distribuição de Budget (atual vs proposta com NEW=5%)
7. Cruzamento PCR × Matriz (% distribuição + % Bônus/NGR + heatmap)
8. Top 5 Jogos por Célula
9. Projeção Mai-Jul (estático março)
10. Próximos passos

#### `05_projecao_atualizada.py` (opcional)
**O que faz:**
- Carrega baseline operacional de `Análise Ações PCR/baseline_projecao.json` (mediana diária de abril)
- Recalibra as 7 ações com nova base PCR:
  - **A6**: realocação de bônus C/D/E → S/A/B (estrutural, com fator marginal 0.5)
  - **A1**: reativação S/A em risco (recência 8-60d) — taxa 22%
  - **A3**: upgrade B→A (top 25% dos B por PVS) — taxa 18%
  - **A2**: reativação B em risco — taxa 15%
  - **A8**: onboarding NEW→B qualificado — taxa 25%
  - **A4**: upgrade C→B (top 25% dos C) — taxa 12%
  - **A5**: cross-sell mono→misto S/A/B — taxa 8%
- Aplica fator `92/90` para extrapolar 90d → trimestre
- Cenários: 75%, 100%, 125%

⚠️ Em uma rodada que mantém projeção de março, **não rodar este script** — usar valores fixos no HTML.

---

## 5. Cálculos detalhados

### 5.1 Distribuição proposta de bônus (v1.3)

| Tier | % do budget | Racional |
|------|-------------|----------|
| S | 20% | Subinvestido, ROI alto |
| A | 35% | Motor — ROI alto |
| B | 25% | Funil para A |
| C | 8% | ROI negativo, cortar broadcast |
| D | 4% | ROI muito negativo, reduzir |
| E | 3% | Desinvestir |
| **NEW** | **5%** | Régua de onboarding sutil (para NEW × Matriz Bom/MB/Mediano) |
| **Total** | **100%** | Mantido vs orçamento atual |

⚠️ A coluna NEW só foi incluída na **rodada de abril**. Em março era 20/35/25/10/5/5.

### 5.2 Cruzamento PCR × Matriz

**% distribuição (linha = 100%):**
```python
ct = pd.crosstab(df['rating_v13'], df['classificacao'])
ct_pct = ct.div(ct.sum(axis=1), axis=0) * 100
```

**% Bônus/NGR por célula:**
```python
piv_bonus = df.pivot_table(values='bonus_issued', index='rating_v13', columns='classificacao', aggfunc='sum')
piv_ngr = df.pivot_table(values='ngr_total', index='rating_v13', columns='classificacao', aggfunc='sum')
piv_pct = (piv_bonus / piv_ngr) * 100
```

**Leitura da % Bônus/NGR:**
- 0–10% positivo: eficiente
- 10–25% positivo: atenção
- >25% positivo: bônus pesando demais
- Negativo: bônus em célula com NGR negativo (amplifica perda)

### 5.3 Heatmap intensities (cor por volume de bônus)

```python
abs_bonus_max = max(abs(v) for v in piv_bonus.values.flatten() if v is not None)
intensity = abs(cell_bonus) / abs_bonus_max
L = 100 - 35 * intensity   # lightness HSL: branco → dourado
color = f"hsl(45, 75%, {L:.0f}%)"
```

### 5.4 Cor do texto da % Bônus/NGR

```python
def cell_pct_class(pct):
    if pct is None: return ""
    if pct < 0: return "neg"      # vermelho
    if 0 <= pct <= 10: return "pos"  # verde
    if pct > 25: return "neg"        # vermelho (bônus consumindo demais)
    return ""                         # preto default (10-25%)
```

---

## 6. Estrutura de pastas (recomendada)

```
Segmentação/
├── GUIA_REPRODUCAO_PCR.md            ← este documento
│
├── Pré Análise PCR/                  ← arquivos auxiliares
│   ├── game_image_mapping_*.csv
│   └── schema_out.txt                (schema do Athena)
│
├── PCR Mapping/                      ← histórico de PCR
│   └── pcr_ratings_*.csv
│
├── Antigos/                          ← versões arquivadas
│
├── Estratégia Final Março/           ← rodada anterior congelada
│   ├── Estrategia_CRM_por_Rating_PCR_v2.html
│   ├── Projecao_Trimestral_Mai_Jul.html
│   ├── Segmentacao_Multibet_v2.md
│   └── ...
│
├── Análise Ações PCR/                ← scripts e dados intermediários da rodada
│   ├── baseline_projecao.json
│   ├── projecao_etapa1_baseline.py
│   ├── cruzamento_bonus_ngr.json
│   └── ...
│
└── Estratégia Abril/                 ← rodada atual
    ├── pcr_atual_<data>.csv          ← snapshot novo
    ├── matriz_risco_<data>.csv       ← snapshot novo
    ├── 01_aplicar_modelo_v2.py
    ├── 02_athena_top5_jogos.py
    ├── 03_calculos_locais.py
    ├── 04_gerar_html.py
    ├── 05_projecao_atualizada.py
    ├── 06_gerar_projecao_html.py
    ├── 07_comparativo_html.py
    ├── 08_athena_extras.py
    ├── base_v13_apr27.csv            ← gerado por 01
    ├── calculos_locais.json          ← gerado por 03
    ├── top5_jogos_apr27.json         ← gerado por 02
    ├── extras_indicadores.json       ← gerado por 08
    ├── projecao_abril.json           ← gerado por 05
    └── *.html                        ← entregáveis
```

---

## 7. Comandos para reproduzir do zero (passo a passo)

### Pré-requisitos

```bash
# Python 3.10+
pip install pandas pyathena
```

### Setup
1. Pegar credenciais Athena de `Mapeamento/Athena_Mapping.py`
2. Receber os 2 CSVs de snapshot (PCR + Matriz) do time de dados
3. Colocar no folder `Segmentação/Estratégia <Mês>/`
4. Atualizar `D_INI` e `D_FIM` nos scripts 02 e 08 conforme janela 90d
5. Atualizar nomes dos arquivos PCR/Matriz nos scripts 01

### Execução

```bash
cd "Segmentação/Estratégia Abril/"

# 1. Modelo v2 + cruzamento matriz
python 01_aplicar_modelo_v2.py

# 2-3-8. Em paralelo (ou sequencial)
python 02_athena_top5_jogos.py
python 03_calculos_locais.py
python 08_athena_extras.py

# 4. HTML estratégia (consolida tudo)
python 04_gerar_html.py
```

**Tempo total esperado:** ~5 min (a maior parte é Athena).

---

## 8. Regras críticas (memória do projeto)

⚠️ **As 5 regras que NÃO podem ser violadas:**

1. **NÃO filtrar `c_category='real_user'`** em análises de segmentação. Manter todas as categorias.
2. **CSVs de saída em formato BR**: `sep=";"`, `decimal=","`, `encoding="utf-8-sig"`.
3. **Tabelas Iceberg `_ec2`**: SEMPRE filtrar por `dt` (sem ele = full scan, estoura recursos).
4. **Credenciais Athena**: copiar de `Mapeamento/Athena_Mapping.py`. NUNCA digitar manualmente.
5. **Janela 90d**: a janela do PCR é rolling 90 dias terminando no `snapshot_date`. Athena queries devem usar exatamente essa janela para consistência (28/01 a 27/04 para snapshot 27/04).

---

## 9. Output esperado (sanity checks)

Para validar que o pipeline rodou correto, conferir:

### `01_aplicar_modelo_v2.py`
- Total jogadores: ~130–160k (ordem de grandeza)
- NEW: 5–10% da base
- Cobertura matriz: ≥85% identificados

### `03_calculos_locais.py`
- S total: ~1.000–2.000 jogadores
- A total: ~8.000–12.000
- Bônus/GGR: ~20–25%
- NGR > 0 em S/A/B; NGR < 0 em C/D/E
- ROI S/A: positivo, geralmente +12 a +20x
- ROI C/D/E: negativo

### `02_athena_top5_jogos.py`
- ~700k linhas player × game (90d casino)
- NGR total casino: ~R$ 30–40M (9pts/100 sobre 0-100% margem casino real)
- Top providers em S/A: PGSoft + Pragmatic + Spribe (Aviator) dominam ~90%

### `04_gerar_html.py`
- HTML output: ~80 KB
- 10 seções principais
- Distribuição de budget proposta soma 100%

---

## 10. Documentos de referência (no repositório)

- **`Estratégia Final Março/Segmentacao_Multibet_v2.md`** — especificação completa do PCR v1.3 (componentes, pesos, escala, NEW, integração Smartico, matriz de risco, budget, métricas de saúde, ciclo mensal). **Este é o "manual de regras"**.
- **`Estratégia Final Março/Estrategia_CRM_por_Rating_PCR_v2.html`** — entregável da rodada de março (referência visual).
- **`Estratégia Abril/Estrategia_CRM_por_Rating_PCR_abril.html`** — entregável atual.
- **`Análise Ações PCR/`** — scripts e dados intermediários históricos.

---

## 11. Próxima rodada — checklist

Quando for executar a próxima rodada (snapshot 27/05 ou similar):

- [ ] Receber novos CSVs (PCR + Matriz) com data atualizada
- [ ] Criar pasta `Estratégia Maio/` e mover arquivos atuais para `Estratégia Final Abril/`
- [ ] Copiar scripts 01–08 da pasta atual para a nova pasta
- [ ] Atualizar nomes de arquivos no script 01 (e em 03 se necessário)
- [ ] Atualizar `D_INI` e `D_FIM` nos scripts 02 e 08 (janela 90d ending na nova data)
- [ ] Rodar pipeline (passos 4–6 acima)
- [ ] Validar com sanity checks da seção 9
- [ ] Comparar com rodada anterior via `07_comparativo_html.py`
- [ ] Decidir se atualiza Playbook/Quick-wins/Projeção (mantém ou refaz)

---

## 12. Notas finais

- **Playbook e Quick-wins** estão "congelados" desde março — só são atualizados quando o time de CRM revalida.
- **Projeção** segue valores de março (R$ 13,75M ganho calibrado, R$ 33,33M cenário base) até decisão explícita de recalcular.
- **Distribuição de bônus** com NEW=5% é a versão atual (entrou na rodada de abril).
- **Matriz de Risco v2** evolui em paralelo (sistema separado, 21 tags). PCR e Matriz são complementares: PCR responde "quanto vale", Matriz responde "como joga".

Para perguntas sobre o modelo PCR conceitual, consultar `Segmentacao_Multibet_v2.md`.
Para perguntas sobre execução do pipeline, este guia tem tudo que precisa.

---

## 13. Material para entregar ao analista

Para o analista carregar no Claude Code dele e reproduzir do zero, **enviar os seguintes itens em um zip:**

### Documentação (obrigatória para o Claude Code dele entender)
- [ ] **`GUIA_REPRODUCAO_PCR.md`** (este arquivo) — manual completo
- [ ] **`Segmentacao_Multibet_v2.md`** — especificação do modelo PCR (regras de cálculo, pesos, NEW, etc.)

### Snapshots de entrada (atualizar a cada rodada)
- [ ] **`pcr_atual_<YYYYMMDDHHMM>.csv`** — snapshot do PCR (gerado pela equipe de dados)
- [ ] **`matriz_risco_<YYYYMMDDHHMM>.csv`** — snapshot da Matriz de Risco

### Mapping local (uma vez só, raro mudar)
- [ ] **`game_image_mapping_<data>.csv`** — lookup de jogos (manter o mais recente)

### Scripts do pipeline (8 arquivos)
- [ ] `01_aplicar_modelo_v2.py`
- [ ] `02_athena_top5_jogos.py`
- [ ] `03_calculos_locais.py`
- [ ] `04_gerar_html.py`
- [ ] `05_projecao_atualizada.py`
- [ ] `06_gerar_projecao_html.py`
- [ ] `07_comparativo_html.py`
- [ ] `08_athena_extras.py`

### Dados de referência (para projeção e comparativo)
- [ ] **`baseline_projecao.json`** (em `Análise Ações PCR/`) — mediana diária de abril (baseline operacional)

### Credenciais (separado, com cuidado)
- [ ] Credenciais AWS Athena (do `Mapeamento/Athena_Mapping.py` da operação) — passar por canal seguro

### Schema de referência (opcional mas útil)
- [ ] **`schema_out.txt`** (em `Pré Análise PCR/`) — schema das tabelas Athena

---

### Como o analista usa no Claude Code dele

1. **Coloca esta pasta na raiz do projeto dele:**
   ```
   Segmentação/
   ├── GUIA_REPRODUCAO_PCR.md           ← apresentar como contexto
   ├── Segmentacao_Multibet_v2.md       ← especificação do modelo
   ├── Estratégia <Mês>/
   │   ├── pcr_atual_<data>.csv
   │   ├── matriz_risco_<data>.csv
   │   ├── 01_aplicar_modelo_v2.py
   │   ├── ... (demais scripts)
   ├── Pré Análise PCR/
   │   └── game_image_mapping_<data>.csv
   └── Análise Ações PCR/
       └── baseline_projecao.json
   ```

2. **Configura o Claude Code dele:**
   - Carrega o `GUIA_REPRODUCAO_PCR.md` como contexto
   - Configura credenciais Athena no `Mapeamento/Athena_Mapping.py`

3. **Pede ao Claude Code:**
   ```
   "Use o GUIA_REPRODUCAO_PCR.md e rode o pipeline completo
   sobre os arquivos da pasta 'Estratégia <Mês>'. Atualize a janela
   90d para terminar em <data do snapshot>."
   ```

4. **Valida com os sanity checks da seção 9.**

---

*Versão: 1.0 — Geração 2026-04-27 — Rodada base: Abril 2026 (snapshot 27/04)*
