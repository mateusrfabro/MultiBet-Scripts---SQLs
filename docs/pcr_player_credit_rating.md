# PCR — Player Credit Rating v1.0

> Documento tecnico do sistema de rating de jogadores.
> Criado: 15/04/2026 | Autor: Mateus Fabro | Status: Pipeline pronto, deploy EC2 pendente

---

## 1. Visao Geral

O PCR (Player Credit Rating) e um sistema de classificacao de jogadores inspirado em credit scoring bancario. Atribui um rating de **E** (pior) a **S** (melhor) baseado em 9 dimensoes de valor e comportamento.

**Objetivo:** substituir segmentacoes manuais por um score quantitativo, reproduzivel e atualizado diariamente, permitindo ao CRM e ao time de negocio tomar decisoes baseadas em dados.

### Origem

O Castrin (Head de Dados) criou a segmentacao PVS (Player Value Score) com 6 segmentos para apresentacao a diretoria em Abril/2026. O PCR evolui esse modelo adicionando normalizacao por percentil, penalizacao por bonus e escala tipo credit rating.

---

## 2. Arquitetura

```
[Athena]                          [Super Nova DB]              [Consumo]
ps_bi.fct_player_activity_daily   multibet.pcr_ratings         CRM / Dashboards
ps_bi.dim_user                    multibet.pcr_atual (view)    Reports CSV
bireports_ec2.tbl_ecr             multibet.pcr_resumo (view)   Analises ad-hoc
        |                                  |
        +--- pcr_pipeline.py --------------+
             (Athena read -> calculos Python -> PostgreSQL write)
```

| Componente | Caminho |
|---|---|
| Pipeline principal | `pipelines/pcr_pipeline.py` |
| Script antigo (CSV only) | `scripts/pcr_scoring.py` |
| Deploy EC2 | `ec2_deploy/deploy_pcr_pipeline.sh` |
| Run wrapper EC2 | `ec2_deploy/run_pcr_pipeline.sh` |
| HTML apresentacao | `PCR_Player_Credit_Rating_v1.2.html` |

### Estrategia de persistencia

- **TRUNCATE + INSERT** — somente o snapshot mais recente e mantido na tabela
- View `pcr_atual` aponta automaticamente para o ultimo snapshot
- View `pcr_resumo` agrega por rating (contagem, GGR, PVS medio)

### Cron EC2 (planejado)

| Hora UTC | BRT | Pipeline |
|----------|------|----------|
| 03:30 | 00:30 | grandes_ganhos |
| 04:00 | 01:00 | sync_google_ads |
| 04:15 | 01:15 | sync_meta_spend |
| 05:00 | 02:00 | risk_matrix |
| **06:30** | **03:30** | **pcr_pipeline** |

---

## 3. Escala de Ratings (E-S v1.2)

| Rating | Faixa PVS | Perfil | Jogadores (14/04) | GGR |
|--------|-----------|--------|-------------------|-----|
| **S** | Top 1% (>= P99) | Whale | ~1.600 | R$ 6.2M |
| **A** | 92-99% | VIP | ~11.200 | R$ 13.8M |
| **B** | 75-92% | Em crescimento | ~27.200 | R$ 4.1M |
| **C** | 50-75% | Regular | ~40.000 | R$ -4.3M |
| **D** | 25-50% | Casual | ~40.000 | R$ -2.7M |
| **E** | Bottom 25% | Engajamento minimo | ~40.000 | R$ -731K |

**Total base:** ~159.800 jogadores ativos (apostou ou depositou nos ultimos 90 dias).

> **Nota:** ratings C, D e E tem GGR negativo — sao jogadores que ganham mais do que perdem. Isso e esperado: a curva de valor e fortemente concentrada no topo (S+A = ~12% da base, ~80% do GGR positivo).

---

## 4. Player Value Score (PVS) — Formula

O PVS e um score de 0 a 100 calculado por 9 componentes normalizados por percentil:

| Componente | Peso | Direcao | Descricao |
|---|---|---|---|
| GGR Total | +25% | Maior = melhor | Receita bruta gerada pelo jogador |
| Deposito Total | +15% | Maior = melhor | Volume total de depositos |
| Recencia | +12% | Mais recente = melhor | Dias desde ultima atividade (invertido) |
| Margem GGR/Turnover | +10% | Invertido | Margem da casa sobre o jogador |
| Num Depositos | +10% | Maior = melhor | Frequencia de depositos |
| Dias Ativos | +8% | Maior = melhor | Dias com atividade no periodo |
| Mix Produto | +5% | Misto = bonus | MISTO=100, CASINO/SPORT=40, OUTRO=0 |
| Taxa Atividade | +5% | Maior = melhor | dias_ativos / 90 (janela) |
| **Bonus Penalizador** | **-10%** | Maior = pior | bonus_issued / total_deposits |

**Normalizacao:** cada componente e convertido para percentil rank (0-100) antes de aplicar o peso.

---

## 5. Fontes de Dados

| Fonte | Database | Uso |
|---|---|---|
| `fct_player_activity_daily` | ps_bi | Metricas diarias: GGR, depositos, saques, rounds, bonus |
| `dim_user` | ps_bi | Cadastro: external_id, registration_date, affiliate_id, is_test |
| `tbl_ecr` | bireports_ec2 | Status da conta (c_category): real_user, closed, fraud, etc. |

### Definicao de jogador ativo (v1.1)

**Ativo** = quem APOSTOU (casino ou sportsbook) OU DEPOSITOU nos ultimos 90 dias.
- Login sozinho NAO conta
- Bonus emitido sem aposta NAO conta
- D-0 (dia corrente) e EXCLUIDO (dados parciais)
- Jogadores de teste (`is_test = true`) sao excluidos

---

## 6. Tabela no Banco — `multibet.pcr_ratings`

| Coluna | Tipo | Descricao |
|---|---|---|
| `snapshot_date` | DATE | Data do calculo (PK junto com player_id) |
| `player_id` | BIGINT | ecr_id — ID interno 18 digitos |
| `external_id` | BIGINT | Smartico user_ext_id — usar para joins CRM |
| `rating` | VARCHAR(2) | E, D, C, B, A ou S |
| `pvs` | NUMERIC(8,2) | Player Value Score (0-100) |
| `ggr_total` | NUMERIC(15,2) | GGR em BRL |
| `ngr_total` | NUMERIC(15,2) | NGR em BRL |
| `total_deposits` | NUMERIC(15,2) | Depositos em BRL |
| `total_cashouts` | NUMERIC(15,2) | Saques em BRL |
| `num_deposits` | INTEGER | Qtd depositos |
| `days_active` | INTEGER | Dias com atividade |
| `recency_days` | INTEGER | Dias desde ultima atividade |
| `product_type` | VARCHAR(10) | CASINO, SPORT ou MISTO |
| `casino_rounds` | BIGINT | Rodadas de casino |
| `sport_bets` | BIGINT | Apostas esportivas |
| `bonus_issued` | NUMERIC(15,2) | Bonus emitido em BRL |
| `bonus_ratio` | NUMERIC(8,4) | Bonus / Depositos |
| `wd_ratio` | NUMERIC(8,4) | Saques / Depositos |
| `net_deposit` | NUMERIC(15,2) | Depositos - Saques |
| `margem_ggr` | NUMERIC(8,4) | GGR / Turnover |
| `ggr_por_dia` | NUMERIC(15,2) | GGR medio por dia ativo |
| `affiliate_id` | VARCHAR(300) | ID do afiliado |
| `c_category` | VARCHAR(50) | Status da conta (real_user, fraud, closed...) |
| `registration_date` | DATE | Data de cadastro |
| `created_at` | TIMESTAMPTZ | Timestamp da insercao |

### Views disponiveis

```sql
-- Snapshot mais recente (usar esta para consultas)
SELECT * FROM multibet.pcr_atual ORDER BY pvs DESC;

-- Resumo por rating
SELECT * FROM multibet.pcr_resumo;

-- Top 50 VIPs ativos
SELECT * FROM multibet.pcr_atual
WHERE c_category = 'real_user' AND rating IN ('S', 'A')
ORDER BY pvs DESC LIMIT 50;
```

---

## 7. Status da Conta (c_category)

| Valor | Descricao | % base (14/04) |
|---|---|---|
| `real_user` | Conta real ativa | 88.4% |
| `play_user` | Conta demo | 4.3% |
| `rg_closed` | Fechada por jogo responsavel | 3.0% |
| `fraud` | Fraude identificada | 2.1% |
| `closed` | Fechada pelo jogador | 1.4% |
| `rg_cool_off` | Cooling off (jogo responsavel) | 0.8% |

> **Recomendacao:** filtrar `c_category = 'real_user'` para analises de negocio. Manter fraud/closed para analises de risco.

---

## 8. Como Rodar

```bash
# Local — apenas CSV (sem gravar no banco)
python pipelines/pcr_pipeline.py --dry-run

# Local — com gravacao no Super Nova DB
python pipelines/pcr_pipeline.py

# EC2 — deploy completo
# 1. SCP dos arquivos
scp -i ~/Downloads/etl-key.pem pipelines/pcr_pipeline.py ec2-user@<IP>:/home/ec2-user/multibet/pipelines/
scp -i ~/Downloads/etl-key.pem ec2_deploy/run_pcr_pipeline.sh ec2-user@<IP>:/home/ec2-user/multibet/

# 2. Rodar deploy script na EC2
bash ec2_deploy/deploy_pcr_pipeline.sh
```

---

## 9. Relacao com a Matriz de Risco v2

O PCR e a Matriz de Risco sao complementares:

| Dimensao | PCR | Matriz de Risco v2 |
|---|---|---|
| **Foco** | Valor do jogador (quanto vale) | Comportamento (como joga) |
| **Metodo** | Score numerico (PVS 0-100) | 21 tags booleans com pesos |
| **Saida** | Rating E-S | Tier (Muito Bom a Muito Ruim) |
| **Tabela** | `multibet.pcr_ratings` | `multibet.risk_tags` |
| **View** | `multibet.pcr_atual` | `multibet.matriz_risco` |
| **Cron** | 03:30 BRT | 02:00 BRT |

**Uso combinado (recomendado):**
```sql
SELECT
    p.player_id,
    p.rating AS pcr_rating,
    p.pvs,
    r.score_norm AS risk_score,
    r.classificacao AS risk_tier,
    r.tags
FROM multibet.pcr_atual p
LEFT JOIN multibet.matriz_risco r ON p.player_id = r.player_id
WHERE p.c_category = 'real_user'
ORDER BY p.pvs DESC;
```

---

## 10. Roadmap de Evolucao

### Pendencias atuais (P0)
- [ ] Validar `ecr_status` no dim_user (valores uteis: active/closed/suspended)
- [ ] Deploy na EC2 (scripts prontos, aguardando validacao do Head)

### Evolucoes planejadas
- **PRS (Player Risk Score):** 6 dimensoes de risco (velocidade saque, bonus abuse, padroes suspeitos)
- **Notch System:** ajuste fino (-2 a +2) dentro de cada rating para diferenciar jogadores no mesmo tier
- **Outlook temporal:** tendencia de 30d (subindo/estavel/caindo) baseada em media movel do PVS
- **Historico:** manter snapshots diarios para acompanhar evolucao do jogador ao longo do tempo (hoje so mantem o ultimo)

---

## 11. Entregas geradas

| Arquivo | Descricao |
|---|---|
| `reports/pcr_ratings_2026-04-14_FINAL.csv` | CSV completo (159.829 jogadores) |
| `reports/pcr_ratings_2026-04-14_legenda.txt` | Dicionario de colunas |
| `PCR_Player_Credit_Rating_v1.2.html` | Apresentacao visual (HTML) |
| `reports/pcr_resumo_2026-04-09.csv` | Resumo por rating |
