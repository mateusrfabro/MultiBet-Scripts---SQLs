# Matriz de Risco — Classificacao Comportamental de Jogadores

> Modelo de scoring que classifica jogadores em 5 niveis de saude/risco com base em 21 comportamentos observados nos dados transacionais da plataforma.

---

## O que e a Matriz de Risco?

A Matriz de Risco e um sistema automatizado que analisa o comportamento de cada jogador ativo e atribui uma **classificacao de risco** baseada em dados reais da operacao.

Ela responde a pergunta: **"Este jogador e saudavel para a operacao ou representa risco?"**

O modelo roda diariamente sobre os dados dos ultimos 90 dias, analisando depositos, saques, apostas, uso de bonus e padroes de sessao.

---

## Como funciona

```
1. Coleta dados transacionais dos ultimos 90 dias
2. Avalia 21 comportamentos (tags) para cada jogador
3. Soma os pontos de cada tag ativa (score bruto)
4. Normaliza para escala de 0 a 100
5. Classifica em 5 niveis (tiers)
```

---

## Classificacoes (Tiers)

| Classificacao | Score | O que significa | Acao recomendada |
| --- | --- | --- | --- |
| **Muito Bom** | Acima de 75 | Jogador legitimo, alta frequencia e valor | Retencao premium, pipeline VIP |
| **Bom** | 51 a 75 | Jogador ativo e saudavel | Manter elegibilidade a promocoes |
| **Mediano** | 26 a 50 | Comportamento misto (positivo e negativo) | Monitoramento, reengajamento |
| **Ruim** | 11 a 25 | Dependente de promocoes, sinais de risco | Restringir bonus, monitorar |
| **Muito Ruim** | 10 ou menos | Multiplos comportamentos de risco | Encaminhar para Compliance, suspender bonus |
| **Sem Score** | -- | Atividade insuficiente para classificar | Acompanhar |

---

## As 21 Tags Comportamentais

Cada tag e um comportamento identificado automaticamente nos dados. Tags positivas indicam jogador saudavel, tags negativas indicam risco.

### Tags Positivas (indicam jogador saudavel)

| Tag | Pontos | O que identifica |
| --- | --- | --- |
| VIP Whale | +30 | Jogador de altissimo valor (GGR elevado + alta frequencia) |
| Reengajado | +30 | Voltou apos 30+ dias inativo e manteve engajamento |
| Winback Alto Valor | +25 | Reativado com GGR expressivo |
| Sustentado | +15 | Continua jogando apos sacar (nao "saca e foge") |
| Reinvestidor | +15 | Saca e deposita novamente em ate 7 dias |
| Depositante Regular | +10 | Deposita regularmente (3+ vezes por mes) |
| Organico (sem bonus) | +10 | Deposita sem usar bonus |
| Ativo sem Promo | +10 | Ativo na ultima semana sem usar promocao |
| Engajado | +10 | 3 a 10 sessoes por dia (nivel saudavel) |
| Slot Player | +5 | Focado em slots com deposito (perfil casino) |
| Sazonal | +5 | Jogador que aparece em eventos/promocoes |
| Alerta RG | +1 | 10+ sessoes/dia — sinal de jogo responsavel |

### Tag Neutra

| Tag | Pontos | O que identifica |
| --- | --- | --- |
| Zero Risco | 0 | Valor de saque proximo ao de deposito (conservador) |

### Tags Negativas (sinais de risco)

| Tag | Pontos | O que identifica |
| --- | --- | --- |
| Saque Rapido | -25 | Deposita e saca em menos de 1 hora |
| Cashout & Run | -25 | Usa bonus, saca e desaparece por 48h+ |
| So Promocao | -15 | So deposita quando tem promocao (80%+) |
| Rollback Alto | -15 | Taxa de cancelamentos acima de 10% |
| Encadeador de Promos | -10 | Encadeia promocoes sem jogo organico |
| Comportamento Suspeito | -10 | Saques em horarios extremos ou valores anomalos |
| Multi-Sessao | -10 | 3+ jogos simultaneos na mesma hora (possivel bot) |
| Conta Nova | -5 | Conta com menos de 2 dias (monitoramento preventivo) |

---

## Formula de Scoring

### Score bruto

A soma dos pontos de todas as tags ativas do jogador.

**Exemplos:**
- Jogador com Depositante Regular (+10) + Engajado (+10) + Reinvestidor (+15) = **+35 pontos**
- Jogador com Saque Rapido (-25) + So Promocao (-15) = **-40 pontos**
- Jogador sem nenhuma tag = **Sem Score**

### Normalizacao (0 a 100)

O score bruto e convertido para uma escala de 0 a 100 usando a formula:

```
Score normalizado = (score bruto + 35) / 85 x 100
```

Limitado entre 0 e 100. Calibrado com percentis reais da base.

| Score bruto | Score normalizado | Classificacao |
| --- | --- | --- |
| -35 ou menos | 0 | Muito Ruim |
| -25 | 12 | Ruim |
| -10 | 29 | Mediano |
| 0 | 41 | Mediano |
| +6 (mediana tipica) | 48 | Mediano |
| +10 | 53 | Bom |
| +20 | 65 | Bom |
| +35 | 82 | Muito Bom |
| +50 ou mais | 100 | Muito Bom |

---

## Regras e Parametros

### Base de jogadores

- **Quem entra:** Jogadores com pelo menos 1 deposito confirmado OU 1 aposta realizada nos ultimos 90 dias
- **Quem nao entra:** Contas de teste, jogadores sem atividade financeira no periodo

### Filtros de qualidade

- Apenas transacoes confirmadas/efetivadas (exclui tentativas falhadas)
- Contas de teste excluidas via flag do sistema
- Timestamps em UTC, convertidos para horario local onde aplicavel

### Janela temporal

- **90 dias rolling** — cada dia a janela avanca, sempre olhando os ultimos 3 meses
- Excecoes: "Conta Nova" (-5) olha apenas os ultimos 2 dias; "Ativo sem Promo" (+10) olha os ultimos 7 dias

---

## Como cada area pode usar

### CRM e Retencao

| Uso | Como |
| --- | --- |
| Segmentacao por tier | Criar segmentos automaticos (VIP, Saudavel, Monitorar, Restrito, Investigar) |
| Micro-segmentacao | Combinar tags (ex: So Promocao + Encadeador = "Bonus Grinders") |
| Bonus proporcional | Usar score 0-100 para personalizar valor do bonus |
| Reativacao | Identificar jogadores "Reengajados" para campanhas dedicadas |

### Riscos e Compliance

| Uso | Como |
| --- | --- |
| Lista de investigacao | Filtrar tier Muito Ruim para analise manual |
| Jogo Responsavel | Monitorar jogadores com Alerta RG (sessoes excessivas) |
| Anti-fraude | Cruzar tags Saque Rapido + Cashout & Run + Rollback Alto |
| Auditoria | Snapshots historicos diarios permitem rastrear evolucao de qualquer jogador |

### Marketing

| Uso | Como |
| --- | --- |
| Exclusao de abusers | Nao enviar promos para "So Promocao" e "Encadeador de Promos" |
| Campanhas sazonais | Direcionar para jogadores "Sazonais" em eventos/feriados |
| Programa organico | Reconhecer jogadores "Organico (sem bonus)" com beneficios exclusivos |

---

## Exemplos Praticos

### Jogador "Muito Bom" (score 82)

> Deposita regularmente (+10), saca e reinveste (+15), nao usa bonus (+10), engajado com 5 sessoes/dia (+10), GGR alto (+30), joga slots (+5). Total bruto: +80. Score normalizado: 82.

**Acao:** Retencao VIP, cashback premium, account manager.

### Jogador "Mediano" (score 47)

> Deposita regularmente (+10), mas so deposita em dias de promo (-15), usa bonus e continua jogando (+15), sazonal (+5). Total bruto: +15. Score normalizado: 47.

**Acao:** Monitorar, testar reengajamento sem bonus para avaliar se joga organicamente.

### Jogador "Muito Ruim" (score 6)

> Depositou e sacou em 30 minutos (-25), usou bonus e sumiu (-25), so deposita em promos (-15), encadeia promos (-10). Total bruto: -75. Score normalizado: 0.

**Acao:** Suspender bonus, encaminhar para Compliance, investigar possivel lavagem.

---

## Evolucao Planejada

| Fase | O que muda |
| --- | --- |
| **v2.1** | Adicionar tags especificas de apostas esportivas |
| **v2.2** | Adicionar tags de fraude avancada (velocidade de transacoes, abuso de free spins) |
| **v2.3** | Integracao automatica com CRM via API |
| **v3.0** | Score com suavizacao temporal (media movel) + scoring gradual por intensidade |

---

## Especificacoes Tecnicas (resumo)

| Item | Valor |
| --- | --- |
| **Atualizacao** | Diaria (automatizada) |
| **Janela de analise** | 90 dias (rolling) |
| **Numero de tags** | 21 (12 positivas, 8 negativas, 1 neutra) |
| **Escala do score** | 0 a 100 |
| **Numero de tiers** | 5 + Sem Score |
| **Fonte de dados** | Dados transacionais da plataforma (data lake) |
| **Persistencia** | Banco de dados relacional + CSV diario |
| **Historico** | Snapshots diarios preservados para auditoria |

---

> **Desenvolvido por:** Squad Intelligence Engine
> **Ultima atualizacao:** Abril 2026
