# Segmentação Diária A+S — Base Operacional para CRM

> Base diária com os jogadores de **maior valor** da operação (top 8% — ratings A e S do PCR), enriquecida com sinais de comportamento, risco e ciclo de vida, para o time de CRM operacionalizar **régua de bônus, retenção e ações segmentadas**.

---

## O que é

A Segmentação A+S é uma **lista diária e automática** dos jogadores de elite da base — aqueles classificados como **Whale (S)** ou **VIP (A)** pelo modelo de credit rating de jogadores (PCR).

Cada linha representa um jogador, e cada coluna entrega um sinal acionável: quanto ele valeu nos últimos 30/90 dias, em que momento do ciclo de vida está, qual jogo prefere, em qual horário joga, se depende de bônus, se tem perfil de abuso, se está em alguma restrição regulatória.

**Para que serve:** o time de CRM usa essa base para tomar decisões diárias — quem priorizar, quem reativar, quem reduzir bônus, quem chamar para um upgrade de tier, quem encaminhar para o atendimento dedicado.

Ela responde à pergunta: **"Quem são meus melhores jogadores hoje, e o que cada um precisa receber?"**

---

## Como funciona

```
1. Roda toda madrugada, logo após o cálculo do PCR (snapshot D-1).
2. Filtra apenas os jogadores com rating A ou S (~8% da base ativa).
3. Cruza com a Matriz de Risco Comportamental (qualidade do jogo).
4. Calcula a coluna 'tendência' (estável, subindo de tier, caindo de tier).
5. Enriquece com sinais de 30 dias, top jogos por tier, ciclo de vida e KYC.
6. Persiste o snapshot histórico em banco relacional.
7. Publica o CSV final + legenda explicativa.
8. Envia por e-mail aos donos das ações de CRM.
9. Publica tags operacionais no perfil do jogador no CRM, prontas para uso em régua de campanhas.
```

A entrega é **idempotente** (rodar duas vezes no mesmo dia gera o mesmo resultado) e **incremental** (nunca sobrescreve histórico — guarda todos os snapshots para auditoria e séries temporais).

---

## Estrutura da entrega

A base diária tem três blocos de informação por jogador:

### Bloco 1 — Identificação e classificação
Quem é o jogador, qual rating tem, em que tier comportamental cai, qual o status atual da conta (ativa, fechada, em pausa de Jogo Responsável).

### Bloco 2 — Métricas de valor e atividade
Quanto trouxe de receita (GGR/NGR), quanto depositou e sacou, quantas apostas fez, qual ticket médio — em duas janelas: **últimos 30 dias** (sinal recente, gatilho operacional) e **últimos 90 dias** (foto consolidada, baseline de tier).

### Bloco 3 — Comportamento e contexto
Em que ciclo de vida está (ativo, em risco de churn, dormente), qual produto prefere (cassino, esportes, misto), em qual dia/horário costuma jogar, top jogos do segmento dele, dependência de bônus, sinais de abuso e nível de KYC.

---

## Glossário das principais colunas

| Coluna | O que mede | Por que importa |
| --- | --- | --- |
| **Rating** | Classificação do jogador (S = top 1% / Whale; A = top 7% / VIP) | Define o tier que decide a régua de CRM |
| **PVS** | Player Value Score (0 a 100) | Posição exata do jogador dentro do tier — permite ranquear A's "quase S" |
| **Tendência** | Estável / Subindo / Caindo | Sinaliza A's prestes a virar S (oportunidade de upgrade) ou S's perdendo gás (risco de queda) |
| **Tier de Risco Comportamental** | Muito Bom / Bom / Mediano / Ruim / Muito Ruim | Mede a *qualidade* do jogo (ortogonal ao valor) — um S pode ser "Ruim" se for bonus-chaser |
| **Status de Conta** | Ativa / Fechada / Pausa Jogo Responsável / Fraude | Filtro operacional — quem pode receber bônus, quem está bloqueado, quem está suspenso temporariamente |
| **Status de Ciclo de Vida** | Novo / Ativo / Em Risco / Churned / Dormente | Quando agir — ativo recente recebe ação diferente de quem sumiu há 60 dias |
| **GGR / NGR (30d e 90d)** | Receita bruta e líquida gerada | Valor financeiro produzido — base para ROI por ação de CRM |
| **Volume de depósitos / saques (30d e 90d)** | Quanto o jogador movimentou | Indicador de saúde financeira da conta |
| **Ticket médio de depósito** | Valor médio por depósito | Ajuda a calibrar valor de bônus (não dá bônus de R$ 500 para quem deposita R$ 50) |
| **Top jogos / Top providers do tier** | Jogos e fornecedores favoritos do segmento | Insumo para ações temáticas (free spins do jogo certo, cashback no provider certo) |
| **Dia/Horário dominante** | Quando o tier costuma jogar | Insumo para timing de campanhas (push, e-mail, SMS) |
| **Sinal de abuso de bônus** | 1 se o comportamento aponta abusador / promo-chaser | Trava CRM agressivo em quem estraga o ROI de bônus |
| **Nível de KYC** | KYC 0 a KYC 3 | Limita ações regulamentadas (saque, depósito, valor de bônus) |
| **Restrições regulatórias** | Auto-exclusão / pausa cool-off / produto bloqueado | Filtros obrigatórios — não envia oferta para quem está em Jogo Responsável |
| **BTR (Bonus Turnover Ratio)** | Quanto do bônus virou dinheiro real (turnover) | Mede eficiência do bônus — BTR alto significa bônus rolando, BTR baixo significa bônus parado |

---

## Janelas de tempo

Toda métrica numérica (GGR, depósito, etc.) é calculada em **janelas rolling fixas**:

- **30 dias rolling** — termina em **D-1** (exclui o dia parcial). Janela de **gatilho operacional** — sinal recente para CRM agir hoje.
- **90 dias rolling** — também termina em **D-1**. Janela de **baseline** — é o que define o rating do PCR e a maturidade do jogador.

Por que rolling e não calendário (mês fechado)? Porque CRM opera diariamente — precisa de sinal contínuo, não esperar o mês fechar. E porque o PCR já trabalha em 90d rolling, então as janelas casam.

A escolha de **D-1** (excluir o dia atual) é deliberada: o dia em curso é sempre parcial (algumas horas de atividade) e injetaria volatilidade no sinal. Truncar em D-1 dá uma foto estável.

---

## Como o CRM usa

O fluxo típico de uso é:

1. **De manhã**, o operador recebe o CSV no e-mail.
2. **Filtra** por critério da ação (ex: "S em tendência Caindo, status ativo, sem abuso de bônus, ciclo Em Risco").
3. **Define a ação** (ex: bônus de reengajamento de R$ 100 com BTR esperado de 5x).
4. **Importa a lista** no sistema de campanhas (CRM operacional).
5. **Mede o resultado** no fim da janela de campanha — comparando com o snapshot do dia da ação.

A coluna `Tendência` é o gatilho mais valioso: identifica os ~5% da base que estão **prestes a mudar de tier** — exatamente onde CRM tem mais alavanca.

---

## Roadmap de evolução

A base evolui em **três versões progressivas**, cada uma adicionando colunas conforme entram em produção:

### v2.1 — Foco em estado e risco regulatório

Adiciona o ciclo de vida do jogador (novo / ativo / em risco / churned / dormente), os flags de status regulatório (auto-exclusão, restrição de produto, nível de KYC) e o sinal de abuso de bônus derivado da Matriz de Risco.

**Ganho:** o operador passa a saber, na linha do jogador, **se pode agir** (filtro regulatório) e **em que momento ele está** (filtro de ciclo).

### v2.2 — Foco em valor recente e ticket

Adiciona as métricas de **30 dias** (GGR, NGR, depósito, saque, aposta — em volume e contagem) e os tickets médios — tanto do próprio jogador quanto a média do tier dele.

**Ganho:** o operador para de olhar só o consolidado de 90d e passa a ver o **sinal recente**, o que muda totalmente a leitura — um S que veio caindo nos últimos 30d demanda ação diferente de um S estável.

### v2.3 — Foco em personalização e timing

Adiciona top jogos e top providers do tier, dia e horário dominantes, último produto jogado, e métricas de bônus expandidas (BTR por vertical, último bônus emitido, dependência de bônus lifetime).

**Ganho:** as ações deixam de ser genéricas (push às 19h, free spin no jogo da moda) e passam a ser **calibradas pelo perfil do tier** (push no horário onde aquele tier joga mais, free spin no provider que ele realmente usa).

---

## Decisões metodológicas

### Por que rating A e S apenas (e não a base toda)?

Porque ações de CRM têm **custo operacional** (bônus, equipe de relacionamento, infraestrutura de campanha). Concentrar esforço nos top 8% da base — que respondem por **>60% da receita** — é a alocação ótima de orçamento. Os tiers B/C/D/E são tratados em campanhas massivas com lógica diferente (broadcast, automações, missões).

### Por que manter contas fechadas e em pausa de Jogo Responsável na base?

Porque o operador precisa **ver a coluna `Status de Conta` para decidir o que fazer** — não para fingir que esses jogadores não existem. Se filtrássemos antes, alguém poderia montar uma campanha sem perceber que está mirando em quem não pode receber. **A base mostra tudo, e o filtro fica explícito na operação.**

### Por que duas janelas (30d e 90d)?

Porque servem a propósitos diferentes:

- **90d** = baseline. Estável, foto consolidada, define o rating.
- **30d** = gatilho. Recente, captura mudança, ativa a ação.

Ter as duas permite distinguir um jogador "S consolidado em queda recente" de um "S consolidado estável" — e tratá-los como casos distintos.

### Por que cruzar com Matriz de Risco Comportamental?

Porque **valor (PCR) e risco (Matriz) são ortogonais.** Um jogador pode ter alto valor (S) e ainda assim apresentar comportamento problemático (promo chainer, abuso de bônus). Ignorar a Matriz faria o CRM gastar bônus em quem destrói o ROI da operação.

O cruzamento gera uma matriz **Rating × Tier de Risco** com células específicas. Algumas células (ex: S × Muito Bom) recebem investimento agressivo. Outras (ex: A × Ruim) viram caso de auditoria — não de campanha.

---

## Garantias operacionais

A entrega tem três camadas de segurança automatizadas:

1. **Idempotência diária** — o snapshot do dia é único; rodar duas vezes não duplica linha nem altera histórico.
2. **Histórico preservado** — todos os snapshots passados ficam no banco para análise temporal e rollback de campanha.
3. **Monitor de células** — se uma célula (ex: B × Muito Ruim) ficar 3 dias seguidos com **NGR negativo**, dispara alerta para auditoria — significa que a célula está consumindo bônus sem retorno e algo precisa ser revisto.

### Cobertura medida em ambiente real

| Sinal | Cobertura | Observação |
|---|---|---|
| KYC do jogador | 100% | Todos os jogadores ativos têm registro de nível KYC. |
| Tier de risco comportamental | 95–98% | Quem não casou cai em "Não Identificado" (jogador novo ou fora da janela). |
| Top jogos do tier | ~88% | Alguns IDs antigos do catálogo de jogos ficam sem nome legível e aparecem como código — a operação reconhece e segue. |
| Métricas de 30 dias | 65–70% | Reflete naturalmente o % de jogadores A/S que tiveram atividade no último mês — os demais entram com zero (esperado, não é bug). |
| Sinal de abuso de bônus | 100% | Todos os jogadores recebem 0 ou 1 (regra binária objetiva). |

---

## O que NÃO está na base (e por quê)

- **Dados pessoais (nome, e-mail, telefone)** — fora do escopo. A base usa o **ID do jogador no CRM** para o operador casar no sistema de campanhas, sem expor PII.
- **LTV projetado** — cálculo separado, vive em outra entrega.
- **Atribuição de afiliado por canal** — vive na base de tráfego/affiliates.
- **Saldos e contas correntes em tempo real** — a base é diária, não real-time.

---

## Resumo

A Segmentação A+S é a **base de trabalho diária do CRM** — tudo que ele precisa saber sobre os 8% mais valiosos da operação, em uma única lista, com janelas estáveis, regras claras e histórico auditável.

A versão final entrega **57 colunas** combinando rating, valor, comportamento, ciclo, jogos preferidos, status regulatório e sinais de risco — pronta para virar campanha, régua, ou decisão estratégica do head de operação.
