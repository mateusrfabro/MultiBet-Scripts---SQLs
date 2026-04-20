# Proposta: PCR_RATING_NEW para jogadores novatos

**Data:** 2026-04-20
**Autor:** Mateus Fabro (com auditoria de `pipelines/pcr_pipeline.py`)
**Stakeholders de decisao:** Raphael (CRM Leader), Castrin (Head de Dados)
**Tempo estimado de reuniao:** 15 min
**Status:** aguardando aprovacao — **NAO deployar** PCR v2 sem decisao desta proposta

---

## Contexto

O PCR (Player Credit Rating) classifica jogadores em 6 tiers `S`/`A`/`B`/`C`/`D`/`E` baseado no PVS (Player Value Score), uma formula composta de:
- **Valor:** GGR, depositos totais, ticket medio
- **Risco:** margem GGR, razao bonus/deposito
- **Outlook:** taxa de atividade, recencia

O PCR hoje aceita **qualquer jogador com 1+ evento** (1 aposta OU 1 deposito OU 1 rodada) nos ultimos 90 dias.

## Problema identificado

A formula PVS usa **ratios** que sao estatisticamente instaveis com amostra pequena:

| Ratio | Exemplo casual (1 evento) | Problema |
|---|---|---|
| `margem_ggr = GGR / turnover_total` | 1 rodada R$ 100, win R$ 5 = margem 0.95 | 1 observacao nao define margem |
| `bonus_ratio = bonus_issued / total_deposits` | Bonus boas-vindas R$ 50, deposito R$ 10 = 5.0 | Penalidade maxima (-10) pra novato normal |
| `taxa_atividade = days_active / 90` | 1 dia ativo / 90 = 0.011 | Score 1.1 (ultimo percentil) pra quem se cadastrou ontem |

**Consequencia:** jogadores recem-chegados (FTDs recentes, que sao exatamente o publico-alvo de CRM de reativacao/boas-vindas) caem automaticamente no **rating E** por construcao matematica, sem refletir o comportamento real deles.

## Impacto atual (estimado)

- ~6.000 FTDs/mes caem direto em `PCR_RATING_E`
- Smartico dispara campanha de "reativacao de engajamento minimo" (mensagem: "volte a jogar!")
- Cliente novo recebe mensagem de reativacao quando deveria receber **onboarding/boas-vindas**
- Perda de experiencia + possivel churn precoce

## Proposta tecnica

### Threshold (logica OU)

Jogador entra em **PCR_RATING_NEW** se satisfaz qualquer uma das condicoes:
- `days_active < 14` (menos de 2 semanas de atividade no periodo)
- `num_deposits < 3` (menos de 3 depositos totais)

### Justificativa dos cortes

- **14 dias:** periodo suficiente pra estabilizacao de padrao semanal (1-2 ciclos de cashout/deposit) + alinhado com SLA comum de onboarding no iGaming
- **3 depositos:** minimo pra ter media/mediana de deposito com algum signal (2 pontos sao insuficientes pra detectar variancia)
- **OU logica:** jogador com 1 deposito gigante em 1 dia nao deveria ser classificado junto com whales, e whale com 30 depositos em 10 dias tambem precisa de maturacao pra ranking

### Implementacao

```python
# pipelines/pcr_pipeline.py — separar novatos antes do ranking PVS
df_novos = df[
    (df["days_active"] < 14) | (df["num_deposits"] < 3)
].copy()
df_novos["rating"] = "NEW"
df_novos["pvs"] = None  # ou NaN, sem ranking percentil

df_maduros = df[~df.index.isin(df_novos.index)].copy()
# calcular PVS e atribuir S/A/B/C/D/E apenas em df_maduros
```

### Mapping Smartico

```python
# scripts/push_pcr_to_smartico.py
RATING_TO_SMARTICO = {
    "S": "PCR_RATING_S",
    "A": "PCR_RATING_A",
    "B": "PCR_RATING_B",
    "C": "PCR_RATING_C",
    "D": "PCR_RATING_D",
    "E": "PCR_RATING_E",
    "NEW": "PCR_RATING_NEW",  # <-- ADICIONAR
}
```

### Validacao antes do rollout

1. Rodar o pipeline em shadow mode por 3 dias (salvar na tabela mas NAO pushar)
2. Validar distribuicao: esperado ~10-20% da base em NEW (FTDs do ultimo mes + cohort parcial)
3. Conferir que percentis do PVS de maduros se mantem estaveis apos exclusao (nao deve mudar muito — novatos eram minoria distorcendo a cauda)
4. Comparar tags anteriores de ate 100 jogadores — quem era E e virou NEW? Quem se manteve E?

## Decisao requerida

| Quem | O que precisa decidir |
|---|---|
| **Raphael (CRM)** | (a) Aprovar criacao da tag `PCR_RATING_NEW` no Smartico; (b) Mapear a tag para uma jornada de boas-vindas (diferente da jornada E de reativacao); (c) Informar se quer thresholds diferentes dos sugeridos (14d/3dep) |
| **Castrin (Head)** | (a) Aprovar exclusao dos novatos do ranking PVS (afeta relatorios de distribuicao de tier); (b) Decidir se `PCR_RATING_NEW` conta no reporting de "cobertura do CRM" ou fica em bucket separado |

## Cronograma proposto

- **D+0:** reuniao 15 min (Raphael + Castrin + eu)
- **D+1:** se aprovado, implementar em pipeline (~30 min de codigo)
- **D+1 a D+3:** rodar shadow mode, validar distribuicao
- **D+4:** ativar push Smartico com tag NEW
- **D+7:** review de impacto com Raphael (quantos novos caem na jornada, CTR comparado ao pre-mudanca)

## O que acontece se nao aprovar

Manter comportamento atual: FTD novo continua recebendo `PCR_RATING_E` e campanha de reativacao. Isso e: (a) experiencia ruim do cliente novo, (b) CTR menor na campanha (publico errado pra mensagem errada), (c) subutilizacao de jornada de onboarding do Smartico. Nao e compliance/regulatorio, mas e perda mensuravel de ativacao.

## Arquivos relacionados

- Pipeline: `pipelines/pcr_pipeline.py` (linhas 196-199 HAVING + 297-307 formula PVS)
- Push CRM: `scripts/push_pcr_to_smartico.py` (linha 81 mapping)
- Doc conceitual: `docs/pcr_player_credit_rating.md`
- Auditoria origem desta proposta: `docs/auditoria_sql_pcr_20260420.md` secao 7.1
