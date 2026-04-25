# Log de Entregas — `reports/`

Append-only. Uma linha por entrega final ao stakeholder.
Formato: `YYYY-MM-DD HH:MM | demandante | demanda | arquivo principal | status`

> Status: `entregue` | `pendente_validacao` | `archived`

## 2026-04

```
2026-04-25 11:19 | (validacao tecnica)  | teste affiliate-daily 464673 (gap-resistant via bireports) | affiliate_daily_464673_2026-04-23.csv               | entregue
2026-04-25 09:30 | Rapha (CRM)          | base lifetime affiliate 363722 (Pri Simoes) com Smartico  | affiliate_363722_pri_simoes_base_players_FINAL.csv  | entregue
2026-04-24 17:25 | (validacao tecnica)  | teste CLI affiliate-daily 464673 06/04                    | affiliate_daily_464673_2026-04-06.csv                | entregue
```

## 2026-03 (e anteriores) — historico

Entregas anteriores nao foram registradas neste log na epoca. Para arqueologia,
usar `git log --diff-filter=A -- reports/` (busca por arquivo adicionado).

---

## Como adicionar uma entrega ao log

Apos rodar uma extracao para stakeholder, adicione linha como:

```
YYYY-MM-DD HH:MM | <demandante> | <demanda 1 frase> | <arquivo principal> | entregue
```

**Demandantes recorrentes:**
- Rapha (CRM Leader)
- Dudu (Head Trafego)
- Castrin (Head Dados)
- Mauro (Sr Analytics)
- Gusta (Sr Infra)
- Rafael Conson (CGO)
- Gabriel Barbosa (CTO)

**Use "(validacao tecnica)"** quando for teste interno do squad — nao foi pedido por stakeholder.

## Por que existe este log

Sem isso, em 6 meses ninguem sabe pra quem foi cada CSV de `reports/`.
Quando um stakeholder pergunta "voce me mandou aquela base?", consulta-se aqui.

Custo: 5 segundos por entrega. Beneficio: rastreabilidade total.
