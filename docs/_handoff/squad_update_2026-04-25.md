# Squad Update — 25/04/2026 (sabado)

> Resumo da sessao tecnica de organizacao + achado critico para o squad.
> Mensagem pronta para postar no canal/grupo (ajustar tom conforme audiencia).

---

## Versao curta (WhatsApp/Slack)

```
Pessoal, organizei nossa estrutura interna no fim de semana e achei um bug:

1. ACHADO CRITICO: ps_bi.fct_player_activity_daily esta com gap de 18 dias
   (parou em 06/04). Reports diarios que usam essa tabela estao zerados ou
   incompletos desde entao. Gusta/Mauro, voces tem fonte canonica
   alternativa rodando? Migrei nosso pipeline pra bireports_ec2 enquanto isso.

2. CLI nova interna (snova_cli) — proxima demanda do Rapha sai em 2 min com
   auditor automatico. Comandos: affiliate-base, affiliate-daily.
   Plano de adicionar mais conforme aparecerem demandas recorrentes.

3. Memory/docs reorganizado — quem precisar de schema/regra de SQL,
   abrir memory/INDEX_*.md ou docs/INDEX.md. MEMORY.md tinha 239 linhas
   (truncava nas sessoes), agora 73 com hub + 5 indices.

4. Limpeza: 36 arquivos versionados antigos arquivados em _archive/lixeira_2026-04-25/
   (~544 MB liberados). Reversivel ate 04/05; daqui 7 dias deletamos definitivamente
   se ninguem precisar.

Detalhes em docs/_handoff/squad_update_2026-04-25.md (versao longa) e
docs/CATALOG.md (catalogo de metricas — referencia canonica metrica->tabela->campo).

Commits: 79b8e94 (snapshot pre-archive) + cf59676 (fase B archive).
```

---

## Versao longa (para Castrin / Mauro / Gusta — pessoal/email)

### Achados

**1. `ps_bi.fct_player_activity_daily` parou em 06/04/2026**

Evidencia (rodada em 25/04):

| Fonte | Ultima data | Status |
|---|---|---|
| `fund_ec2.tbl_real_fund_txn` (raw) | 23/04 — 1.6M txns/dia | OK |
| `ps_bi.dim_user` (DIM) | 24/04 — 677 cadastros hoje | OK |
| **`ps_bi.fct_player_activity_daily` (FATO)** | **06/04** | **18 dias parada** |

Diagnostico: dado bruto chega normal, dim do dbt esta atualizada, mas a fato
do dbt nao materializa desde 07/04. Job especifico do dbt parou.

Impacto:
- Qualquer dashboard/report que consome `fct_player_activity_daily` tem zeros nos ultimos 18 dias
- O `affiliate-daily` da CLI nova foi migrado para `bireports_ec2.tbl_ecr_wise_daily_bi_summary`
  (gap-resistant) enquanto o dbt nao volta

Pergunta:
- **Gusta/Mauro:** voces tem alguma view alternativa rodando que substitui essa fato?
  Se sim, qual? Quero migrar tudo pra fonte canonica.
- Se nao, podem investigar o job? Detalhe em `memory/project_dbt_fct_gap.md`.

### Trabalho feito

**A) CLI interna `snova_cli` (produtividade)**

Estrutura:
```
MultiBet/
├── cli.py                          # entry point argparse
├── db/helpers.py                   # SQL fragments reutilizaveis
├── db/auditor.py                   # AthenaAuditor (cross-check)
└── snova_cli/commands/
    ├── affiliate_base.py           # base lifetime players (CRM)
    └── affiliate_daily.py          # KPIs D-1 (REG/FTD/GGR/NGR)
```

Uso:
```bash
python cli.py affiliate-base 363722 --name "Pri Simoes"
python cli.py affiliate-daily 464673 --date 2026-04-23
```

Cada comando roda **auditor automatico** cruzando 2-3 fontes (ps_bi vs ecr_ec2 raw)
e reprova entrega se divergencia >5% / 15% conforme caso.

Roadmap aberto: cohort, ftd-report, base-sem-ftd, risk-summary.
Conforme aparecer demanda, vira CLI em ~30 min.

**B) Memory + docs reorganizados**

`MEMORY.md` (carrega automatico em toda sessao do Claude) estava com 239 linhas
e truncava (limite 200). Refatorado:
- Hub MEMORY.md em 73 linhas
- 5 sub-indices: schemas, feedbacks_criticos, feedbacks_operacionais, projetos, referencias

Novo `docs/INDEX.md` mapeia todos os 50+ docs por tema.
Novo `docs/CATALOG.md` (em construcao) — catalogo metrica -> tabela canonica
-> campo -> SQL validado -> dono. Quem decidir uma metrica, registrar la.

**C) Limpeza (squad workflow respeitado)**

Squad agents `best-practices` + `auditor` consultados antes de cada fase.
- Fase A (delete): 5 arquivos orfaos shell + 5 scripts `_tmp_*` + 6 outputs CLI test
- Fase B (archive): 20 scripts versionados + 16 reports versionados (~544 MB)
- Tudo em `_archive/lixeira_2026-04-25/` (gitignored, local). Revisar em 04/05.

### Para o time

**Castrin:** o catalogo (`docs/CATALOG.md`) pode virar artefato canonico se ajudar.
Aberto a feedback do que adicionar.

**Mauro:** validacao da formula NGR proxy (GGR - bonus_issued) vs canonica
(GGR - BTR - RCA) — qual usar nos reports diarios? Ate agora uso proxy.

**Gusta:** preciso confirmar onde o orquestrador EC2 esta hoje pra eventualmente
agendar a CLI quando virar pipeline.

**Rapha:** entrega da Pri Simoes (363722) ja foi (2278 players, csv + legenda
em `reports/`). Para proximas bases assim, posso rodar a CLI direto.

---

## Para mim (Mateus) — proxima sessao

- [ ] Reportar este resumo no canal do squad
- [ ] Pingar Gusta/Mauro especificamente sobre o gap dbt
- [ ] Validar com Castrin se NGR proxy esta OK enquanto BTR canonica nao roda
- [ ] Adicionar mais metricas em CATALOG.md quando aparecerem em demandas
- [ ] Em 04/05 (segunda) revisar lixeira (ver `memory/project_lembrete_lixeira_2026_04_25.md`)
