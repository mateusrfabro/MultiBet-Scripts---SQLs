# Deploy Isolado — Views Gold Casino & Sportsbook v4.1

**Status:** PRONTO, aguardando validação do Gusta antes de subir pra EC2.

## O que este deploy faz

Automatiza o refresh diario das 8 views gold do SuperNova DB consumidas
pelo front-end (Gusta) — aba Casino + Sportsbook + Overview do dashboard.

Sem este deploy, as silver tables ficam **congeladas no timestamp da ultima
execucao manual** e o dashboard mostra dados parados. As views gold (`vw_*`)
sao `CREATE VIEW` puras no PostgreSQL — elas buscam dados na hora, sem cache,
mas **dependem das silver tables (fct_*, fact_*) estarem atualizadas**.
As silvers sao populadas por pipelines Python `TRUNCATE + INSERT`, e esses
pipelines precisam de cron.

## Principios do deploy

**ISOLAMENTO TOTAL de aplicacoes existentes na EC2:**

1. **Pasta propria:** `/home/ec2-user/multibet/views_casino_sportsbook/`
   — zero sobreposicao com arquivos da raiz ou outros deploys
2. **Reutiliza** (sem modificar):
   - `/home/ec2-user/multibet/venv/` (venv Python)
   - `/home/ec2-user/multibet/.env` (credenciais Athena + Super Nova)
   - `/home/ec2-user/multibet/bigquery_credentials.json` (se necessario)
   - `/home/ec2-user/multibet/bastion-analytics-key.pem` (tunnel Super Nova)
3. **Crontab APPEND-ONLY:** faz backup do crontab atual antes, adiciona
   entries novas no fim, jamais edita ou remove entries existentes
4. **Logs separados:** `views_casino_sportsbook/logs/` (nao polui
   `/home/ec2-user/multibet/pipelines/logs/` dos pipelines legados)
5. **Horario escolhido:** 04:30 BRT (07:30 UTC) — janela livre:
   - Depois de `grandes_ganhos.sh` (00:30 BRT)
   - Depois de `sync_all.py` (~02:00-04:00 BRT)
   - Antes do expediente (09:00 BRT)
   - Nao conflita com `etl_aquisicao_trafego` (hourly minuto :10)

## Arquivos

```
views_casino_sportsbook/
├── README.md                              ← este arquivo
├── deploy_views_casino_sportsbook.sh      ← script de deploy (executar na EC2)
├── rollback_views_casino_sportsbook.sh    ← script de rollback
├── run_views_casino_sportsbook.sh         ← script de execucao (chamado pelo cron)
└── pipelines/
    ├── fct_casino_activity.py             ← silver casino (fund_ec2)
    ├── fct_sports_activity.py             ← silver sports (fund_ec2) — qty_bets v4
    ├── fact_casino_rounds.py              ← silver jogos casino (bireports v4.1)
    ├── fact_sports_bets_by_sport.py       ← silver sports por esporte (vendor_ec2)
    ├── fact_sports_bets.py                ← silver sports + open bets (cap odds v4)
    ├── fct_active_players_by_period.py    ← tabela active players NOVA v4
    └── create_views_casino_sportsbook.py  ← recria 8 views gold
```

## Pre-requisitos na EC2

Antes de deployar, verificar que existem na raiz `/home/ec2-user/multibet/`:
- `venv/bin/python3` funcional
- `.env` com credenciais Athena + Super Nova
- `bastion-analytics-key.pem` (chmod 600)
- Pasta `db/` com modulos `athena.py` e `supernova.py`

Se faltar qualquer um, o smoke test do deploy vai falhar claramente.

## Como deployar (APOS validacao do Gusta)

```bash
# 1. Do PC local, copiar esta pasta inteira pra EC2
scp -i etl-key.pem -r \
    "c:/Users/NITRO/OneDrive - PGX/MultiBet/ec2_deploy/views_casino_sportsbook" \
    ec2-user@54.197.63.138:/home/ec2-user/multibet/

# 2. SSH na EC2
ssh -i etl-key.pem ec2-user@54.197.63.138

# 3. Rodar o deploy (inclui smoke test + adicao ao crontab)
cd /home/ec2-user/multibet/views_casino_sportsbook
chmod +x deploy_views_casino_sportsbook.sh run_views_casino_sportsbook.sh rollback_views_casino_sportsbook.sh
bash deploy_views_casino_sportsbook.sh

# 4. Validar
crontab -l | tail -10
tail -f logs/views_$(date +%Y-%m-%d).log
```

## Como fazer rollback (se der problema)

```bash
cd /home/ec2-user/multibet/views_casino_sportsbook
bash rollback_views_casino_sportsbook.sh

# Para deletar a pasta tambem:
bash rollback_views_casino_sportsbook.sh --purge
```

O rollback:
- Remove entries deste deploy do crontab (preserva outras entries)
- Mantem pasta e logs por seguranca (use `--purge` se quiser apagar)
- NAO toca nas silver tables nem views gold — elas ficam com os dados
  da ultima execucao (front continua funcionando, so nao atualiza mais)

## Validacao pos-deploy

```bash
# Crontab nao foi quebrado
crontab -l  # conferir que entries antigas permanecem + novas foram adicionadas

# Views gold respondendo
ssh-tunnel-ou-comando-equivalente
psql ...
SELECT MAX(dt) FROM multibet.vw_casino_kpis;         -- deve ser D-0 ou D-1
SELECT MAX(dt) FROM multibet.vw_sportsbook_kpis;     -- deve ser D-0 ou D-1
SELECT period, product, unique_players, refreshed_at
FROM multibet.vw_active_players_period LIMIT 18;     -- refreshed_at deve ser de hoje
```

## Ordem de execucao (cron)

Tudo em um script `run_views_casino_sportsbook.sh` que encadeia os 7 pipelines:

1. `fct_casino_activity` (~30s) — silver casino diaria
2. `fct_sports_activity` (~60s) — silver sports diaria (inclui `qty_bets` via SB_BUYIN)
3. `fact_casino_rounds` (~5min) — silver casino por jogo (catalogo bireports v4.1)
4. `fact_sports_bets_by_sport` (~3min) — silver sports por esporte
5. `fact_sports_bets` (~6min) — silver sports agregada + open bets (cap odds v4)
6. `fct_active_players_by_period` (~2min) — tabela 18 linhas (players unicos)
7. `create_views_casino_sportsbook` (~10s) — recria DDL das 8 views (idempotente)

**Tempo total:** ~18 minutos. `set -e` no script garante que se qualquer
pipeline falhar, os seguintes nao rodam (fail-fast).

## Horario do cron

**04:30 BRT (07:30 UTC) diariamente** — `30 7 * * *`

Por que 04:30:
- Depois dos pipelines existentes (grandes_ganhos, sync_all, sync_all_aquisicao)
- Antes do expediente brasileiro (dashboard pronto as 09:00)
- Minuto :30 nao conflita com etl_aquisicao_trafego (minuto :10)

Se quiser mudar, editar a linha `CRON_ENTRY=` no `deploy_views_casino_sportsbook.sh`.

## Versionamento

- **v4.0 (10/04/2026):** 3 bloqueadores + 3 gaps resolvidos (Opcao 2 + Opcao 3)
- **v4.1 (10/04/2026):** category no fact_casino_rounds corrigido (bireports
  direto, sem fallback ps_bi.dim_game), deploy automatizado EC2

## Rollback plan resumido

Se qualquer coisa der errado apos deploy:

1. `crontab -l | grep VIEWS_CASINO_SPORTSBOOK_V4` — ver entries ativas
2. `bash rollback_views_casino_sportsbook.sh` — remover entries
3. Verificar que entries antigas (grandes_ganhos, sync_all etc) permanecem
4. Dashboard continua funcional com dados da ultima execucao ate reexecucao manual

Zero impacto em aplicacoes existentes (por design).
