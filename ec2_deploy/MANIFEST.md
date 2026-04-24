# MANIFEST — Pipelines do MultiBet no Orquestrador

> Fonte única de verdade do estado esperado dos pipelines que a pasta `ec2_deploy/` empacota.
> Atualizado em 2026-04-24.

Esta pasta contém **o código + scripts de deploy** dos pipelines que alimentam o Super Nova DB e dashboards. O orquestrador (Prefect, 3.87.109.131) deve agendar estes pipelines conforme a cadência abaixo.

## Visão geral

| # | Pipeline | Cadência sugerida (UTC / BRT) | Comando | Destino | Depende de |
|---|---|---|---|---|---|
| 1 | `sync_meta_spend` | `0 9,13,17,21,1 * * *` (5x/dia — 06h, 10h, 14h, 18h, 22h BRT) | `python3 pipelines/sync_meta_spend.py --days 2` | `multibet.fact_ad_spend` (ad_source='meta') | `.env` `META_ADS_ACCESS_TOKEN`, `META_APP_ID`, `META_APP_SECRET`, `META_ADS_ACCOUNT_IDS` |
| 2 | `sync_google_ads_spend` | `0 9,13,17,21,1 * * *` (5x/dia) | `python3 pipelines/sync_google_ads_spend.py --days 3` | `multibet.fact_ad_spend` (ad_source='google_ads') | `.env` `GOOGLE_ADS_*` |
| 3 | `refresh_meta_token` | `0 5 1 * *` (dia 1 de cada mês, 02:00 BRT) | `python3 pipelines/refresh_meta_token.py` | reescreve `META_ADS_ACCESS_TOKEN` no `.env` | `META_APP_ID` + `META_APP_SECRET` |
| 4 | `grandes_ganhos` | `30 3 * * *` (00:30 BRT) | `python3 pipelines/grandes_ganhos.py` | `multibet.grandes_ganhos` (Top 50 wins) | Athena, `ecr_ec2`, `fund_ec2` |
| 5 | `push_risk_to_smartico` | `30 5 * * *` (02:30 BRT, após risk_matrix) | `python3 pipelines/push_risk_to_smartico.py` | Smartico API (tags + tier) | `multibet.risk_tags`, Smartico token |
| 6 | `export_smartico_sent_today` | `0 10 * * *` (07:00 BRT, audit pós-push) | `python3 pipelines/export_smartico_sent_today.py` | CSV audit/Slack | idem push_smartico |
| 7 | `fact_sports_odds_performance` | `0 8 * * *` (05:00 BRT) | `python3 pipelines/fact_sports_odds_performance.py` | `multibet.fact_sports_odds_performance` | Athena `vendor_ec2.tbl_sports_book_*` |
| 8 | `game_image_mapping` (v4) | `30 2 * * *` (antes do Grandes Ganhos) | via `deploy_game_enrich_v4.sh` | `multibet.game_image_mapping` | Athena catálogo |
| 9 | `pcr_pipeline` | `30 6 * * *` (03:30 BRT) | `python3 pipelines/pcr_pipeline.py` | `multibet.pcr_ratings` | Athena `ps_bi.fct_player_activity_daily`, `dim_user` |
| 10 | `views_casino_sportsbook` | `30 7 * * *` (04:30 BRT) | `python3 views_casino_sportsbook/run_views_casino_sportsbook.sh` | 7 views gold Casino+SB | Athena |

## Checklist de ativação (Gusta)

Pra cada pipeline acima, criar deployment no Prefect:

1. **Pull commit mais recente** do repo `github.com/mateusrfabro/multibet-analytics`
2. **Conferir `.env`** do orquestrador — variáveis obrigatórias abaixo
3. **Criar deployment Prefect** com o comando da coluna 4 na cadência da coluna 3
4. **Rodar smoke test** em dry-run / `--days 2` antes de agendar

## Variáveis de ambiente obrigatórias

### `.env` (Super Nova DB + bastion)
```
SUPERNOVA_HOST=supernova-db.c8r8mcwe6zq9.us-east-1.rds.amazonaws.com
SUPERNOVA_DB=supernova_db
SUPERNOVA_USER=analytics_user
SUPERNOVA_PASS=<DM>
BASTION_HOST=<IP>
BASTION_USER=ec2-user
SUPERNOVA_PEM_PATH=bastion-analytics-key.pem
```

### `.env` (Meta Ads — pipelines 1 e 3)
```
META_ADS_ACCESS_TOKEN=EAASFqlKv054BR...           # atualiza automático via refresh_meta_token
META_APP_ID=1272866485031838                      # app Caixinha
META_APP_SECRET=<DM>                              # 32 chars
META_ADS_ACCOUNT_IDS=act_1418521646228655,act_1531679918112645,act_1282215803969842,act_4397365763819913,act_26153688877615850,act_1394438821997847
```
**Nota:** `act_846913941192022` ("Multibet sem BM") excluída — sem permissão no token BM2. O pipeline tem resiliência por conta (commit `f2800b7`), então incluir esta conta na lista é tolerável (gera apenas warning).

### `.env` (Google Ads — pipeline 2)
```
GOOGLE_ADS_DEVELOPER_TOKEN=...
GOOGLE_ADS_CLIENT_ID=...
GOOGLE_ADS_CLIENT_SECRET=...
GOOGLE_ADS_REFRESH_TOKEN=...
GOOGLE_ADS_CUSTOMER_ID=4985069191
GOOGLE_ADS_LOGIN_CUSTOMER_ID=1004058739
```

### `.env` (Smartico — pipelines 5 e 6)
```
SMARTICO_API_KEY=...
SMARTICO_BASE_URL=https://api.smartico.ai
```

## Arquivos nesta pasta (mapa)

- `db/` — conectores Python (espelho de `/db/` do repo)
- `pipelines/` — pipelines executáveis
- `sql/` — DDLs versionados
- `views_casino_sportsbook/` — sub-pacote com 7 views gold + pipelines próprios
- `deploy_*.sh` — scripts one-shot de setup inicial (instalar dependências, criar venv, setar permissões)
- `run_*.sh` — wrappers de cron (cadência no comentário)
- `DEPLOY.md` — guia de deploy detalhado

## Separação de preocupações

- Este manifesto cobre **pipelines empacotados em `ec2_deploy/`** (core MultiBet BRL).
- O orquestrador do Gusta já roda **outros pipelines** (`sync_all_incremental_*`, `sync_all_aquisicao`, `risk_matrix`, `fact_casino_rounds`, etc.) que não vivem nesta pasta — são código próprio do orquestrador. Consultar inventário Prefect dele.
- Play4Tune (PKR, Super Nova Bet) ainda não está contemplado — bloqueado aguardando confirmação do token Meta PKR pelo gestor.

## Referências

- Comportamento Meta API + tokens: [memory/reference_meta_marketing_api.md](../memory/reference_meta_marketing_api.md)
- Política deploy EC2: [CLAUDE.md](../CLAUDE.md) § Fluxo de Deploy EC2
- Regra git-first: `memory/feedback_git_first_then_ec2_deploy.md`
- Ec2 migração orquestrador: `memory/project_ec2_migracao_orquestrador.md`

## Mudanças recentes (24/04/2026)

- ✅ `sync_meta_spend.py`: adiciona D-0 intraday, resiliência por conta, alerta expiração token, page_views + reach
- ✅ `sync_google_ads_spend.py`: paridade D-0 intraday
- ✅ NOVO: `refresh_meta_token.py` — renovação automática mensal via `fb_exchange_token`
- ✅ `db/smartico_api.py`: `_warn_if_silent_drop` (detecta eventos descartados silenciosamente)
- ✅ Tabela `multibet.fact_ad_spend` ganhou `page_views` + `reach`
- ✅ View nova `multibet.vw_ad_daily_summary` — agregado `(dt, ad_source)` com 11 KPIs prontos (CPC, CTR, CPV, CPM, frequency, landing_rate, CPL, CFTD, REG/FTD)
