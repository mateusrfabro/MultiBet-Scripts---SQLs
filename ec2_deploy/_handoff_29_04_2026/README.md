# Handoff Meta Ads + Google Ads — 29/04/2026

**Foco:** ativar/atualizar pipelines de spend Meta + Google + refresh mensal do token Meta no orquestrador. Mais nada.

## Os 8 arquivos desta pasta vão para a EC2 nestes destinos:

| De (aqui) | Para (EC2) | Ação |
|---|---|---|
| `pipelines/sync_meta_spend.py` | `~/multibet-analytics/pipelines/sync_meta_spend.py` | substitui |
| `pipelines/sync_google_ads_spend.py` | `~/multibet-analytics/pipelines/sync_google_ads_spend.py` | substitui |
| `pipelines/refresh_meta_token.py` | `~/multibet-analytics/pipelines/refresh_meta_token.py` | NOVO |
| `db/meta_ads.py` | `~/multibet-analytics/db/meta_ads.py` | substitui |
| `db/google_ads.py` | `~/multibet-analytics/db/google_ads.py` | substitui |
| `run_sync_meta_ads.sh` | `~/multibet-analytics/run_sync_meta_ads.sh` | substitui |
| `run_sync_google_ads.sh` | `~/multibet-analytics/run_sync_google_ads.sh` | substitui |
| `run_refresh_meta_token.sh` | `~/multibet-analytics/run_refresh_meta_token.sh` | NOVO |

## Mais 3 coisas (não-arquivo)

1. **Conferir 4 vars no `.env` do orquestrador** (Meta — `META_APP_SECRET` te mando por DM):
   ```
   META_ADS_ACCESS_TOKEN=EAA...                           # já existe, vai ser reescrito pelo refresh mensal
   META_APP_ID=1272866485031838                           # app Caixinha
   META_APP_SECRET=<DM>                                   # 32 chars
   META_ADS_ACCOUNT_IDS=act_1418521646228655,act_1531679918112645,act_1282215803969842,act_4397365763819913,act_26153688877615850,act_1394438821997847
   ```
   > Nota: `act_846913941192022` ("Multibet sem BM") **excluída de propósito** — sem permissão no token BM2 (gera warning). Se aparecer no `.env` antigo, **remover**.

2. **Cron (3 mudanças):**
   - **Adicionar:** `sync_meta_spend.py --days 2` em **`0 9,13,17,21,1 * * *`** (5x/dia: 06h, 10h, 14h, 18h, 22h BRT)
   - **Adicionar:** `sync_google_ads_spend.py --days 3` em **`0 9,13,17,21,1 * * *`** (5x/dia, mesma cadência)
   - **Adicionar:** `refresh_meta_token.py` em **`0 5 1 * *`** (dia 1 de cada mês, 02:00 BRT)

3. **Smoke test pós-deploy:**
   ```bash
   cd ~/multibet-analytics && source venv/bin/activate
   python3 pipelines/sync_meta_spend.py --days 2
   python3 pipelines/sync_google_ads_spend.py --days 2
   ```
   Esperado: log "Sync concluido: N linhas | M contas | Spend: R$ X" sem stack trace.

## Contexto rápido (por que esse handoff)

- O cron Meta antigo da EC2 ETL **parou em 24/04 18:22 UTC** (5 dias sem carga). Já repus o spend de 25-28/04 manualmente local antes deste handoff (271 linhas, R$ 412.552,67) — histórico está OK até hoje.
- O `sync_meta_spend.py` ganhou em 24/04: `page_views`, `reach`, alerta de expiração de token, resiliência por conta (1 conta caída não derruba pipeline).
- O `refresh_meta_token.py` é **novo** — chama `fb_exchange_token` na Meta e reescreve o `META_ADS_ACCESS_TOKEN` no `.env` (token atual expira em **22/06/2026**, 54d).
- A view `multibet.vw_ad_daily_summary` já consome esses dados e produz o relatório no padrão Castrin/Gab (CUSTO ADS, CLICK, CPC, CTR, PAGE VIEW, CPV, LEADS BKO, CPL, FTD, CFTD, REG/FTD).

## Não precisa mexer em nada fora dessa pasta

Os outros pipelines do orquestrador (pcr_pipeline, segmentacao_sa, grandes_ganhos, fact_sports_odds, etc.) **não mudaram**. Deixa como está.
