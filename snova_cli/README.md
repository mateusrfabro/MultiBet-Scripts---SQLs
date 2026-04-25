# Super Nova CLI

CLI interna do squad de dados para **demandas recorrentes**. Evita
copiar e colar script toda vez que o Rapha/Dudu/Mauro pede o mesmo tipo
de extração (base de players, report diário de affiliate etc).

## Como executar

```bash
# Na raiz do projeto
python cli.py <comando> [args]
```

Também roda como módulo:
```bash
python -m snova_cli ...
```

## Comandos disponíveis

### `affiliate-base` — base lifetime de players

Lista todos os players (lifetime) de um ou mais affiliates com `ecr_id`,
`smartico_id`, nome completo, tracker e país. Output pronto pro CRM.

```bash
# 1 affiliate
python cli.py affiliate-base 363722 --name "Pri Simoes"

# N affiliates consolidados
python cli.py affiliate-base 363722 532570 --name "Consolidado"
```

**O que faz:**
1. Puxa players do `ps_bi.dim_user` (filtro `is_test` + `external_id IS NOT NULL`)
2. Roda **auditor Athena** cruzando 3 fontes: `ps_bi.dim_user`, `ecr_ec2.tbl_ecr`, `ecr_ec2.tbl_ecr_banner`
3. Valida unicidade de `smartico_id` e nulls em campos críticos
4. Gera `reports/affiliate_<IDS>_<slug>_base_players_FINAL.csv` + legenda `.txt`

### `affiliate-daily` — report diário

KPIs consolidados do dia (REG, FTD, FTD Deposit, Dep Amount, GGR Cassino,
GGR Sport, NGR, Saques). Pronto pra WhatsApp.

```bash
# D-1 automático
python cli.py affiliate-daily 464673

# Data específica
python cli.py affiliate-daily 464673 --date 2026-04-06

# N affiliates consolidados
python cli.py affiliate-daily 464673 532570 532571 --date 2026-04-06
```

**O que faz:**
1. KPIs via `ps_bi.fct_player_activity_daily`
2. REG/FTD via `ps_bi.dim_user` com filtro `signup_datetime` convertido para BRT
3. Imprime tabela formatada (padrão WhatsApp) no console
4. Gera CSV + legenda em `reports/`

## Arquitetura

```
MultiBet/
├── cli.py                      # entry point (argparse)
├── db/
│   ├── athena.py               # conexão Athena (já existia)
│   ├── helpers.py              # SQL fragments reutilizáveis (novo)
│   └── auditor.py              # classe AthenaAuditor (novo)
└── snova_cli/
    ├── __init__.py
    ├── README.md               # este arquivo
    └── commands/
        ├── affiliate_base.py   # lifetime
        └── affiliate_daily.py  # D-1
```

### `db/helpers.py` — por que existe

Centraliza fragmentos SQL que se repetem em todo script:

- `FILTER_NOT_TEST_PSBI` — `(is_test = false OR is_test IS NULL)`
- `to_brt(col)` — `col AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'`
- `to_brt_date(col)` — mesma conversão + `CAST AS DATE`
- `affiliate_in(ids)` — `CAST(affiliate_id AS VARCHAR) IN ('...')`  (CAST obrigatório)
- `fmt_brl(v)` / `fmt_int(v)` / `fmt_pct(v)` — formatadores BR
- `clean_tz_columns(df)` — fix recorrente de `datetimetz` antes de salvar Excel
- `save_csv_with_legenda(...)` — entrega padrão (CSV UTF-8 + `_legenda.txt`)

**Sem isso**, cada script duplica e algum esquece uma regra
(ex: `c_amount_in_ecr_ccy` em vez de `c_confirmed_amount_in_inhouse_ccy`,
`is_test` esquecido, `CAST` faltando...). Feedback crítico do projeto.

### `db/auditor.py` — por que existe

Regra do squad (`feedback_gatekeeper_deploy_automatizado.md`): nada vai
pro Head/CTO sem auditor. O `AthenaAuditor` padroniza o cross-check:

```python
a = AthenaAuditor()
a.add_count("ps_bi.dim_user", 2278)
a.add_count("ecr_ec2.tbl_ecr", 2278)
a.compare_counts(baseline_label="ps_bi.dim_user")
a.check_unique("ps_bi.dim_user", df, "smartico_id")
a.check_nulls(df, ["ecr_id", "smartico_id"])

a.report()              # imprime sumário (OK / ALERTA / FALHA)
if a.is_approved():     # gate de entrega
    entregar()
```

Tolerância padrão: divergência <2% = OK, <5% = ALERTA, ≥5% = FALHA.

## Adicionando comandos novos

1. Crie `snova_cli/commands/<nome>.py` com uma função `run(...)`
2. No `cli.py`, adicione um subparser no `build_parser()` e aponte para `run`
3. Atualize este README
4. Teste empiricamente antes de commitar (auditor tem que rodar)

## Tarefa recorrente de migração (para depois)

Scripts antigos a serem descontinuados conforme comandos correspondentes
forem testados em produção:

- `scripts/extract_affiliate_532570.py` → `affiliate-base 532570`
- `scripts/extract_affiliate_363722_pri_simoes.py` → `affiliate-base 363722 --name "Pri Simoes"`
- `scripts/extract_3affiliates.py` → `affiliate-base 532570 532571 464673`
- `scripts/extract_affiliates_d1.py` → `affiliate-daily 532570 532571 464673`

Não apagar os originais até a CLI ter sido usada em produção por 2 semanas sem regressão.
