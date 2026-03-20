# Contexto do Analista

## Quem sou eu
Sou analista de dados com 26 anos no mercado de iGaming. 
Trabalho na Super Nova Gaming, empresa de soluções para bets regulamentadas. 
Atendo 100% do tempo a MultiBet e demandas externas.
Estou há 6 meses nessa função e venho aprendendo muito, principalmente com IA.
Meu objetivo é crescer para gestor/gerente, então preciso entregar além do esperado.

## Minha equipe
- **Castrin (Caio):** Head de dados, foco em se tornar CFO
- **Mauro:** Analista sênior, foco em analytics
- **Gusta:** Analista sênior, foco em infra
- **Eu:** Analista de dados, quero crescer para gestão

## Ferramentas que uso
- **Banco de dados:** AWS Athena (Iceberg Data Lake, read-only) — databases com sufixo `_ec2` (ex: `fund_ec2`, `ecr_ec2`, `bireports_ec2`)
- **CRM:** BigQuery da Smartico
- **IDE:** VS Code, DBeaver
- **Versionamento:** GitHub
- **Linguagens:** Python, SQL
- **Frontend:** Flask + HTML + CSS (API chama os dados via request, Flask
  interpreta o HTML, gera arquivo index.html como página)

> **Redshift foi descontinuado** para análises. Todo acesso transacional passa agora pelo Athena.

> **Super Nova DB (PostgreSQL): NÃO usar como fonte de dados para entregas.**
> As tabelas canônicas estão em validação. Usar **somente Athena** para extrações/análises.
> O Super Nova DB pode ser usado como destino (persistir resultados), mas nunca como fonte
> até que o usuário confirme explicitamente que uma tabela foi validada.

## Como devo ser ajudado
1. **SQL:** sempre otimizado, com comentários explicando cada bloco,
   pensando que o banco é read-only no Athena (motor Trino/Presto)
2. **Python:** código limpo, com logs e tratamento de erros,
   sempre explicando o racional
3. **Análises:** explique o porquê das decisões, não só o como
4. **Dashboards:** padrão Flask + HTML + CSS + API
5. **Sempre pergunte** se não ficou claro o que é pedido antes de sair fazendo
6. **Entregue além:** sugira melhorias, aponte riscos, pense como um gestor
7. **Me ensine enquanto faz:** quero entender, não só copiar

## Contexto dos bancos de dados
- **Athena (Iceberg Data Lake):** banco principal para análises transacionais.
  Databases usam sufixo `_ec2`: `fund_ec2`, `ecr_ec2`, `bireports_ec2`, `bonus_ec2`,
  `cashier_ec2`, `casino_ec2`, `csm_ec2`, `vendor_ec2`, etc.
  Script: `db/athena.py` → `query_athena(sql, database="fund_ec2")`
- **BigQuery (Smartico):** CRM, use a documentação disponível.
- Quando não souber a estrutura exata, pergunte ou sugira que eu consulte
  a documentação antes de montar a query.
- **Redshift foi descontinuado** — não usar mais para novas análises.

### Regra de fuso horário (OBRIGATÓRIA)
- **O Athena/Iceberg opera em UTC.** Toda query que extraia, filtre ou
  exiba dados com timestamp DEVE converter para BRT usando:
  `AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'`
  (sintaxe Presto/Athena — diferente do Redshift)
- Isso vale para SELECTs, WHEREs com data, relatórios, exports, pipelines
  — sem exceção.
- Nunca retorne timestamps crus (UTC) em entregas finais ao negócio.

### Regras obrigatórias Athena SQL — validadas pelo arquiteto (17/03/2026)

**REGRA DE OURO — Filtro de Partição `dt` (fund_ec2):**
Sempre incluir antes do filtro de timestamp — evita Full Scan no S3 (custo AWS alto):
`f.dt IN ('2026-03-16', '2026-03-17')` — incluir todas as datas do período.

**Campo de valor:** `c_confirmed_amount_in_inhouse_ccy` (líquido confirmado) — NÃO `c_amount_in_ecr_ccy`

**Status:** `c_txn_status = 'txn_confirmed_success'` — NÃO `'SUCCESS'` (era Redshift)

**Sintaxe Presto:** Cast = `TIMESTAMP '2026-03-16'` | Rollback = `COUNT_IF(c_txn_type = 72)` | Timezone = `AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'`

### Padrões obrigatórios para pipelines Athena (Trino/Presto)

#### 1. Sintaxe de cálculo financeiro
- Usar `date_trunc('day', coluna)` para truncar datas (não `TRUNC()`)
- Cast explícito quando necessário: `CAST(valor AS DOUBLE)` ou `CAST(coluna AS VARCHAR)`
- Funções de data: `date_add('day', n, data)`, `date_diff('day', inicio, fim)`

#### 2. Joins e tabelas temporárias
- **Nunca usar `CREATE TEMP TABLE`** — não é suportado no Athena
- Para cohorts intermediários: usar **CTE** (`WITH cohort AS (...)`)
- Para cohorts massivos que precisam persistir: `CREATE TABLE s3_staging.nome AS SELECT ...`
  (cria tabela externa no S3 via Athena — lembrar de dropar depois)

#### 3. Conexão e persistência
- Toda persistência usa a **API do Athena** via `db/athena.py` (boto3)
- Não há overhead de SSH/Postgres — a conexão é direta via AWS SDK
- Para escrita de resultados: salvar em S3 ou inserir no Super Nova DB (PostgreSQL)

#### 4. Otimização de custo (Athena cobra por dados escaneados)
- **Sempre filtrar pelas colunas de partição** nas cláusulas WHERE — geralmente `date` ou `dt`
- Evitar `SELECT *` — selecionar apenas as colunas necessárias
- Preferir as camadas pré-agregadas (`ps_bi`) quando disponíveis, em vez dos `_ec2` brutos
- Usar `LIMIT` durante desenvolvimento/testes para não escanear tabelas inteiras

## Meu objetivo com cada entrega
Quero ser reconhecido pelo time, mostrar capacidade de gestão e crescer 
na empresa. Cada entrega deve ser sólida, bem documentada e com raciocínio 
claro — como um analista sênior entregaria.

## Super Nova Bet (Paquistão)
Em breve iniciaremos operações. Quando houver demandas relacionadas, 
leia a documentação disponível e contribua com opiniões sobre boas práticas.