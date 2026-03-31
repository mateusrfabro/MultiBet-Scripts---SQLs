# Deploy — Pipelines MultiBet (EC2)

## Estrutura da pasta
```
multibet/
├── .env                          ← copiar do .env.example e preencher credenciais
├── bigquery_credentials.json     ← chave de serviço do BigQuery (pedir pro Mateus)
├── bastion-analytics-key.pem     ← chave SSH do bastion (pedir pro Mateus)
├── requirements.txt
├── run_grandes_ganhos.sh         ← cron: pipeline Grandes Ganhos (diário)
├── run_anti_abuse.sh             ← loop: bot Anti-Abuse Multiverso (a cada 5 min)
├── db/
│   ├── bigquery.py
│   ├── redshift.py
│   └── supernova.py
├── logs/                         ← criada automaticamente
└── pipelines/
    ├── grandes_ganhos.py         ← pipeline Grandes Ganhos
    ├── anti_abuse_multiverso.py  ← bot Anti-Abuse Campanha Multiverso
    └── ddl_grandes_ganhos.sql
```

## Setup na EC2

```bash
# 1. Criar pasta e copiar os arquivos
mkdir -p /home/ec2-user/multibet
# (copiar todos os arquivos desta pasta para /home/ec2-user/multibet/)

# 2. Instalar Python 3.12+ (se não tiver)
sudo yum install python3.12 -y  # Amazon Linux 2023
# ou
sudo yum install python3 -y     # Amazon Linux 2

# 3. Criar venv e instalar dependências
cd /home/ec2-user/multibet
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 4. Configurar .env
cp .env.example .env
nano .env  # preencher REDSHIFT_PASSWORD e ajustar paths

# 5. Copiar credenciais
# - bigquery_credentials.json → /home/ec2-user/multibet/
# - bastion-analytics-key.pem → /home/ec2-user/multibet/
chmod 600 bastion-analytics-key.pem

# 6. Testar manualmente
source venv/bin/activate
python3 pipelines/grandes_ganhos.py

# 7. Dar permissão de execução ao script
chmod +x run_grandes_ganhos.sh
```

## Agendar no cron (diário às 00:30 BRT)

```bash
crontab -e
```

Adicionar a linha:
```
30 3 * * * /home/ec2-user/multibet/run_grandes_ganhos.sh
```

> **Nota:** 3:00 UTC = 0:00 BRT. Então `30 3` = 00:30 BRT.

## Logs
Os logs ficam em `pipelines/logs/grandes_ganhos_YYYY-MM-DD.log`.

## ETL Aquisicao Trafego — cron horario (a cada 60 min)

Alimenta `multibet.aquisicao_trafego_diario` no Super Nova DB.
Consumido pela aba "Aquisicao Trafego" do front `db.supernovagaming.com.br`.

### Deploy

```bash
# 1. Copiar pipeline e wrapper (ja estao no ec2_deploy/)
# pipelines/etl_aquisicao_trafego_diario.py
# run_etl_aquisicao_trafego.sh
# db/athena.py

# 2. Instalar pyathena (se ainda nao tiver)
source venv/bin/activate
pip install pyathena>=3.0

# 3. Garantir variaveis no .env
# ATHENA_AWS_ACCESS_KEY_ID=...
# ATHENA_AWS_SECRET_ACCESS_KEY=...
# ATHENA_S3_STAGING=s3://aws-athena-query-results-803633136520-sa-east-1/
# ATHENA_REGION=sa-east-1
# BASTION_HOST=...
# SUPERNOVA_HOST=...
# SUPERNOVA_PASS=...

# 4. Testar manualmente
python3 pipelines/etl_aquisicao_trafego_diario.py --days 1

# 5. Dar permissao e agendar
chmod +x run_etl_aquisicao_trafego.sh
crontab -e
```

### Crontab

```
# ETL Aquisicao Trafego — a cada hora (minuto 10, evita colisao com outros ETLs)
10 * * * * /home/ec2-user/multibet/run_etl_aquisicao_trafego.sh
```

> Roda a cada hora no minuto 10 (ex: 00:10, 01:10, ..., 23:10 UTC).
> Reprocessa D-2 + D-1 + hoje (parcial). Idempotente (DELETE + INSERT).

### Logs
```bash
tail -f pipelines/logs/etl_aquisicao_trafego_$(date +%Y-%m-%d).log
```

## Bot Anti-Abuse — Campanha Multiverso

Monitora os 6 Fortune games (PG Soft) em tempo real, detecta fraude e alerta no Slack.

### Variáveis de ambiente necessárias (adicionar ao `.env`)
```
SLACK_WEBHOOK_MULTIVERSO=https://hooks.slack.com/services/...
```

### Iniciar / Parar / Status
```bash
chmod +x run_anti_abuse.sh

./run_anti_abuse.sh           # inicia em background (loop a cada 5 min)
./run_anti_abuse.sh status    # verifica se está rodando
./run_anti_abuse.sh stop      # para o bot
```

### Logs
```bash
tail -f logs/anti_abuse_$(date +%Y-%m-%d).log
```

### Reiniciar após reboot da EC2
Adicionar ao crontab para iniciar automaticamente:
```bash
crontab -e
# adicionar:
@reboot /home/ec2-user/multibet/run_anti_abuse.sh
```

---

## Observações
- A senha do Redshift expira periodicamente — atualizar no `.env` quando necessário.
- O IP do bastion (`supernova.py`) pode mudar se a EC2 não tiver Elastic IP.
- Se o pipeline rodar NA PRÓPRIA EC2 do bastion, o tunnel SSH não é necessário
  (conectar direto no RDS). Nesse caso, simplificar `supernova.py`.
