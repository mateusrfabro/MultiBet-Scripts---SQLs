# Setup Google Drive — Distribuição CSV (segmentação A+S)

> Guia direto. Setup uma vez (~25min), depois 0 trabalho.

---

## Passo 1 — Criar projeto no Google Cloud (5min)

1. Abra https://console.cloud.google.com
2. Topo da página → seletor de projetos → **NOVO PROJETO**
3. Nome: `multibet-analytics-pipelines` (ou qualquer)
4. Clica em **Criar**

## Passo 2 — Habilitar Google Drive API (1min)

1. Menu (☰) → **APIs e serviços** → **Biblioteca**
2. Busca: `Google Drive API`
3. Clica → **HABILITAR**

## Passo 3 — Criar Service Account (5min)

1. Menu → **IAM e administrador** → **Contas de serviço** → **+ CRIAR CONTA DE SERVIÇO**
2. Nome: `multibet-pipeline-bot`
3. Pula "Conceder acesso ao projeto" (não precisa)
4. Pula "Conceder acesso aos usuários" (não precisa)
5. Clica em **Concluído**
6. Anota o **e-mail da SA** (ex: `multibet-pipeline-bot@multibet-analytics-pipelines.iam.gserviceaccount.com`)

## Passo 4 — Gerar chave JSON (2min)

1. Lista de Service Accounts → clica em `multibet-pipeline-bot`
2. Aba **CHAVES** → **ADICIONAR CHAVE** → **Criar nova chave** → **JSON** → **Criar**
3. Salva o arquivo `.json` baixado em local seguro (ex: `C:\Users\NITRO\.gdrive_creds\multibet-sa.json`)
4. **NUNCA commitar esse JSON no git** (já está no `.gitignore` por convenção `*.json`)

## Passo 5 — Criar pasta no Drive (3min)

1. Abre https://drive.google.com (com sua conta gmail)
2. **+ Novo** → **Pasta** → nome: `Multibet Analytics — Reports`
3. Clica direito na pasta → **Compartilhar**
4. Adiciona o **e-mail da Service Account** (anotado no Passo 3) → permissão **Editor**
5. Adiciona os destinatários CRM com permissão **Visualizador**:
   - `ext.caio.ferreira@multi.bet.br`
   - `victor.campello@multi.bet.br`
   - `liliane.carvalho@multi.bet.br`
   - `raphael.braga@multi.bet.br`
   - `ext.andreza.ribeiro@multi.bet.br`
   - `felipe.lio@multi.bet.br`
   - `gabriel.tameirao@multi.bet.br`
6. **Desmarca** "Notificar pessoas" (evita e-mail de convite spam)

## Passo 6 — Pegar Folder ID

1. Abre a pasta no Drive
2. URL: `https://drive.google.com/drive/folders/1aB2c3D4eFgH5iJ6kL7mN8oP9qRsT0uV`
3. **Folder ID** = parte após `/folders/` (ex: `1aB2c3D4eFgH5iJ6kL7mN8oP9qRsT0uV`)

## Passo 7 — Configurar `.env`

Adiciona ao `.env` da máquina local (e depois da EC2):

```bash
GDRIVE_CREDENTIALS_JSON=C:/Users/NITRO/.gdrive_creds/multibet-sa.json
GDRIVE_FOLDER_ID=1aB2c3D4eFgH5iJ6kL7mN8oP9qRsT0uV
```

## Passo 8 — Instalar dependências

```bash
pip install google-api-python-client google-auth-httplib2
```

(Já adicionado em `requirements.txt`.)

## Passo 9 — Testar local

```bash
python -c "
from db.google_drive import GoogleDriveUploader
drive = GoogleDriveUploader()
file_id, link = drive.upload_replace('output/players_segmento_SA_2026-04-28_FINAL.csv')
print(f'Subiu: {link}')
"
```

Se aparecer um link `https://drive.google.com/file/d/...` → **funcionou** ✅

## Passo 10 — Castrin valida

1. Abre o Drive (gmail dele ou web)
2. Vê a pasta compartilhada `Multibet Analytics — Reports`
3. Confere o CSV de hoje

---

## Como vai funcionar diariamente

- **04:00 BRT (cron EC2)** — pipeline gera CSV + sobe pro Drive
- **Nome do arquivo:** `players_segmento_SA_<data>_FINAL.csv` (substitui se já existir do mesmo dia, mantém histórico de outros dias)
- **CRM acessa:** Drive web → pasta compartilhada → baixa CSV
- Drive **notifica automaticamente** os destinatários quando há arquivo novo (configurável no Drive de cada um)

## Troubleshooting

| Erro | Causa | Solução |
|---|---|---|
| `403: insufficient permissions` | SA não tem acesso à pasta | Compartilhar pasta com e-mail da SA (Passo 5.4) |
| `404: File not found` | Folder ID errado | Conferir URL da pasta (Passo 6) |
| `googleapiclient not installed` | dependência faltando | `pip install google-api-python-client` |

## Migração futura: e-mail SMTP

Quando TI liberar SMTP AUTH, podemos voltar a usar e-mail (basta reverter as 1 mudança no `pipelines/segmentacao_sa_diaria.py:1054`). Drive continua funcionando em paralelo se quiser ambos.
