# Setup Slack — Distribuição CSV (segmentação A+S)

> Setup uma vez (~10min). Depois 0 trabalho.

---

## Passo 1 — Criar Slack App (3min)

1. Abre https://api.slack.com/apps
2. Login com sua conta Slack do workspace Multibet
3. Clica **Create New App** → **From scratch**
4. Nome: `Multibet Analytics Bot` (ou outro)
5. Workspace: seleciona o workspace da Multibet
6. Clica **Create App**

## Passo 2 — Configurar permissões do Bot (2min)

1. Menu lateral esquerdo → **OAuth & Permissions**
2. Rola até **Scopes** → **Bot Token Scopes** → **Add an OAuth Scope**
3. Adiciona estes 2 scopes:
   - `files:write` (subir arquivos)
   - `chat:write` (enviar mensagens)

## Passo 3 — Instalar o app no workspace (1min)

1. Volta pro topo da página **OAuth & Permissions**
2. Clica **Install to Multibet** (ou nome do workspace)
3. Autoriza a instalação
4. **Copia o Bot User OAuth Token** (começa com `xoxb-...`)

## Passo 4 — Criar/escolher canal e convidar bot (2min)

1. No Slack, cria um canal dedicado: `#segmentacao-as-diaria` (ou usa um existente como `#crm-reports`)
2. Adiciona os 7 destinatários CRM ao canal:
   - Castrin (Caio Ferreira)
   - Victor Campello
   - Liliane Carvalho
   - Raphael Braga
   - Andreza Ribeiro
   - Felipe Lio
   - Gabriel Tameirão
3. Convida o bot:
   ```
   /invite @Multibet Analytics Bot
   ```

## Passo 5 — Pegar Channel ID (1min)

1. Clica direito no nome do canal → **Copy link**
2. URL fica tipo: `https://multibet.slack.com/archives/C0123456789`
3. **Channel ID** = parte após `/archives/` (ex: `C0123456789`)

## Passo 6 — Configurar `.env` (30s)

Adiciona ao `.env` da máquina local (e depois da EC2):

```bash
SLACK_BOT_TOKEN=xoxb-XXXXXXXXX-XXXXXXXXX-XXXXXXXXXXXXXXXXXXXXX
SLACK_CHANNEL_ID=C0123456789
```

## Passo 7 — Testar (1min)

```bash
python -c "
from db.slack_uploader import enviar_mensagem_slack
enviar_mensagem_slack('Teste do bot — pipeline conectado.')
"
```

Se chegar mensagem no canal → **funcionou** ✅

---

## Como vai funcionar diariamente

- **04:00 BRT (cron EC2)** — pipeline gera CSV + posta no Slack
- **Mensagem no canal** com:
  - Resumo (N jogadores, S+A, GGR 90d)
  - CSV anexado (4-5MB)
  - Legenda anexada
- **Equipe CRM** baixa o CSV direto do Slack

## Troubleshooting

| Erro | Causa | Solução |
|---|---|---|
| `not_in_channel` | Bot não está no canal | `/invite @nome-do-bot` |
| `missing_scope` | Faltou scope no Slack App | Voltar passo 2, adicionar scope, reinstalar |
| `invalid_auth` | Token errado | Conferir `SLACK_BOT_TOKEN` no `.env` |
| `channel_not_found` | Channel ID errado | Conferir `SLACK_CHANNEL_ID` (passo 5) |
