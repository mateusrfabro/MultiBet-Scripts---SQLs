# Anti-Abuse Bot — Campanha Multiverso
**Resumo técnico para o time de infra/dados**

---

## O que o bot faz

Monitora comportamentos suspeitos nos **6 jogos Fortune (PG Soft)** durante a Campanha Multiverso.
Roda em loop a cada **5 minutos** via BigQuery (Smartico CRM) e envia alerta no Slack quando detecta jogadores com risco ALTO.

---

## Flags monitoradas e pontuação

| Flag | Quando dispara | Pontos |
|---|---|---|
| `CONTA_POS_PROMO` | Conta criada depois de `2026-03-13 20:00 UTC` | +45 |
| `BOT_SPEED_Xs` | Média de intervalo entre apostas < **1.0 segundo** (automação) | +30 |
| `BONUS_REPETIDO_xN` | Mesma quest entregue mais de 1x para o mesmo jogador | +55 |
| `BONUS_EXCESSO_N` | Total de bônus da campanha > 18 (máximo legítimo = 6 jogos × 3 quests) | +55 |

### Níveis de risco

| Nível | Score | Ação |
|---|---|---|
| BAIXO | < 35 | Só registra no log — sem alerta |
| MEDIO | ≥ 35 | Aparece na tabela mas não dispara Slack |
| ALTO | ≥ 55 | **Envia alerta imediato no Slack** |

### Combinações mais comuns

- Bônus repetido (sozinho): 55 → **ALTO** (fraude direta)
- Conta pós-promo + bot: 45+30 = 75 → **ALTO**
- Conta pós-promo + bônus repetido: 45+55 = 100 → **ALTO** (caso grave)
- Conta pós-promo sozinha: 45 → **MEDIO**
- Bot sozinho: 30 → **BAIXO** (sinal isolado, sem alerta)

---

## Mensagem enviada no Slack (#risco-multiverso)

Quando há pelo menos 1 jogador ALTO risco, o bot envia:

```
🚨 ANTI-ABUSE MULTIVERSO — 13/03/2026 20:35 BRT

Risco ALTO:        🔴 3 jogadores
Risco MÉDIO:       🟡 7 jogadores
Total ativos:      142 jogadores
Total apostado:    R$ 48.320,00
Janela analisada:  Últimas 24h
Jogos monitorados: 6 Fortune (PG Soft)

─────────────────────────────────────────────────────────────────────
          ID  SCORE      TOTAL      P&L  FLAGS
----------------------------------------------------------------------
   123456789    100  R$ 2.500  R$  -200  BONUS_REPETIDO_x3, CONTA_POS_PROMO
   987654321     75  R$ 1.800  R$   150  CONTA_POS_PROMO, BOT_SPEED_0.4s
   111222333     55  R$   980  R$    40  BONUS_REPETIDO_x2

Links diretos para o perfil no Smartico:
• 123456789  [100]  BONUS_REPETIDO_x3, CONTA_POS_PROMO
• 987654321  [75]   CONTA_POS_PROMO, BOT_SPEED_0.4s
• 111222333  [55]   BONUS_REPETIDO_x2

ℹ️ Score alto = mais sinais de fraude acumulados. Avaliar o conjunto de flags, não só o número.
```

**Quando NÃO tem ALTO risco:** silêncio total no Slack. O bot só registra no log local da EC2.

---

## Onde os dados são consultados

| Dado | Fonte (BigQuery Smartico) |
|---|---|
| Apostas nos Fortune games | `tr_casino_bet` |
| Ganhos | `tr_casino_win` |
| Saques aprovados | `tr_acc_withdrawal_approved` |
| Bônus da campanha | `j_bonuses` (filtrado pelos 18 Journey IDs) |
| Data de cadastro | `j_user.core_registration_date` |

---

## Snapshots JSON (histórico EC2)

A cada ciclo de 5 min, salva um arquivo em `reports/anti_abuse_YYYYMMDD_HHMM.json` com:

```json
{
  "gerado_em": "2026-03-13 20:35:00 BRT",
  "total_jogadores": 142,
  "alto_risco": 3,
  "medio_risco": 7,
  "jogadores": [
    {
      "user_id": 123456789,
      "risk_score": 100,
      "risk_level": "ALTO",
      "flags": "BONUS_REPETIDO_x3, CONTA_POS_PROMO",
      "total_wagered": 2500.0,
      "pnl": -200.0,
      ...
    }
  ]
}
```

Arquivos com mais de **7 dias** são removidos automaticamente.

---

## Arquivos entregues

```
anti_abuse_deploy/
├── pipelines/
│   ├── anti_abuse_multiverso.py        ← bot principal
│   └── anti_abuse_multiverso_test.py   ← versão com thresholds baixos para testar o Slack na EC2
├── db/
│   └── bigquery.py                     ← conector BigQuery
├── run_anti_abuse.sh                   ← start / stop / status
├── .env                                ← credenciais (preencher na EC2)
├── .env.example                        ← template
├── requirements.txt
├── DEPLOY.md                           ← instruções de setup
└── info.json                           ← documentação do projeto
```

Para deploy, ver `DEPLOY.md`.
