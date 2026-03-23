# Guia para o Designer — Dashboard Google Ads

## O que voce precisa alterar

**Um arquivo apenas:** `static/css/theme.css`

Abra esse arquivo e altere as variaveis no topo (secao `:root`).
Todo o dashboard vai atualizar automaticamente.

## Variaveis disponiveis

### Cores
| Variavel | O que controla | Valor atual |
|---|---|---|
| `--cor-fundo` | Fundo da pagina | `#0F1923` |
| `--cor-card` | Fundo dos cards | `#1A2735` |
| `--cor-texto` | Texto principal | `#E8EAED` |
| `--cor-texto-secundario` | Texto secundario | `#8B9CAF` |
| `--cor-destaque` | Azul dos botoes/links | `#4A9EFF` |
| `--cor-destaque-ngr` | Dourado do NGR | `#FFB020` |
| `--cor-positivo` | Verde (subiu) | `#00C853` |
| `--cor-negativo` | Vermelho (caiu) | `#FF5252` |
| `--cor-grafico-cassino` | Roxo nos graficos | `#7C4DFF` |
| `--cor-grafico-sport` | Verde agua nos graficos | `#00BFA5` |

### Tipografia
| Variavel | O que controla | Valor atual |
|---|---|---|
| `--fonte-principal` | Fonte do texto | `Inter` |
| `--fonte-numeros` | Fonte dos valores | `JetBrains Mono` |
| `--tamanho-kpi-valor` | Tamanho do numero no card | `32px` |
| `--tamanho-titulo` | Tamanho do titulo | `24px` |

### Espacamento
| Variavel | O que controla | Valor atual |
|---|---|---|
| `--padding-card` | Espacamento interno dos cards | `24px` |
| `--gap-cards` | Espaco entre cards | `16px` |
| `--border-radius` | Arredondamento dos cantos | `12px` |

## Estrutura de arquivos

```
dashboards/google_ads/
    static/
        css/
            theme.css       ← VOCE MEXE AQUI
        js/
            dashboard.js    ← NAO MEXER (logica)
    templates/
        dashboard.html      ← estrutura (mexer so se mudar layout)
        login.html          ← tela de login
    app.py                  ← backend (NAO MEXER)
    queries.py              ← dados (NAO MEXER)
    config.py               ← configuracao (NAO MEXER)
```

## Classes CSS importantes

| Classe | Onde aparece |
|---|---|
| `.kpi-card` | Card de metrica (REG, FTD, etc.) |
| `.kpi-value` | Valor grande dentro do card |
| `.kpi-label` | Label pequeno acima do valor |
| `.variacao-up` | Badge verde (subiu) |
| `.variacao-down` | Badge vermelho (caiu) |
| `.destaque-ngr` | Card do NGR (borda dourada) |
| `.executive-summary` | Frase resumo no topo |
| `.insight-critical` | Alerta critico (vermelho) |
| `.insight-warning` | Alerta atencao (amarelo) |
| `.insight-positive` | Insight positivo (verde) |
| `.chart-card` | Container dos graficos |
| `.data-table` | Tabela de detalhamento |
| `.legenda` | Secao "Como ler este dashboard" |

## Como testar alteracoes

1. Abra `theme.css` no editor
2. Mude as variaveis que quiser
3. Salve o arquivo
4. Recarregue a pagina no navegador (F5)
5. Pronto — o visual atualiza instantaneamente

## Logo e marca

Para adicionar a logo da MultiBet:
1. Coloque o arquivo em `static/img/logo.png`
2. No `dashboard.html`, busque `header-title` e adicione:
   ```html
   <img src="{{ url_for('static', filename='img/logo.png') }}" alt="MultiBet" height="32">
   ```