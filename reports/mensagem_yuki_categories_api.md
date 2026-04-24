# Mensagem pro Yuki — categories-api

Olha Yuki, vi o teu repo `categories-api` (branch `feat/nrt`) — da pra simplificar bastante. Ja deixei tudo pronto do lado do banco pra voce plugar direto.

## TL;DR

Criei a view `multibet.vw_front_api_games` no Super Nova DB. O shape dela é **1:1 com teu `GameResponseDto`** (CamelCase, todos os 12 campos). Se trocar as queries Athena do `GameCachedRepository` por SELECTs nessa view, voce:
- Elimina query Athena do caminho critico do request
- Ganha cobertura de catálogo (antes era `ps_bi.dim_game` = 0.2%, agora bireports = 99%+)
- Simplifica o repositorio (1 SELECT simples vs WITH/JOIN)

## Como consumir

```sql
-- shape completo GameResponseDto (ja com aliases CamelCase)
SELECT "gameId", "name", "gameSlug", "gamePath", "image", "provider",
       "category", "categoryDescription",
       "totalBets", "uniquePlayers", "totalBet", "totalWins",
       "rank", "live_subtype", "has_jackpot", "windowEndUtc"
FROM multibet.vw_front_api_games;
```

## Substituicao direta no `GameCachedRepository`

```typescript
// findMostPlayed (era Athena)
const rows = await this.postgres.query(
  `SELECT * FROM multibet.vw_front_api_games
   WHERE "rank" IS NOT NULL
   ORDER BY "rank"
   LIMIT $1 OFFSET $2`,
  [limit, offset]
);

// findMostPaid
`SELECT * FROM multibet.vw_front_api_games
 WHERE "totalWins" > 0
 ORDER BY "totalWins" DESC
 LIMIT $1`

// findByCategory
`SELECT * FROM multibet.vw_front_api_games
 WHERE LOWER("category") = LOWER($1)
 ORDER BY COALESCE("rank", 999999)
 LIMIT $1 OFFSET $2`

// findByProvider
`SELECT * FROM multibet.vw_front_api_games
 WHERE LOWER("provider") = LOWER($1)
 ORDER BY COALESCE("rank", 999999)`

// findLive
`SELECT * FROM multibet.vw_front_api_games
 WHERE "category" = 'live'`

// findFortune
`SELECT * FROM multibet.vw_front_api_games
 WHERE "name" ILIKE '%fortune%' OR "name" ILIKE '%fortuna%'`

// findCrash (continua precisando da lista hardcoded, mas joga contra PG)
`SELECT * FROM multibet.vw_front_api_games
 WHERE "name" ILIKE '%crash%'
    OR "gameId" IN ('8369', '1301', '1320')`
```

## Janela 24h rolante

- O `rank`, `totalBets`, `totalBet`, `totalWins` refletem as ultimas 24h ROLANTES (janela movel).
- Refresh a cada 4h (00, 04, 08, 12, 16, 20 BRT).
- `windowEndUtc` marca o timestamp do refresh — use se precisar mostrar "atualizado ha X horas".

## Numeros reais (agora)

- 2.311 jogos na view (so ativos + com imagem)
- 90 jogos com atividade financeira nas ultimas 24h
- Turnover 24h: R$ 35.444,56 | Wins 24h: R$ 36.264,94
- Top por rank: Fortune Gems 500 #1, Pinata Wins #4, Aviator #10

## Caveats (pra voce saber)

1. `has_jackpot` atualmente retorna `FALSE` para todos — fonte `vendor_ec2.tbl_vendor_games_mapping_mst` deu Empty no Athena. To investigando com o time. **Nao bloqueia** (no DTO esse campo nao existe, só to deixando la pra quando resolvermos).
2. `live_subtype` tem granularidade que voce ainda nao usa (Roleta, Blackjack, Baccarat, GameShow, Outros) — se quiser expor em rota nova (`/games/live/roulette` etc), da pra usar.
3. Cron 4h ainda nao esta ativo na EC2 — aguarda validacao Castrin/CTO. Ate la, eu rodo manual. Se voce plugar amanha, garanto um refresh antes.

## Repo / Commit

- Nosso lado: https://github.com/mateusrfabro/multibet-analytics (commit `05cf38b`)
- DDL: [pipelines/ddl/ddl_game_image_mapping_v3.sql](pipelines/ddl/ddl_game_image_mapping_v3.sql)
- Pipeline: [pipelines/game_image_mapper.py](pipelines/game_image_mapper.py)

Bora? Se quiser fazer dupla amanha eu te ajudo a plugar — sao 7 metodos do repositorio pra trocar, ~1h de refactor.

— Mateus
