-- =====================================================================
-- Backfill: corrige strings "NaN"/"nan"/"None" em fact_casino_rounds
-- Data: 2026-04-22
-- Motivo:
--   Pipeline `fact_casino_rounds.py` (ate commit anterior) usava
--   `str(row["vendor_id"] or "unknown")`. Para NULL do Athena, pandas
--   entrega numpy.nan (float truthy) -> `NaN or "unknown"` = NaN ->
--   `str(NaN)` = "nan". Ao inserir, virava string literal no Postgres,
--   escapando do COALESCE do dashboard (app.py Supernova-Dashboard)
--   e aparecendo como "NaN" no Top 10 Providers / Top 20 Jogos.
--
-- Fix definitivo no pipeline (commit de hoje): df.fillna(...) + replace
-- antes do insert. Esse backfill limpa o que ja esta gravado.
--
-- Como rodar:
--   psql -h supernova-db...us-east-1.rds.amazonaws.com -U <user> \
--     -d supernova -f backfill_fix_nan_strings.sql
--
-- Seguro: apenas UPDATE pontual nas linhas afetadas (nao-destrutivo).
-- =====================================================================

BEGIN;

-- 1. Diagnostico pre-fix (quantas linhas seriam afetadas)
SELECT 'vendor_id'     AS coluna, COUNT(*) AS linhas_nan
  FROM multibet.fact_casino_rounds
 WHERE vendor_id IN ('NaN','nan','None')
UNION ALL
SELECT 'sub_vendor_id', COUNT(*)
  FROM multibet.fact_casino_rounds
 WHERE sub_vendor_id IN ('NaN','nan','None')
UNION ALL
SELECT 'game_category', COUNT(*)
  FROM multibet.fact_casino_rounds
 WHERE game_category IN ('NaN','nan','None')
UNION ALL
SELECT 'game_name',     COUNT(*)
  FROM multibet.fact_casino_rounds
 WHERE game_name IN ('NaN','nan','None');

-- 2. Fix vendor_id
UPDATE multibet.fact_casino_rounds
   SET vendor_id = 'unknown'
 WHERE vendor_id IN ('NaN','nan','None');

-- 3. Fix sub_vendor_id
UPDATE multibet.fact_casino_rounds
   SET sub_vendor_id = ''
 WHERE sub_vendor_id IN ('NaN','nan','None');

-- 4. Fix game_category
UPDATE multibet.fact_casino_rounds
   SET game_category = 'Outros'
 WHERE game_category IN ('NaN','nan','None');

-- 5. Fix game_name (preserva 'Desconhecido' que o pipeline ja usa como default)
UPDATE multibet.fact_casino_rounds
   SET game_name = 'Desconhecido'
 WHERE game_name IN ('NaN','nan','None');

-- 6. Validacao pos-fix (deve retornar 0 em todas)
SELECT 'vendor_id'     AS coluna, COUNT(*) AS linhas_nan_restantes
  FROM multibet.fact_casino_rounds
 WHERE vendor_id IN ('NaN','nan','None')
UNION ALL
SELECT 'sub_vendor_id', COUNT(*)
  FROM multibet.fact_casino_rounds
 WHERE sub_vendor_id IN ('NaN','nan','None')
UNION ALL
SELECT 'game_category', COUNT(*)
  FROM multibet.fact_casino_rounds
 WHERE game_category IN ('NaN','nan','None')
UNION ALL
SELECT 'game_name',     COUNT(*)
  FROM multibet.fact_casino_rounds
 WHERE game_name IN ('NaN','nan','None');

-- 7. Conferencia: dashboard deve voltar a ter Aviator/Fortune Rabbit em 1 linha so
SELECT game_name, vendor_id, sub_vendor_id,
       SUM(total_rounds) AS rodadas,
       SUM(ggr_real)     AS ggr_brl
  FROM multibet.fact_casino_rounds
 WHERE dt >= CURRENT_DATE - INTERVAL '30 days'
   AND game_name IN ('Aviator','Fortune Rabbit')
 GROUP BY game_name, vendor_id, sub_vendor_id
 ORDER BY game_name, rodadas DESC;

COMMIT;
