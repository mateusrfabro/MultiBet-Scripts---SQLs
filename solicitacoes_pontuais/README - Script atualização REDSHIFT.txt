Monitor de Freshness (Redshift) — ECR & BIREPORTS

Objetivo:

Este script mede o frescor (freshness) de tabelas chave nos schemas ecr e bireports, retornando:

1. Última atualização da tabela (via MAX(timestamp) da coluna de update)

2. Última atualização convertida para America/Sao_Paulo

3. Relógio atual do DW (via GETDATE())

4. Atraso em minutos entre o relógio do DW e a última atualização convertida

A ideia é responder: “essas tabelas estão atualizando ou travaram?” com um indicador simples de atraso.

Como funciona:

1. A CTE freshness consolida um bloco por tabela, cada um retornando:

alvo (ex.: ecr.tbl_ecr)

ultima_atualizacao_tabela = MAX(coluna_timestamp)

2. A CTE clock pega uma única vez o horário do banco:

relogio_atual_dw = GETDATE()

3. No SELECT final:

converte ultima_atualizacao_tabela para America/Sao_Paulo

calcula atraso_minutos com DATEDIFF