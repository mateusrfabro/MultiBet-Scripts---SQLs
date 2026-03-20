README | Análise de Safra FTD (Fevereiro 2026):

1. Objetivo Técnico
Extrair o comportamento de retenção e LTV (Lifetime Value) da safra de novos depositantes (FTDs) do mês de Fevereiro de 2026, vinculados ao afiliado/tracker 297657. A query foi desenhada para garantir a exclusividade mútua de faixas: cada jogador é contabilizado em apenas um bucket conforme sua frequência exata de depósitos no período.

2. Arquitetura do SQL
A solução utiliza uma abordagem de Single-Pass Scan via Redshift, otimizada para alta performance em volumes massivos de dados:

Identificação de Safra: Utiliza a função de janela MIN() OVER para carimbar o nascimento financeiro de cada usuário de forma atômica.

Filtros de Data: Implementados na CTE inicial para garantir buscas SARGable e evitar Full Table Scans.

Normalização Cambial: Divisão obrigatória por 100.0 para converter centavos (padrão Pragmatic) em Reais (BRL).

Formatação Nativa: Uso de REPLACE e TO_CHAR para entregar a vírgula decimal brasileira, eliminando erros de escala em 100x no Excel.

3. Regras de Negócio Aplicadas
Afiliado Isolado: Filtra rigorosamente o ID 297657 nas colunas c_affiliate_id e c_tracker_id.

Isolamento de Safra: Apenas jogadores cujo primeiríssimo depósito na história ocorreu em Fevereiro entram no relatório.

Buckets de Recorrência: Agrupamento por quantidade exata de depósitos confirmados dentro do mês de Fevereiro.

4. Protocolo de Exportação (DBeaver → Excel)
Para que os dados sejam lidos corretamente sem necessidade de Power Query ou ajustes manuais:

Formato: Escolha CSV na exportação do DBeaver.

Delimitador: Selecione obrigatoriamente ; (Ponto e Vírgula).

Configurações: Desmarque a opção "Formatar números" []. O SQL já realiza a formatação de vírgula necessária para o Excel brasileiro.

Abertura: Basta dar um duplo clique no arquivo final. O Excel reconhecerá os valores monetários (ex: 830511,47) instantaneamente.