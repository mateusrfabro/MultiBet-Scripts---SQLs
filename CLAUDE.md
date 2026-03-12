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
- **Banco de dados:** AWS Redshift (read-only) — entregue pela Pragmatic Solutions
- **CRM:** BigQuery da Smartico
- **IDE:** VS Code, DBeaver
- **Versionamento:** GitHub
- **Linguagens:** Python, SQL
- **Frontend:** Flask + HTML + CSS (API chama os dados via request, Flask 
  interpreta o HTML, gera arquivo index.html como página)

## Como devo ser ajudado
1. **SQL:** sempre otimizado, com comentários explicando cada bloco, 
   pensando que o banco é read-only no Redshift
2. **Python:** código limpo, com logs e tratamento de erros, 
   sempre explicando o racional
3. **Análises:** explique o porquê das decisões, não só o como
4. **Dashboards:** padrão Flask + HTML + CSS + API
5. **Sempre pergunte** se não ficou claro o que é pedido antes de sair fazendo
6. **Entregue além:** sugira melhorias, aponte riscos, pense como um gestor
7. **Me ensine enquanto faz:** quero entender, não só copiar

## Contexto dos bancos de dados
- **Redshift (Pragmatic Solutions):** use a documentação e IA da Pragmatic
  para mapear schemas, tabelas e colunas mais confiáveis. Sempre priorize
  confiabilidade dos dados.
- **BigQuery (Smartico):** CRM, use a documentação disponível.
- Quando não souber a estrutura exata, pergunte ou sugira que eu consulte
  a documentação antes de montar a query.

### Regra de fuso horário (OBRIGATÓRIA)
- **O cluster Redshift opera em UTC.** Toda query que extraia, filtre ou
  exiba dados com timestamp do Redshift DEVE converter para BRT usando:
  `CONVERT_TIMEZONE('UTC', 'America/Sao_Paulo', coluna_timestamp)`
- Isso vale para SELECTs, WHEREs com data, relatórios, exports, pipelines
  — sem exceção.
- Nunca retorne timestamps crus (UTC) em entregas finais ao negócio.

## Meu objetivo com cada entrega
Quero ser reconhecido pelo time, mostrar capacidade de gestão e crescer 
na empresa. Cada entrega deve ser sólida, bem documentada e com raciocínio 
claro — como um analista sênior entregaria.

## Super Nova Bet (Paquistão)
Em breve iniciaremos operações. Quando houver demandas relacionadas, 
leia a documentação disponível e contribua com opiniões sobre boas práticas.