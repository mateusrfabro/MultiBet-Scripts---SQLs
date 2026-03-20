README: Unificação TAP + Cruzamento com PGS (Usuários)

Objetivo
- Unificar os 2 arquivos de usuários reportados na Smartico TAP em uma única base.
- Verificar se existe repetição de usuários entre os 2 arquivos (e evidenciar).
- Cruzar TAP vs PGS e entregar a lista de usuários que estão na PGS mas não aparecem na TAP.
- Entregar a quantidade de jogadores por status de conta na PGS (RG).

O que foi feito (resumo)
1) Unificação TAP (Smartico)
- Arquivos recebidos: base_restante.csv e lista_email.csv
- Identificador de usuário usado: user_ext_id
- Resultado da validação: NÃO há repetição de usuários entre os arquivos (overlap = 0).
  Ou seja: nenhum user_ext_id aparece nos dois arquivos ao mesmo tempo.
- Como as colunas também são iguais, a unificação correta foi feita juntando as linhas (UNION/append).
- Entregável: tap_unified_union.csv

2) Cruzamento PGS x TAP (gap de reporte)
- Comparação de usuários por identificador:
  PGS.ext_id vs TAP.user_ext_id
- Resultado: 2.225 usuários existem na PGS e não aparecem na TAP.
- Entregável: pgs_not_in_tap_by_ext_id.csv
- Resumo numérico do cruzamento: pgs_vs_tap_summary.txt

3) Análise adicional — jogadores por status de conta (PGS)
- Status solicitados: rg_closed e rg_cooloff
- Campos equivalentes na base PGS:
  rg_closed → is_rg_closed
  rg_cooloff → rg_cool_off_status
- Contagens entregues em 2 formatos:
  a) Por status/valor (mais simples): pgs_status_counts_by_flag.csv
  b) Por combinação (mostra sobreposição entre status): pgs_status_counts_by_combination.csv

4) Follow-up — Análise de account_category (PGS)
- Coluna: account_category (já presente na base PGS)
- Distribuição encontrada:
  real_user:    701.680 (65,53%)
  play_user:    281.735 (26,31%)
  suspended:     37.467 (3,50%)
  closed:        37.338 (3,49%)
  rg_closed:      9.143 (0,85%)
  rg_cool_off:    1.803 (0,17%)
  fraud:          1.615 (0,15%)
- Entregável: pgs_account_category_counts.csv

5) Follow-up — Double check PGS x TAP
- Total de jogadores PGS:          1.070.781
- Total de jogadores TAP:          3.762.529
- PGS encontrados na TAP:          1.068.556
- PGS NÃO encontrados na TAP:          2.225
- Confirmação: os 2.225 estão corretos.
  A TAP possui mais registros que a PGS porque inclui jogadores
  que existem na Smartico mas não na base PGS.
- Entregável: followup_summary.txt

6) Bônus — Cruzamento account_category x presença na TAP
- Mostra quantos jogadores de cada status estão ou não na TAP.
- Dos 2.225 não encontrados na TAP:
  real_user: 1.334 (60%) | play_user: 655 (29%) | closed: 140 |
  suspended: 66 | rg_closed: 20 | rg_cool_off: 8 | fraud: 2
- Entregável: pgs_account_category_vs_tap_pivot.csv

Principais resultados
- TAP:
  base_restante.csv: 2.713.954 linhas
  lista_email.csv: 1.048.575 linhas
  overlap entre os dois (user_ext_id em ambos): 0
- PGS x TAP:
  Total PGS: 1.070.781 | Total TAP: 3.762.529
  PGS encontrados na TAP: 1.068.556
  PGS não encontrados na TAP: 2.225
- Status PGS (is_rg_closed / rg_cool_off_status):
  is_rg_closed: 0=1.061.657 | 1=9.124
  rg_cool_off_status: not set=1.034.213 | active=33.485 | inactive=3.083
- Account Category PGS:
  real_user=701.680 | play_user=281.735 | suspended=37.467 |
  closed=37.338 | rg_closed=9.143 | rg_cool_off=1.803 | fraud=1.615

Arquivos anexos (outputs finais)
- report.txt
  Evidência da unificação TAP (overlap=0 e colunas idênticas).
- tap_unified_union.csv
  Base TAP unificada (resultado final).
- pgs_vs_tap_summary.txt
  Resumo numérico do cruzamento PGS x TAP.
- pgs_not_in_tap_by_ext_id.csv
  Lista de usuários que estão na PGS e não aparecem na TAP (principal pedido).
- pgs_status_counts_by_flag.csv
  Quantidade de jogadores por status/valor na PGS.
- pgs_status_counts_by_combination.csv
  Quantidade de jogadores por combinação de status na PGS.
- pgs_account_category_counts.csv
  Distribuição de jogadores por account_category na PGS.
- pgs_account_category_vs_tap_pivot.csv
  Cruzamento account_category x presença na TAP.
- followup_summary.txt
  Resumo consolidado das análises do follow-up.