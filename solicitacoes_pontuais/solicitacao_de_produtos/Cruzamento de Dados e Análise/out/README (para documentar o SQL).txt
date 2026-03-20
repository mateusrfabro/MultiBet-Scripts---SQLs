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

4) Follow-up — Análise de account_category (Redshift — 10/03/2026)
- Fonte: query direta no Redshift (coluna c_category em bireports.tbl_ecr)
- Distribuição encontrada:
  real_user:    688.169 (63,49%)
  play_user:    303.822 (28,03%)
  closed:        41.214 (3,80%)
  suspended:     37.467 (3,46%)
  rg_closed:      9.595 (0,89%)
  rg_cool_off:    2.007 (0,19%)
  fraud:          1.616 (0,15%)
- Total PGS (Redshift): 1.083.890
- Entregável: pgs_account_category_counts.csv

5) Follow-up — Double check PGS x TAP (Redshift 10/03 vs TAP local)
- Total de jogadores PGS (Redshift):    1.083.890
- Total de jogadores TAP (arquivo):     3.762.529
- PGS encontrados na TAP:               1.068.556
- PGS NÃO encontrados na TAP:              15.334
- Nota: na entrega anterior (base local de 04/03) eram 2.225.
  A diferença (15.334 - 2.225 = 13.109) corresponde a novos cadastros
  entre 04/03 e 10/03 que ainda não constam na TAP.
- Entregável: followup_summary.txt

6) Cruzamento account_category x presença na TAP
- Mostra quantos jogadores de cada status estão ou não na TAP.
- Dos 15.334 não encontrados na TAP:
  play_user: 6.330 | real_user: 5.748 | closed: 2.938 |
  rg_closed: 168 | rg_cool_off: 82 | suspended: 66 | fraud: 2
- Entregável: pgs_account_category_vs_tap_pivot.csv

Principais resultados
- TAP:
  base_restante.csv: 2.713.954 linhas
  lista_email.csv: 1.048.575 linhas
  overlap entre os dois (user_ext_id em ambos): 0
- PGS x TAP (Redshift 10/03/2026):
  Total PGS: 1.083.890 | Total TAP: 3.762.529
  PGS encontrados na TAP: 1.068.556
  PGS não encontrados na TAP: 15.334
- Status PGS (is_rg_closed / rg_cool_off_status — base local 04/03):
  is_rg_closed: 0=1.061.657 | 1=9.124
  rg_cool_off_status: not set=1.034.213 | active=33.485 | inactive=3.083
- Account Category PGS (Redshift 10/03/2026):
  real_user=688.169 | play_user=303.822 | closed=41.214 |
  suspended=37.467 | rg_closed=9.595 | rg_cool_off=2.007 | fraud=1.616

Arquivos anexos (outputs finais)
- report.txt
  Evidência da unificação TAP (overlap=0 e colunas idênticas).
- tap_unified_union.csv
  Base TAP unificada (resultado final).
- pgs_vs_tap_summary.txt
  Resumo numérico do cruzamento PGS x TAP.
- pgs_not_in_tap_by_ext_id.csv
  Lista de usuários que estão na PGS e não aparecem na TAP (base local 04/03, 2.225 registros).
- pgs_status_counts_by_flag.csv
  Quantidade de jogadores por status/valor na PGS.
- pgs_status_counts_by_combination.csv
  Quantidade de jogadores por combinação de status na PGS.
- pgs_account_category_counts.csv
  Distribuição de jogadores por account_category (Redshift 10/03).
- pgs_account_category_vs_tap_pivot.csv
  Cruzamento account_category x presença na TAP (Redshift 10/03).
- followup_summary.txt
  Resumo consolidado das análises do follow-up (Redshift 10/03).