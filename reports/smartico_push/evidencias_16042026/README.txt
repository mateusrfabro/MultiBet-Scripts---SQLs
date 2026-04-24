Evidencias da validacao manual no CRM Smartico - 16/04/2026
================================================================
Responsavel: Mateus Fabro

Prints capturados do painel Smartico apos o push automatico das
02:30 BRT, validando que os 3 casos de teste selecionados do CSV
smartico_sent_2026-04-16.csv refletem 1:1 no perfil do jogador.

Casos validados (todos PASS):
  1) user_ext_id 765211773344155 (Ednelson Tobias)
     - Smartico ID 276105200
     - Cenario: GANHOU tag RISK_NON_PROMO_PLAYER, mantem TIER_BOM
     - Observado: 9 tags exatas no External markers
     - Arquivo sugerido: caso1_ganhou_tag_765211773344155.png

  2) user_ext_id 28011354 (Andressa Carvalho)
     - Smartico ID 235842086
     - Cenario: PERDEU tag RISK_SLEEPER_LOW_PLAYER, mantem TIER_BOM
     - Observado: 2 tags (ENGAGED_PLAYER, TIER_BOM) - tag removida sumiu
     - Arquivo sugerido: caso2_perdeu_tag_28011354.png

  3) user_ext_id 945271775656588 (Jossie Costa)
     - Smartico ID 278560742
     - Cenario: TROCOU tier de Bom para Mediano
     - Observado: 3 tags (BEHAV_SLOTGAMER, RG_ALERT_PLAYER, TIER_MEDIANO)
                  - TIER_BOM e NON_PROMO_PLAYER removidas com sucesso
     - Arquivo sugerido: caso3_trocou_tier_945271775656588.png

Documentacao consolidada: docs/validacao_smartico_push_abr2026.md

Observacao: salvar os PNGs neste diretorio com os nomes sugeridos
para o doc conseguir referenciar no futuro (link relativo).
