"""
Gera PDF estruturado: Documentacao Dashboard CRM Performance v0
Para apresentacao ao Head (Castrin)
"""
from fpdf import FPDF


class PDF(FPDF):
    def header(self):
        if self.page_no() > 1:
            self.set_font("Helvetica", "I", 8)
            self.set_text_color(120, 120, 120)
            self.cell(0, 6, "CRM Performance v0 - Documentacao Tecnica | MultiBet", align="L")
            self.cell(0, 6, f"Pagina {self.page_no()}/{{nb}}", align="R", new_x="LMARGIN", new_y="NEXT")
            self.line(10, 14, 200, 14)
            self.ln(4)

    def titulo(self, num, texto):
        self.ln(4)
        self.set_font("Helvetica", "B", 14)
        self.set_text_color(0, 70, 150)
        self.cell(0, 8, f"{num}. {texto}", new_x="LMARGIN", new_y="NEXT")
        self.set_draw_color(0, 102, 204)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(4)

    def sub(self, texto):
        self.set_font("Helvetica", "B", 11)
        self.set_text_color(60, 60, 60)
        self.cell(0, 7, texto, new_x="LMARGIN", new_y="NEXT")
        self.ln(2)

    def txt(self, texto):
        self.set_font("Helvetica", "", 10)
        self.set_text_color(30, 30, 30)
        self.multi_cell(0, 5.5, texto)
        self.ln(2)

    def bullet(self, texto):
        self.set_font("Helvetica", "", 10)
        self.set_text_color(30, 30, 30)
        self.cell(8, 5.5, "  -")
        self.multi_cell(0, 5.5, texto)

    def tabela(self, headers, rows, widths=None):
        if not widths:
            w = 190 / len(headers)
            widths = [w] * len(headers)
        self.set_font("Helvetica", "B", 8)
        self.set_fill_color(0, 70, 150)
        self.set_text_color(255, 255, 255)
        for i, h in enumerate(headers):
            self.cell(widths[i], 7, h, border=1, fill=True, align="C")
        self.ln()
        self.set_font("Helvetica", "", 8)
        self.set_text_color(30, 30, 30)
        for row in rows:
            for i, val in enumerate(row):
                self.cell(widths[i], 6, str(val), border=1, align="C" if i > 0 else "L")
            self.ln()
        self.ln(3)

    def box(self, cor, titulo_box, linhas):
        cores = {"green": (230, 245, 230, 34, 139, 34), "orange": (255, 240, 230, 200, 100, 50), "blue": (220, 235, 255, 0, 102, 204)}
        bg = cores.get(cor, cores["blue"])
        self.set_fill_color(bg[0], bg[1], bg[2])
        self.set_draw_color(bg[3], bg[4], bg[5])
        h = 8 + len(linhas) * 5.5
        y0 = self.get_y()
        if y0 + h > 280:
            self.add_page()
            y0 = self.get_y()
        self.rect(10, y0, 190, h, style="DF")
        self.set_xy(14, y0 + 2)
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(bg[3], bg[4], bg[5])
        self.cell(0, 6, titulo_box, new_x="LMARGIN", new_y="NEXT")
        self.set_font("Helvetica", "", 9)
        self.set_text_color(30, 30, 30)
        for l in linhas:
            self.set_x(14)
            self.cell(0, 5.5, f"  {l}", new_x="LMARGIN", new_y="NEXT")
        self.ln(4)


def gerar():
    pdf = PDF()
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(auto=True, margin=20)

    # === CAPA ===
    pdf.add_page()
    pdf.ln(25)
    pdf.set_font("Helvetica", "B", 28)
    pdf.set_text_color(0, 70, 150)
    pdf.cell(0, 15, "Dashboard CRM", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 15, "Performance v0", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(6)
    pdf.set_font("Helvetica", "", 14)
    pdf.set_text_color(100, 100, 100)
    pdf.cell(0, 8, "Documentacao Tecnica + Analise CRM", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(15)
    pdf.set_draw_color(0, 102, 204)
    pdf.line(60, pdf.get_y(), 150, pdf.get_y())
    pdf.ln(12)
    info = [
        ("Periodo:", "01/03/2026 a 30/03/2026 (BRT)"),
        ("Autor:", "Mateus F. (Squad Intelligence Engine)"),
        ("Data:", "31/03/2026"),
        ("Status:", "v0 para validacao com CRM"),
        ("Consumidores:", "Raphael M. (CRM), Castrin (Head), CGO/CTO"),
        ("Dashboard:", "localhost:5051 (Flask)"),
    ]
    for label, val in info:
        pdf.set_font("Helvetica", "B", 11)
        pdf.set_text_color(80, 80, 80)
        pdf.cell(35, 7, label, align="R")
        pdf.set_font("Helvetica", "", 11)
        pdf.cell(0, 7, f"  {val}", new_x="LMARGIN", new_y="NEXT")

    # === EXECUTIVE SUMMARY ===
    pdf.add_page()
    pdf.titulo("1", "Executive Summary")
    pdf.txt(
        "Dashboard de performance de campanhas CRM com dados de marco/2026 inteiro. "
        "Extraido de BigQuery (Smartico CRM) e Athena (Data Lake) como CSVs para validacao "
        "rapida antes de automatizar via banco de dados."
    )
    pdf.box("green", "NUMEROS MARCO/2026", [
        "37.110 players unicos na coorte CRM",
        "30.518 players ativos (com turnover)",
        "GGR: R$ 5,15M | NGR: R$ 3,79M | Depositos: R$ 38,3M",
        "Custo CRM Total: R$ 738K (Bonus R$ 363K + Disparos R$ 375K)",
        "ROI (GGR/Custo): 7.0x | ROI (NGR/Custo): 5.1x",
    ])
    pdf.box("orange", "ACHADO CRITICO: CONCENTRACAO VIP", [
        "559 VIPs (1.5% da base) geram R$ 4,39M de NGR = 100%+ da receita liquida",
        "36.551 Standard tem NGR NEGATIVO (-R$ 601K)",
        "Decisao: investir mais em VIP ou reduzir custo de campanhas mass-market?",
    ])
    pdf.box("blue", "PROXIMOS PASSOS", [
        "1. Validar dashboard com Raphael M. (CRM) - coletar feedback",
        "2. Confirmar classificacao de campanhas (IDs 754, 792, 755, 793 = Cashback VIP?)",
        "3. Apos validacao, automatizar pipeline para dados diarios (Super Nova DB)",
    ])

    # === KPIs ===
    pdf.add_page()
    pdf.titulo("2", "KPIs do Dashboard")
    pdf.sub("Linha 1: Resultado da Coorte CRM")
    pdf.tabela(
        ["KPI", "Valor Marco/26", "Calculo"],
        [
            ["Players Ativos", "30.518", "Users com turnover > 0 no ps_bi"],
            ["Depositos Coorte", "R$ 38,3M", "SUM(depositos_brl) da coorte"],
            ["GGR Coorte", "R$ 5,15M", "SUM(ggr_brl) de toda atividade dos players"],
            ["NGR Coorte", "R$ 3,79M", "SUM(ngr_brl) = GGR - BTR"],
            ["Custo CRM Total", "R$ 738K", "BTR (R$ 363K) + Disparos (R$ 375K)"],
            ["ROI (GGR/Custo)", "7.0x", "GGR / Custo CRM Total"],
        ],
        [45, 40, 105],
    )
    pdf.txt(
        "Nota: GGR e a soma de TODA a atividade dos players da coorte CRM, nao apenas "
        "da campanha especifica. Para v1, implementar GGR incremental (pre/pos campanha)."
    )

    # === FUNIL ===
    pdf.titulo("3", "Funil de Conversao CRM")
    pdf.txt(
        "O funil mede a jornada do player desde a oferta de bonus ate a monetizacao efetiva. "
        "Desenhado com base na analise do agente de CRM do squad iGaming."
    )
    pdf.tabela(
        ["Etapa", "O que mede", "Fonte", "Marco/26", "% sobre topo"],
        [
            ["Oferecidos", "Receberam oferta de bonus", "j_bonuses status=1", "105.312", "100%"],
            ["Completaram", "Cumpriram condicao do bonus", "j_bonuses status=3", "103.163", "98,0%"],
            ["Ativados", "Depositaram OU apostaram", "ps_bi (dep+turn>0)", "34.252", "32,5%"],
            ["Monetizados", "Depositaram E apostaram", "ps_bi (dep>0 AND turn>0)", "28.032", "26,6%"],
        ],
        [30, 50, 40, 30, 30],
    )
    pdf.txt(
        "NOTA: A taxa Oferecidos->Completaram e 98% porque a maioria das campanhas sao "
        "popups com opt-in automatico no Smartico. A queda real esta entre Completaram (103K) "
        "e Ativados (34K) - apenas 33% dos que receberam bonus efetivamente jogaram/depositaram."
    )
    pdf.txt(
        "INSIGHT: 69K users (67%) receberam bonus mas NAO tiveram atividade financeira no "
        "periodo. Esse e o gap que o CRM pode atacar com campanhas de reativacao."
    )

    # === TOP CAMPANHAS POR GGR ===
    pdf.titulo("3b", "Top 5 Campanhas por GGR (cruzamento campanha x financeiro)")
    pdf.txt(
        "GGR por campanha foi calculado cruzando a coorte de users (j_bonuses) "
        "com o financeiro individual (ps_bi). O GGR representa TODA a atividade "
        "do player no periodo, nao apenas o jogo da campanha."
    )
    pdf.tabela(
        ["Campaign ID", "Users", "GGR (R$)", "Tipo provavel"],
        [
            ["754", "6.848", "R$ 2.925.779", "Cashback VIP (a confirmar)"],
            ["1403485", "7.913", "R$ 2.040.489", "Lifecycle"],
            ["1340568", "8.748", "R$ 1.376.398", "Lifecycle"],
            ["792", "1.899", "R$ 997.835", "Cashback VIP (a confirmar)"],
            ["2120332", "663", "R$ 922.236", "Cashback"],
        ],
        [30, 25, 40, 95],
    )
    pdf.txt(
        "Os IDs 754 e 792 sozinhos geram R$ 3.9M de GGR (76% do total). "
        "Confirmar com CRM se sao os programas de Cashback VIP."
    )

    # === CSVs ===
    pdf.add_page()
    pdf.titulo("4", "CSVs Extraidos e Fontes")
    pdf.tabela(
        ["CSV", "Fonte", "Registros", "Tamanho"],
        [
            ["campanhas_diarias.csv", "BigQuery j_bonuses", "2.712", "232 KB"],
            ["disparos_custos.csv", "BigQuery j_communication", "180", "12 KB"],
            ["financeiro_coorte.csv", "BigQuery + Athena ps_bi", "231.720", "14 MB"],
            ["top_jogos.csv", "Athena ps_bi + DE-PARA manual", "30", "2 KB"],
            ["vip_groups.csv", "Derivado do financeiro", "37.110", "1.4 MB"],
        ],
        [50, 55, 30, 30],
    )
    pdf.txt("Script de extracao: scripts/extract_crm_csvs_marco.py")
    pdf.txt("Dashboard le CSVs em memoria (< 1 segundo) via dashboards/crm_report/queries_csv.py")

    pdf.sub("4.1 campanhas_diarias.csv")
    pdf.txt(
        "Fonte: BigQuery j_bonuses. Grao: 1 linha por entity_id x dia. "
        "Colunas: report_date, campaign_id, campaign_name, oferecidos (status 1), "
        "completaram (status 3), expiraram (status 4), custo_bonus_brl (BTR), campaign_type."
    )
    pdf.sub("4.2 financeiro_coorte.csv")
    pdf.txt(
        "Coorte: users com bonus_status_id = 3 (completaram) no BigQuery. "
        "Financeiro: Athena ps_bi.fct_player_activity_daily por user x dia. "
        "Bridge: user_ext_id (BQ) = external_id (ps_bi.dim_user). "
        "Valores em BRL reais. is_test = false."
    )
    pdf.sub("4.3 top_jogos.csv + DE-PARA")
    pdf.txt(
        "Users sao ESPECIFICAMENTE da coorte CRM. DE-PARA de 35 jogos mapeados "
        "manualmente (PG Soft IDs numericos + Pragmatic slugs). "
        "Arquivo: data/crm_csvs/depara_jogos.csv. "
        "dim_game do ps_bi tem apenas 414 jogos; PG Soft nao coberto."
    )

    # === CLASSIFICACAO ===
    pdf.add_page()
    pdf.titulo("5", "Classificacao de Campanhas")
    pdf.tabela(
        ["Tipo", "Campanhas", "Oferecidos", "Custo Bonus", "Exemplo"],
        [
            ["Challenge", "1.073", "26.734", "R$ 0", "[PGS] Challenge Fortune Tiger"],
            ["Cashback_VIP", "7", "21.079", "R$ 353K", "IDs 754, 792, 755, 793"],
            ["DailyFS", "103", "17.974", "R$ 8.6K", "Gire e Ganhe Ratinho"],
            ["RETEM", "279", "16.372", "R$ 0", "RETEM Corujao 27/03"],
            ["Lifecycle", "41", "8.835", "R$ 0", "C&S_LifeCycle_2ndDeposit"],
            ["Gamificacao", "4", "8.315", "R$ 0", "IDs 23053, 33670"],
            ["CrossSell_Sports", "14", "1.251", "R$ 498", "Sportsbook_25GatesOfOlympus"],
            ["Reativacao_FTD", "8", "676", "R$ 133", "UsuariosSemFTD_Missoes"],
            ["CX_Recovery", "1", "135", "R$ 0", "CX_ChamadosAbertos_30Giros"],
            ["Sem_Classificacao", "33", "3.183", "R$ 0", "IDs sem nome"],
        ],
        [35, 22, 22, 25, 86],
    )
    pdf.box("orange", "PENDENCIA: Confirmar com CRM", [
        "IDs 754, 792, 755, 793 = 99% do custo bonus (R$ 353K). Sao Cashback VIP?",
        "33 campanhas sem classificacao = IDs sem nome no j_bonuses. O que sao?",
        "Tipo inferido pelo nome da campanha. Ideal: campo dedicado no Smartico.",
    ])

    # === VIP ===
    pdf.titulo("6", "Analise VIP - Concentracao de Receita")
    pdf.tabela(
        ["Tier", "Criterio", "Users", "NGR Total", "NGR/User", "APD"],
        [
            ["Elite", "NGR >= R$ 10.000", "99", "R$ 2,12M", "R$ 21.375", "15.3"],
            ["Key Account", "NGR >= R$ 5.000", "176", "R$ 1,19M", "R$ 6.738", "14.8"],
            ["High Value", "NGR >= R$ 3.000", "284", "R$ 1,09M", "R$ 3.843", "13.2"],
            ["Standard", "NGR < R$ 3.000", "36.551", "-R$ 601K", "-R$ 16", "6.1"],
        ],
        [30, 35, 20, 30, 30, 20],
    )
    pdf.txt(
        "INSIGHT: 559 VIPs (1.5% da base) geram R$ 4,39M de NGR = mais que 100% da receita "
        "liquida. O segmento Standard INTEIRO tem NGR negativo, ou seja, esta sendo subsidiado. "
        "Decisao estrategica: priorizar programas de retencao VIP vs reduzir custo mass-market."
    )

    # === CUSTOS DISPAROS ===
    pdf.add_page()
    pdf.titulo("7", "Orcamento de Disparos")
    pdf.tabela(
        ["Canal", "Provedor", "Custo/envio", "Envios Marco", "Custo Total"],
        [
            ["SMS", "DisparoPro", "R$ 0,045", "2.037.610", "R$ 87.185"],
            ["SMS", "PushFY", "R$ 0,060", "743.040", "R$ 44.582"],
            ["SMS", "Comtele", "R$ 0,063", "2.739.771", "R$ 173.128"],
            ["WhatsApp", "Loyalty", "R$ 0,160", "325.003", "R$ 52.000"],
            ["Push", "PushFY", "R$ 0,060", "1.059.087", "R$ 63.545"],
            ["Popup", "Smartico", "R$ 0,000", "6.965.741", "R$ 0"],
            ["outro", "desconhecido", "R$ 0,000", "2.482.572", "R$ 0"],
            ["TOTAL", "", "", "15.615.052", "R$ 375.513"],
        ],
        [30, 30, 25, 40, 40],
    )
    pdf.box("blue", "OBSERVACOES PARA O CRM", [
        "Popup (Smartico) = maior volume, custo zero. So alcanca base logada.",
        "'outro/desconhecido' = 2.5M envios sem custo. Quais activity_type_ids sao?",
        "Verba mensal aprovada = PENDENTE (campo budget_monthly_brl)",
        "Projecao de custo = R$ 375K/mes em disparos pagos (SMS + WhatsApp + Push)",
    ])

    # === PENDENCIAS ===
    pdf.titulo("8", "Pendencias para o CRM")
    pdf.tabela(
        ["#", "Pendencia", "Responsavel", "Impacto"],
        [
            ["1", "Confirmar IDs 754/792/755/793 = Cashback VIP", "CRM (Raphael)", "99% do custo bonus"],
            ["2", "Mapeamento entity_id -> campanha ativa", "CRM / Smartico", "Filtrar so ativas"],
            ["3", "bonus_cost_value = BTR? Ou BG?", "CRM / Smartico", "Calculo ROI"],
            ["4", "Verba mensal de disparos", "CRM / Financeiro", "% utilizado"],
            ["5", "Custos Popup/Smartico = R$ 0 mesmo?", "CRM", "Custo real"],
            ["6", "Classificacao de campanhas sem nome", "CRM", "33 sem tipo"],
            ["7", "Definicao de inativo (15d?)", "CRM", "Modulo recuperacao"],
        ],
        [8, 75, 42, 65],
    )

    # === ROADMAP ===
    pdf.titulo("9", "Roadmap pos-validacao")
    pdf.tabela(
        ["Fase", "Entrega", "Estimativa"],
        [
            ["v0 (atual)", "Dashboard com CSVs + documentacao para CRM avaliar", "Pronto"],
            ["v0.5", "Corrigir pendencias CRM + comparativo antes/durante/depois", "3-5 dias"],
            ["v1", "Pipeline automatizada D+1 + Super Nova DB + GGR incremental", "1-2 semanas"],
            ["v2", "Dashboard Flask em producao (EC2) + alertas Slack", "2-3 semanas"],
        ],
        [25, 115, 40],
    )

    # === RASTREABILIDADE DE DADOS ===
    pdf.add_page()
    pdf.titulo("10", "Rastreabilidade: Fonte -> CSV -> Futuro Banco")
    pdf.txt(
        "Cada dado do dashboard tem rastreabilidade completa: de qual tabela/coluna "
        "foi extraido, em qual CSV esta, e para qual tabela do Super Nova DB vai."
    )
    pdf.tabela(
        ["Metrica", "Fonte (tabela.coluna)", "CSV", "Destino Super Nova DB"],
        [
            ["Oferecidos", "BQ j_bonuses.bonus_status_id=1", "campanhas_diarias", "crm_campaign_daily"],
            ["Completaram", "BQ j_bonuses.bonus_status_id=3", "campanhas_diarias", "crm_campaign_daily"],
            ["Custo Bonus", "BQ j_bonuses.bonus_cost_value", "campanhas_diarias", "crm_campaign_daily"],
            ["GGR", "Athena ps_bi.fct_player_activity_daily.ggr_base", "financeiro_coorte", "crm_campaign_daily"],
            ["NGR", "Athena ps_bi.fct_player_activity_daily.ngr_base", "financeiro_coorte", "crm_campaign_daily"],
            ["Turnover", "Athena ps_bi.fct_player_activity_daily.bet_amount_base", "financeiro_coorte", "crm_campaign_daily"],
            ["Depositos", "Athena ps_bi...deposit_success_base", "financeiro_coorte", "crm_campaign_daily"],
            ["Saques", "Athena ps_bi...cashout_success_base", "financeiro_coorte", "crm_campaign_daily"],
            ["Disparos", "BQ j_communication (fact_type_id=1)", "disparos_custos", "crm_dispatch_budget"],
            ["Top Jogos", "Athena ps_bi.fct_casino_activity_daily", "top_jogos", "crm_campaign_game_daily"],
            ["VIP Tier", "Derivado NGR acumulado", "vip_groups", "crm_player_vip_tier"],
            ["Nome Jogo", "ps_bi.dim_game + depara_jogos.csv", "top_jogos", "dim_games_catalog"],
        ],
        [25, 65, 40, 50],
    )
    pdf.txt(
        "IMPORTANTE: As tabelas destino no Super Nova DB (schema multibet) ja existem "
        "com DDL criado (pipelines/ddl_crm_report.py). Apos validacao do CRM, basta "
        "adaptar o script de extracao para persistir direto no banco em vez de CSV."
    )
    pdf.txt("Legenda completa: data/crm_csvs/LEGENDA_crm_csvs.txt")

    # === ACOES SUGERIDAS ===
    pdf.titulo("11", "Acoes Sugeridas por Segmento")
    pdf.tabela(
        ["Segmento", "Situacao", "Acao Recomendada"],
        [
            ["VIP Elite (99 users)", "NGR R$ 2,12M, APD 22.9", "Programa VIP exclusivo, account manager dedicado"],
            ["VIP Key Account (176)", "NGR R$ 1,19M, APD 14.8", "Upgrade path para Elite, ofertas personalizadas"],
            ["VIP High Value (284)", "NGR R$ 1,09M, APD 13.2", "Retencao ativa, alertas de churn"],
            ["Standard (36.551)", "NGR -R$ 601K", "Reduzir custo de campanhas mass-market; focar em ativacao"],
            ["Inativos (69K gap)", "Receberam bonus sem atividade", "Campanha de reativacao por SMS/WhatsApp"],
            ["Cashback VIP (IDs 754/792)", "76% do GGR total", "Manter e otimizar; maior ROI da operacao"],
            ["Challenge (1.073 camps)", "Zero custo de bonus", "Continuar; gera engajamento sem custo"],
        ],
        [42, 50, 98],
    )

    # === GLOSSARIO ===
    pdf.titulo("12", "Glossario")
    pdf.tabela(
        ["Termo", "Definicao"],
        [
            ["GGR", "Gross Gaming Revenue = Apostas - Ganhos dos jogadores"],
            ["NGR", "Net Gaming Revenue = GGR - Bonus Turned Real (BTR)"],
            ["BTR", "Bonus Turned Real = bonus convertido em dinheiro real"],
            ["ARPU", "Average Revenue Per User = GGR / Players ativos"],
            ["APD", "Average Play Days = media de dias com atividade"],
            ["RTP", "Return to Player = % do turnover devolvido ao jogador"],
            ["ROI", "Return on Investment = GGR / Custo CRM Total"],
            ["Coorte CRM", "Users que completaram campanhas (j_bonuses status=3)"],
            ["Net Deposit", "Depositos - Saques = aporte liquido real"],
        ],
        [30, 160],
    )

    # === SALVAR ===
    out = "c:/Users/NITRO/OneDrive - PGX/MultiBet/docs/documentacao_dashboard_crm_v0.pdf"
    pdf.output(out)
    print(f"PDF gerado: {out}")
    print(f"Paginas: {pdf.pages_count}")


if __name__ == "__main__":
    gerar()
