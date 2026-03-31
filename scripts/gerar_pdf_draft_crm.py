"""
Gera PDF do draft do Report Diário CRM.
Usa fpdf2 para criar um documento profissional.

Uso:
    python scripts/gerar_pdf_draft_crm.py
"""

import sys
sys.path.insert(0, "c:/Users/NITRO/OneDrive - PGX/MultiBet")

from fpdf import FPDF


class PDFReport(FPDF):
    """PDF customizado com header/footer profissional."""

    def header(self):
        self.set_font("Helvetica", "B", 9)
        self.set_text_color(120, 120, 120)
        self.cell(0, 6, "DRAFT - Report Diario de Performance CRM | Squad Intelligence Engine", align="C")
        self.ln(8)
        self.set_draw_color(0, 102, 204)
        self.set_line_width(0.5)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(4)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(150, 150, 150)
        self.cell(0, 10, f"Pagina {self.page_no()}/{{nb}} | 25/03/2026 | Mateus F.", align="C")

    def titulo_secao(self, num, texto):
        self.set_font("Helvetica", "B", 14)
        self.set_text_color(0, 70, 150)
        self.cell(0, 10, f"{num}. {texto}", new_x="LMARGIN", new_y="NEXT")
        self.set_draw_color(0, 102, 204)
        self.set_line_width(0.3)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(4)

    def subtitulo(self, texto):
        self.set_font("Helvetica", "B", 11)
        self.set_text_color(50, 50, 50)
        self.cell(0, 8, texto, new_x="LMARGIN", new_y="NEXT")
        self.ln(1)

    def texto(self, txt):
        self.set_font("Helvetica", "", 10)
        self.set_text_color(30, 30, 30)
        self.multi_cell(0, 5.5, txt)
        self.ln(2)

    def texto_bold(self, txt):
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(30, 30, 30)
        self.multi_cell(0, 5.5, txt)
        self.ln(1)

    def bullet(self, txt):
        self.set_font("Helvetica", "", 10)
        self.set_text_color(30, 30, 30)
        self.cell(0, 5.5, f"  -  {txt}", new_x="LMARGIN", new_y="NEXT")

    def tabela(self, headers, rows, col_widths=None):
        if col_widths is None:
            n = len(headers)
            col_widths = [190 / n] * n

        # Header
        self.set_font("Helvetica", "B", 9)
        self.set_fill_color(0, 70, 150)
        self.set_text_color(255, 255, 255)
        for i, h in enumerate(headers):
            self.cell(col_widths[i], 7, h, border=1, fill=True, align="C")
        self.ln()

        # Rows
        self.set_font("Helvetica", "", 9)
        self.set_text_color(30, 30, 30)
        fill = False
        for row in rows:
            if fill:
                self.set_fill_color(240, 245, 255)
            else:
                self.set_fill_color(255, 255, 255)

            max_h = 7
            # Calculate max height needed
            for i, cell_text in enumerate(row):
                lines = self.multi_cell(col_widths[i], 7, str(cell_text), dry_run=True, output="LINES")
                h = len(lines) * 7
                if h > max_h:
                    max_h = h

            x_start = self.get_x()
            y_start = self.get_y()

            # Check page break
            if y_start + max_h > 270:
                self.add_page()
                # Re-draw header
                self.set_font("Helvetica", "B", 9)
                self.set_fill_color(0, 70, 150)
                self.set_text_color(255, 255, 255)
                for i, h in enumerate(headers):
                    self.cell(col_widths[i], 7, h, border=1, fill=True, align="C")
                self.ln()
                self.set_font("Helvetica", "", 9)
                self.set_text_color(30, 30, 30)
                y_start = self.get_y()

            if fill:
                self.set_fill_color(240, 245, 255)
            else:
                self.set_fill_color(255, 255, 255)

            for i, cell_text in enumerate(row):
                self.set_xy(x_start + sum(col_widths[:i]), y_start)
                self.multi_cell(col_widths[i], 7, str(cell_text), border=1, fill=True)

            self.set_xy(x_start, y_start + max_h)
            fill = not fill
        self.ln(4)

    def destaque(self, txt):
        self.set_font("Helvetica", "B", 10)
        self.set_fill_color(255, 248, 220)
        self.set_text_color(150, 100, 0)
        self.multi_cell(0, 6, f"  {txt}", fill=True)
        self.set_text_color(30, 30, 30)
        self.ln(3)

    def tag_status(self, status, cor="green"):
        cores = {
            "green": (34, 139, 34),
            "yellow": (200, 150, 0),
            "red": (200, 50, 50),
            "blue": (0, 102, 204),
        }
        r, g, b = cores.get(cor, (100, 100, 100))
        self.set_font("Helvetica", "B", 9)
        self.set_text_color(r, g, b)
        self.cell(0, 5, status, new_x="LMARGIN", new_y="NEXT")
        self.set_text_color(30, 30, 30)


def gerar_pdf():
    pdf = PDFReport()
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(auto=True, margin=20)

    # --- CAPA ---
    pdf.add_page()
    pdf.ln(30)
    pdf.set_font("Helvetica", "B", 28)
    pdf.set_text_color(0, 70, 150)
    pdf.cell(0, 15, "Report Diario de", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 15, "Performance CRM", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(8)
    pdf.set_font("Helvetica", "", 14)
    pdf.set_text_color(100, 100, 100)
    pdf.cell(0, 8, "Draft de Planejamento e Gap Analysis", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(20)

    pdf.set_draw_color(0, 102, 204)
    pdf.set_line_width(0.8)
    pdf.line(60, pdf.get_y(), 150, pdf.get_y())
    pdf.ln(15)

    info = [
        ("Task:", "86ag3994u (Board BI)"),
        ("Autor:", "Mateus F. (Squad Intelligence Engine)"),
        ("Data:", "25/03/2026"),
        ("Status:", "Draft para validacao com Head"),
        ("Consumidores:", "Time CRM (Raphael M.), BI, CGO/CTO"),
    ]
    for label, val in info:
        pdf.set_font("Helvetica", "B", 11)
        pdf.set_text_color(80, 80, 80)
        pdf.cell(40, 7, label, align="R")
        pdf.set_font("Helvetica", "", 11)
        pdf.cell(0, 7, f"  {val}", new_x="LMARGIN", new_y="NEXT")

    # --- EXECUTIVE SUMMARY ---
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 16)
    pdf.set_text_color(0, 70, 150)
    pdf.cell(0, 10, "Executive Summary", new_x="LMARGIN", new_y="NEXT")
    pdf.set_draw_color(0, 102, 204)
    pdf.set_line_width(0.5)
    pdf.line(10, pdf.get_y(), 200, pdf.get_y())
    pdf.ln(6)

    pdf.set_font("Helvetica", "", 11)
    pdf.set_text_color(30, 30, 30)
    pdf.multi_cell(0, 6,
        "O time de CRM executa campanhas diarias (RETEM, DailyFS, Cashback, Torneios, "
        "Freebets) sem uma visao consolidada de resultados. Este projeto entrega um "
        "report diario automatizado com funil, financeiro, ROI e comparativo por campanha."
    )
    pdf.ln(4)

    # Status box
    pdf.set_fill_color(230, 245, 230)
    pdf.set_draw_color(34, 139, 34)
    pdf.set_line_width(0.3)
    y0 = pdf.get_y()
    pdf.rect(10, y0, 190, 42, style="DF")
    pdf.set_xy(14, y0 + 3)
    pdf.set_font("Helvetica", "B", 11)
    pdf.set_text_color(34, 100, 34)
    pdf.cell(0, 6, "O QUE JA TEMOS PRONTO", new_x="LMARGIN", new_y="NEXT")
    pdf.set_x(14)
    pdf.set_font("Helvetica", "", 10)
    pdf.set_text_color(30, 30, 30)
    prontos = [
        "Tabela fact_crm_daily_performance no Super Nova DB (DDL + JSONB + indexes)",
        "2 pipelines funcionais (BigQuery coorte + funil + Redshift financeiro)",
        "6 regras de negocio validadas (duplo filtro, funil, sub-fund, test users)",
        "Conexoes BigQuery + Athena + Super Nova DB operacionais",
        "14 tabelas de suporte no schema multibet (casino, sports, games, FTD...)",
    ]
    for p in prontos:
        pdf.set_x(14)
        pdf.cell(0, 5.5, f"  -  {p}", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(6)

    # Gap box
    pdf.set_fill_color(255, 240, 230)
    pdf.set_draw_color(200, 100, 50)
    y0 = pdf.get_y()
    pdf.rect(10, y0, 190, 32, style="DF")
    pdf.set_xy(14, y0 + 3)
    pdf.set_font("Helvetica", "B", 11)
    pdf.set_text_color(180, 80, 30)
    pdf.cell(0, 6, "4 GAPS CRITICOS PARA RESOLVER", new_x="LMARGIN", new_y="NEXT")
    pdf.set_x(14)
    pdf.set_font("Helvetica", "", 10)
    pdf.set_text_color(30, 30, 30)
    gaps = [
        "G1. Migrar fonte financeira de Redshift (descontinuado) para Athena/ps_bi",
        "G2. Evoluir grao de campanha+period para campanha+dia (report diario)",
        "G3. Implementar 6 recortes financeiros (Geral, Casino, Sports, Segmento, Jogo, Campanha)",
        "G4. Parametrizar custos por provedor (SMS DisparosPro/Pushfy, WhatsApp Loyalty)",
    ]
    for g in gaps:
        pdf.set_x(14)
        pdf.cell(0, 5.5, f"  -  {g}", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(6)

    # Plano resumo
    pdf.set_font("Helvetica", "B", 11)
    pdf.set_text_color(0, 70, 150)
    pdf.cell(0, 7, "PLANO EM 3 FASES", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 10)
    pdf.set_text_color(30, 30, 30)
    pdf.tabela(
        ["Fase", "Escopo", "Prazo estimado"],
        [
            ["1. Fundacao", "Migrar Athena + grao diario + 6 recortes + piloto RETEM", "~1 semana"],
            ["2. Enriquecimento", "Opt-in, segmentacoes, VIP, comparativo M-1, budget", "~1 semana"],
            ["3. Avancado", "Recuperacao, casino detalhado, meta vs real, dashboard", "~2 semanas"],
        ],
        [35, 110, 45],
    )

    # Quick win box
    pdf.set_fill_color(220, 235, 255)
    pdf.set_draw_color(0, 102, 204)
    y0 = pdf.get_y()
    pdf.rect(10, y0, 190, 28, style="DF")
    pdf.set_xy(14, y0 + 3)
    pdf.set_font("Helvetica", "B", 11)
    pdf.set_text_color(0, 70, 150)
    pdf.cell(0, 6, "QUICK WIN - ENTREGA ESTA SEMANA", new_x="LMARGIN", new_y="NEXT")
    pdf.set_x(14)
    pdf.set_font("Helvetica", "", 10)
    pdf.set_text_color(30, 30, 30)
    pdf.set_x(14)
    pdf.multi_cell(182, 5.5,
        "Migrar a pipeline CRM para Athena e rodar o piloto com a campanha RETEM, "
        "entregando um report Excel com funil completo (8 etapas) + financeiro "
        "(GGR/NGR/depositos) + comparativo BEFORE/DURING/AFTER com dados reais. "
        "Isso ja valida a Fase 1 e gera valor imediato para o time de CRM."
    )
    pdf.ln(6)

    # --- PAGINA: OBJETIVO ---
    pdf.add_page()
    pdf.titulo_secao("1", "Objetivo")
    pdf.texto(
        "Entregar um report recorrente diario (D+1) com visao completa do desempenho "
        "de cada campanha executada pelo CRM - cobrindo funil de conversao, impacto "
        "financeiro, ROI, segmentacoes e comparativo antes/durante/depois."
    )
    pdf.texto(
        "Consumidores: Time CRM (Raphael M.), BI, lideranca (CGO/CTO)."
    )
    pdf.texto(
        "Esse report vai alimentar tanto a operacao diaria do CRM quanto analises "
        "estrategicas ja em andamento, como o acompanhamento da campanha RETEM "
        "(task 86ag3994u no board de BI)."
    )

    # --- SECAO 2: O QUE JA TEMOS ---
    pdf.titulo_secao("2", "O que ja temos pronto (Super Nova DB)")

    pdf.subtitulo("2.1 Tabela fato - fact_crm_daily_performance")
    pdf.tabela(
        ["Item", "Detalhe"],
        [
            ["DDL", "pipelines/ddl/ddl_crm_daily_performance.sql"],
            ["Grao", "1 linha por campanha_id + period (BEFORE/DURING/AFTER)"],
            ["Colunas JSONB", "funil, financeiro, comparativo - flexiveis"],
            ["Indexes", "B-tree (campanha_id + period) + GIN nos JSONBs"],
            ["Constraint", "UNIQUE (campanha_id, period) - UPSERT seguro"],
        ],
        [50, 140],
    )

    pdf.texto_bold("O que ja armazena hoje:")
    pdf.bullet("Funil: enviadas, entregues, abertas, clicadas, convertidas, canais")
    pdf.bullet("Financeiro: total_users, depositos, GGR, BTR, RCA, NGR, APD, sessoes")
    pdf.bullet("Comparativo: NGR incremental, variacao %, custos por canal, ROI")

    pdf.subtitulo("2.2 Tabela dimensao - dim_crm_friendly_names")
    pdf.texto(
        "Mapeia entity_id para nome amigavel + categoria (RETEM, MULTIVERSO, WELCOME) "
        "+ responsavel. Pendencia: Raphael precisa validar/completar o mapeamento."
    )

    pdf.subtitulo("2.3 Pipelines existentes")
    pdf.tabela(
        ["Pipeline", "Arquivo", "Status", "Fonte financeira"],
        [
            ["CRM Daily v1", "fact_crm_daily_performance.py", "Funcional", "Redshift (!)"],
            ["CRM Daily v2", "crm_daily_performance.py", "Funcional", "Redshift (!)"],
            ["Report Promocoes", "report_crm_promocoes.py", "Completo", "Athena"],
            ["Report Multiverso", "report_multiverso_campanha.py", "Completo", "BigQuery+Athena"],
            ["Anti-Abuse", "anti_abuse_multiverso.py", "Em producao", "Athena"],
        ],
        [38, 62, 30, 60],
    )

    pdf.subtitulo("2.4 Regras de negocio validadas")
    pdf.tabela(
        ["Regra", "Impacto"],
        [
            ["Duplo filtro CRM (entity_id + template_id)", "Evita inflacao de ~39% nos completadores"],
            ["Semantica do funil (fact_type_id 1-5)", "Define exatamente o que cada etapa mede"],
            ["Sub-fund isolation (Real vs Bonus)", "GGR Real precisao de 0.000% vs referencia"],
            ["Glossario de KPIs (GGR, NGR, BTR)", "Padroniza definicoes na entrega"],
            ["Fuso horario UTC -> BRT", "Obrigatorio em toda query Athena"],
            ["Exclusao de test users", "Evita 3% de divergencia"],
        ],
        [85, 105],
    )

    pdf.subtitulo("2.5 Infraestrutura de dados")
    pdf.tabela(
        ["Fonte", "O que fornece", "Status"],
        [
            ["BigQuery (Smartico)", "Funil CRM, coorte bonus, opt-in, journeys", "OK"],
            ["Athena (Iceberg)", "GGR, depositos, turnover, sessoes, jogos", "OK"],
            ["Super Nova DB", "Persistencia de resultados (destino)", "OK"],
            ["ps_bi (dbt)", "Camada pre-agregada em BRL", "OK"],
        ],
        [45, 105, 40],
    )

    pdf.subtitulo("2.6 Tabelas de suporte ja criadas (schema multibet)")
    pdf.tabela(
        ["Tabela", "Uso no report"],
        [
            ["fact_casino_rounds", "Top jogos, GGR casino, RTP"],
            ["fact_sports_bets", "Sportsbook por esporte"],
            ["dim_games_catalog", "Catalogo com flags jackpot/freespin"],
            ["agg_financial_monthly", "Baseline financeiro mensal"],
            ["fact_ftd_deposits", "FTDs para campanhas de ativacao"],
        ],
        [70, 120],
    )

    # --- SECAO 3: GAP ANALYSIS ---
    pdf.add_page()
    pdf.titulo_secao("3", "Gap Analysis - PRD vs. infraestrutura atual")

    pdf.subtitulo("3.1 Gaps CRITICOS (bloqueiam entrega)")
    pdf.tabela(
        ["#", "Requisito", "O que falta", "Esforco"],
        [
            ["G1", "Migracao Redshift -> Athena", "Pipelines CRM usam Redshift (descontinuado). Reescrever para ps_bi/bireports_ec2.", "Alto"],
            ["G2", "Grao diario por campanha", "Hoje e campanha+period (3 linhas). PRD pede 1 linha/campanha/dia.", "Medio"],
            ["G3", "6 recortes financeiros", "Hoje so 'geral'. PRD: Geral, Cassino, Sports, Segmento, Jogo, Campanha.", "Medio"],
            ["G4", "Custos por provedor", "Custo fixo R$0,16. PRD diferencia SMS DisparosPro/Pushfy/WhatsApp.", "Baixo"],
        ],
        [10, 42, 98, 40],
    )

    pdf.subtitulo("3.2 Gaps IMPORTANTES (enriquecem a entrega)")
    pdf.tabela(
        ["#", "Requisito", "O que falta", "Esforco"],
        [
            ["G5", "Opt-in tracking", "Separar quem apostou+opt-in vs apostou sem opt-in (economia)", "Medio"],
            ["G6", "Segmentacao por perfil", "Tipo (Ativacao/Monet./Retencao/Recup.), produto, ticket VIP", "Medio"],
            ["G7", "Comparativo M-1", "Logica ja existe na v2, migrar fonte e incluir APD+sessoes", "Baixo"],
            ["G8", "Budget tracking", "Consumo mensal por canal, % verba, projecao fim do mes", "Medio"],
            ["G9", "Analise VIP", "Quebra Elite/Key Account/High Value por faixas NGR", "Baixo"],
            ["G10", "Recuperacao", "Inativos reengajados, tempo ate deposito, churn D+7", "Alto"],
            ["G11", "Casino detalhado", "Top jogos, GGR casino, proporcao casino/sports, RTP", "Baixo"],
            ["G12", "Meta vs. Realizado", "Campo de meta (input CRM pre-disparo)", "Baixo"],
        ],
        [10, 42, 98, 40],
    )

    # --- SECAO 4: PLANO DE EXECUCAO ---
    pdf.add_page()
    pdf.titulo_secao("4", "Plano de execucao proposto")

    pdf.subtitulo("Fase 1 - Fundacao (prioridade maxima) | Estimativa: ~1 semana")
    pdf.texto("Objetivo: Pipeline funcional com Athena, grao correto, dados financeiros confiaveis.")
    pdf.tabela(
        ["Step", "Entrega", "Dependencia", "Prazo"],
        [
            ["1.1", "Migrar financeiro Redshift -> Athena (ps_bi)", "Nenhuma", "2 dias"],
            ["1.2", "Evoluir DDL para grao diario", "Nenhuma", "0.5 dia"],
            ["1.3", "Parametrizar custos por provedor", "Tabela custos CRM", "0.5 dia"],
            ["1.4", "Implementar 6 recortes financeiros", "1.1 concluido", "1 dia"],
            ["1.5", "Validar pipeline com RETEM (piloto)", "1.1 a 1.4", "1 dia"],
        ],
        [13, 90, 55, 32],
    )

    pdf.subtitulo("Fase 2 - Enriquecimento | Estimativa: ~1 semana")
    pdf.texto("Objetivo: Segmentacoes, opt-in, VIP, comparativo completo.")
    pdf.tabela(
        ["Step", "Entrega", "Dependencia", "Prazo"],
        [
            ["2.1", "Opt-in tracking (economia gerada)", "Fase 1", "1 dia"],
            ["2.2", "Segmentacao por perfil (tipo, produto, ticket)", "Fase 1", "1 dia"],
            ["2.3", "Quebra VIP (Elite / Key Account / High Value)", "Fase 1", "0.5 dia"],
            ["2.4", "Comparativo M-1 com APD + sessoes", "Fase 1", "1 dia"],
            ["2.5", "Budget tracking mensal por canal", "Fase 1", "1 dia"],
        ],
        [13, 90, 55, 32],
    )

    pdf.subtitulo("Fase 3 - Analises avancadas | Estimativa: ~2 semanas")
    pdf.texto("Objetivo: Recuperacao, casino detalhado, dashboard visual.")
    pdf.tabela(
        ["Step", "Entrega", "Dependencia", "Prazo"],
        [
            ["3.1", "Modulo recuperacao (reengajamento, churn D+7)", "Fase 2", "3 dias"],
            ["3.2", "Casino detalhado (top jogos, RTP, GGR neg.)", "Fase 1", "1 dia"],
            ["3.3", "Meta vs. Realizado (input CRM)", "Alinhamento Raphael", "1 dia"],
            ["3.4", "Dashboard Flask (visualizacao diaria)", "Fase 2", "5 dias"],
        ],
        [13, 90, 55, 32],
    )

    # --- SECAO 5: ARQUITETURA ---
    pdf.add_page()
    pdf.titulo_secao("5", "Arquitetura de dados proposta")

    pdf.set_font("Courier", "", 8)
    pdf.set_text_color(30, 30, 30)
    arch = """
 FONTES DE DADOS
 +------------------+-------------------+------------------+
 | BigQuery         | Athena            | Input CRM        |
 | (Smartico)       | (Iceberg/ps_bi)   | (manual/Smartico)|
 +------------------+-------------------+------------------+
 | j_bonuses        | fct_player_       | Metas campanha   |
 | j_communication  |   activity_daily  | Verba mensal     |
 | j_automation     | fct_casino_       | Custos provedor  |
 | j_user           |   activity_daily  |                  |
 | dm_bonus_tmpl    | dim_user/dim_game |                  |
 +------------------+-------------------+------------------+
              |               |               |
              v               v               v
 +--------------------------------------------------------------+
 |            PIPELINE: crm_daily_report.py                     |
 |                                                              |
 |  1. Extrair coorte (BQ j_bonuses + entity_id isolation)      |
 |  2. Extrair funil (BQ j_communication fact_type_id 1-5)      |
 |  3. Extrair financeiro (Athena ps_bi - Real vs Bonus split)  |
 |  4. Extrair casino detail (Athena fct_casino_activity_daily) |
 |  5. Calcular opt-in vs nao opt-in (economia)                 |
 |  6. Calcular ROI, CPA, NGR incremental                      |
 |  7. Montar comparativo M-1 (BEFORE/DURING/AFTER)            |
 |  8. Segmentar (VIP, produto, tipo, ticket)                   |
 |  9. Persistir no Super Nova DB (UPSERT JSONB)                |
 +--------------------------------------------------------------+
              |
              v
 +--------------------------------------------------------------+
 |              SUPER NOVA DB (PostgreSQL)                       |
 |                schema: multibet                               |
 +--------------------------------------------------------------+
 |  fact_crm_daily_performance (campanha x dia x periodo)       |
 |    funil / financeiro / segmentacao / comparativo / budget    |
 |  dim_crm_friendly_names    (entity_id -> nome)               |
 |  dim_crm_campaign_meta     (NEW - metas + config)            |
 |  fact_crm_budget_monthly   (NEW - orcamento disparos)        |
 +--------------------------------------------------------------+
              |
              v
 +--------------------------------------------------------------+
 |                   CAMADA DE ENTREGA                          |
 |  Fase 1-2: Report Excel/CSV diario (D+1 manha)              |
 |  Fase 3:   Dashboard Flask (#bi_reports ou canal CRM)        |
 +--------------------------------------------------------------+
"""
    pdf.multi_cell(0, 3.5, arch)
    pdf.ln(4)

    # --- SECAO 6: DECISOES TECNICAS ---
    pdf.set_font("Helvetica", "", 10)
    pdf.titulo_secao("6", "Decisoes tecnicas relevantes")
    pdf.tabela(
        ["Decisao", "Justificativa"],
        [
            ["ps_bi como fonte principal", "Valores em BRL, Real vs Bonus separados, mais rapido que fund_ec2. Para NGR, ps_bi e a fonte correta."],
            ["JSONB para metricas", "Validado na tabela atual. Adiciona metricas sem ALTER TABLE. GIN index garante performance."],
            ["Coorte via BigQuery", "entity_id + template_id isolation evita 39% de inflacao. Regra critica validada."],
            ["Comparativo M-1", "Baseline = mesmo intervalo mes anterior. Captura sazonalidade real."],
            ["Custos parametrizaveis", "Valores mudam por provedor. Config em vez de hardcode."],
        ],
        [55, 135],
    )

    # --- SECAO 7: DEPENDENCIAS ---
    pdf.titulo_secao("7", "Dependencias externas")
    pdf.tabela(
        ["Dependencia", "Responsavel", "Status"],
        [
            ["Validar dim_crm_friendly_names", "Raphael M. (CRM)", "Pendente"],
            ["Definir metas por campanha", "Time CRM", "Pendente"],
            ["Confirmar verba mensal de disparos", "CRM / Financeiro", "Pendente"],
            ["Tabela de custos por provedor", "Time CRM", "PRD ja fornece ref."],
            ["Acesso a dados opt-in granular", "CRM / Smartico", "A verificar"],
        ],
        [75, 60, 55],
    )

    # --- SECAO 8: RISCOS ---
    pdf.titulo_secao("8", "Riscos identificados")
    pdf.tabela(
        ["Risco", "Mitigacao"],
        [
            ["Divergencia GGR fund_ec2 vs ps_bi (ate R$3.3M)", "Usar ps_bi para NGR (so realcash) - alinhado com definicao de negocio"],
            ["Sobreposicao de campanhas (mesmo user em 2+)", "Last Click attribution ja implementado na v2"],
            ["Dados opt-in insuficientes no BigQuery", "Validar com Smartico; fallback: inferir via j_automation_rule_progress"],
            ["Custo real pode variar vs tabela referencia", "Comecar com valores fixos; evoluir para integracao provedores"],
            ["Delay D+1: dados Athena incompletos de manha", "Validar horario de refresh Iceberg (tipicamente ate 6h BRT)"],
        ],
        [80, 110],
    )

    # --- SECAO 9: PROXIMOS PASSOS ---
    pdf.add_page()
    pdf.titulo_secao("9", "Proximos passos imediatos")

    passos = [
        "Validar este draft com Castrin - alinhar prioridades e timeline",
        "Iniciar Fase 1.1 - migrar fonte financeira de Redshift para Athena (ps_bi)",
        "Agendar alinhamento com Raphael - validar dim_crm_friendly_names + metas + custos",
        "Pilotar com campanha RETEM - primeira execucao end-to-end com dados reais",
        "Definir formato de entrega - Excel D+1 ou canal Slack (#bi_reports)",
    ]
    for i, p in enumerate(passos, 1):
        pdf.set_font("Helvetica", "B", 11)
        pdf.set_text_color(0, 70, 150)
        pdf.cell(10, 7, f"{i}.")
        pdf.set_font("Helvetica", "", 11)
        pdf.set_text_color(30, 30, 30)
        pdf.multi_cell(0, 7, p)
        pdf.ln(2)

    # --- SECAO 10: MOCKUP RETEM ---
    pdf.add_page()
    pdf.titulo_secao("10", "Mockup - Como o report vai parecer (Campanha RETEM)")

    pdf.texto(
        "Abaixo, um exemplo ilustrativo de como sera a saida do report para uma "
        "campanha RETEM. Os valores sao ficticios, mas a estrutura e exatamente "
        "o que sera entregue na Fase 1."
    )

    pdf.subtitulo("Identificacao")
    pdf.tabela(
        ["Campo", "Valor"],
        [
            ["Campanha", "RETEM Fevereiro 2026"],
            ["Tipo", "RETEM"],
            ["Canal", "WhatsApp + Push"],
            ["Segmento alvo", "Retencao - Inatividade 7d+"],
            ["Periodo", "01/02/2026 a 28/02/2026"],
            ["Status", "Encerrada"],
        ],
        [55, 135],
    )

    pdf.subtitulo("Funil de conversao")
    pdf.tabela(
        ["Etapa", "Usuarios", "% da base", "Taxa etapa"],
        [
            ["Segmentados", "15.230", "100,0%", "-"],
            ["Msg entregue", "14.450", "94,9%", "94,9%"],
            ["Visualizaram", "9.120", "59,9%", "63,1%"],
            ["Clicaram", "4.850", "31,8%", "53,2%"],
            ["Converteram (CTA)", "3.200", "21,0%", "66,0%"],
            ["Apostaram", "2.180", "14,3%", "68,1%"],
            ["Completaram condicao", "1.420", "9,3%", "65,1%"],
        ],
        [45, 40, 40, 45],
    )
    pdf.set_font("Helvetica", "I", 9)
    pdf.set_text_color(100, 100, 100)
    pdf.cell(0, 5, "Tempo medio ate completar: 18.3h | Usuarios sem opt-in que apostaram: 760 (economia de bonus)",
             new_x="LMARGIN", new_y="NEXT")
    pdf.ln(3)

    pdf.subtitulo("Resultado financeiro - Recorte Campanha (isolado)")
    pdf.tabela(
        ["Metrica", "BEFORE (Jan)", "DURING (Fev)", "AFTER (D+1 a D+3)", "Delta %"],
        [
            ["Turnover (R$)", "R$ 1.250.000", "R$ 1.890.000", "R$ 420.000", "+51,2%"],
            ["GGR (R$)", "R$ 187.500", "R$ 302.400", "R$ 63.000", "+61,3%"],
            ["NGR (R$)", "R$ 142.000", "R$ 245.800", "R$ 51.200", "+73,1%"],
            ["Net Deposit (R$)", "R$ 320.000", "R$ 485.000", "R$ 95.000", "+51,6%"],
            ["Depositos (qtd)", "3.450", "5.280", "1.100", "+53,0%"],
            ["APD (dias)", "2.8", "4.2", "1.5", "+50,0%"],
            ["Sessoes", "18.200", "31.500", "6.800", "+73,1%"],
        ],
        [38, 38, 38, 38, 38],
    )

    pdf.subtitulo("ROI da campanha")
    pdf.tabela(
        ["Metrica", "Valor"],
        [
            ["Custo bonus distribuido", "R$ 42.600"],
            ["Custo disparos (WhatsApp + Push)", "R$ 3.108"],
            ["Custo total", "R$ 45.708"],
            ["CPA medio (por completador)", "R$ 32,19"],
            ["NGR incremental (DURING - BEFORE)", "R$ 103.800"],
            ["ROI (NGR incremental / Custo total)", "2,27x"],
            ["Economia gerada (sem opt-in)", "R$ 18.240"],
        ],
        [75, 115],
    )

    pdf.set_font("Helvetica", "I", 9)
    pdf.set_text_color(100, 100, 100)
    pdf.cell(0, 5, "* Valores ilustrativos. O report real usara dados extraidos de BigQuery + Athena.",
             new_x="LMARGIN", new_y="NEXT")
    pdf.ln(6)

    # --- ANEXOS ---
    pdf.add_page()
    pdf.titulo_secao("A", "Anexo - Tabela de custos de disparo")
    pdf.tabela(
        ["Canal", "Provedor", "Custo por envio"],
        [
            ["SMS", "Disparos Pro", "R$ 0,045"],
            ["SMS", "Pushfy", "R$ 0,060"],
            ["WhatsApp", "Loyalty", "R$ 0,16"],
        ],
        [50, 70, 70],
    )

    pdf.titulo_secao("B", "Anexo - Faixas VIP")
    pdf.tabela(
        ["Grupo", "Criterio NGR no periodo"],
        [
            ["Elite", "NGR >= R$ 10.000"],
            ["Key Account", "NGR >= R$ 5.000 e < R$ 10.000"],
            ["High Value", "NGR >= R$ 3.000 e < R$ 5.000"],
        ],
        [60, 130],
    )

    pdf.titulo_secao("C", "Anexo - Funil CRM (definicao exata)")
    pdf.tabela(
        ["Etapa", "Fonte", "Significado real"],
        [
            ["Segmentados", "Smartico segment", "Base total da campanha (100%)"],
            ["Enviado", "j_communication type=1", "Sistema empurrou a mensagem"],
            ["Entregue", "j_communication type=2", "Chegou ao dispositivo"],
            ["Visualizado", "j_communication type=3", "Sessao ativa, popup apareceu"],
            ["Clicou", "j_communication type=4", "Qualquer interacao (fechar conta)"],
            ["Converteu", "j_communication type=5", "Clicou no CTA (Participar)"],
            ["Apostou", "j_automation_rule_progress", "Realizou aposta na campanha"],
            ["Completou", "j_bonuses (redeem_date)", "Cumpriu condicao e recebeu beneficio"],
        ],
        [30, 55, 105],
    )

    # --- SALVAR ---
    output = "c:/Users/NITRO/OneDrive - PGX/MultiBet/docs/draft_report_crm_diario.pdf"
    pdf.output(output)
    print(f"PDF gerado com sucesso: {output}")
    print(f"  Paginas: {pdf.pages_count}")


if __name__ == "__main__":
    gerar_pdf()