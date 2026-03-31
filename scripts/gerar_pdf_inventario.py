"""
Gera PDF do inventario schema multibet usando fpdf2.
Saida: reports/inventario_schema_multibet.pdf
"""
from fpdf import FPDF

class PDF(FPDF):
    def header(self):
        self.set_font("Helvetica", "B", 9)
        self.set_text_color(120, 120, 120)
        self.cell(0, 5, "Inventario Schema multibet - Super Nova DB", align="R", new_x="LMARGIN", new_y="NEXT")
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(2)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(150, 150, 150)
        self.cell(0, 10, f"Pagina {self.page_no()}/{{nb}}", align="C")

    def section_title(self, title):
        self.ln(4)
        self.set_font("Helvetica", "B", 13)
        self.set_text_color(30, 60, 120)
        self.cell(0, 8, title, new_x="LMARGIN", new_y="NEXT")
        self.set_draw_color(30, 60, 120)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(3)

    def sub_title(self, title):
        self.ln(2)
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(50, 50, 50)
        self.cell(0, 6, title, new_x="LMARGIN", new_y="NEXT")
        self.ln(1)

    def body_text(self, text):
        self.set_font("Helvetica", "", 9)
        self.set_text_color(40, 40, 40)
        self.multi_cell(0, 4.5, text)
        self.ln(1)

    def bold_text(self, text):
        self.set_font("Helvetica", "B", 9)
        self.set_text_color(40, 40, 40)
        self.multi_cell(0, 4.5, text)
        self.ln(1)

    def table(self, headers, rows, col_widths=None):
        if col_widths is None:
            col_widths = [190 / len(headers)] * len(headers)
        # Header
        self.set_font("Helvetica", "B", 7.5)
        self.set_fill_color(30, 60, 120)
        self.set_text_color(255, 255, 255)
        for i, h in enumerate(headers):
            self.cell(col_widths[i], 5.5, h, border=1, fill=True, align="C")
        self.ln()
        # Rows
        self.set_font("Helvetica", "", 7)
        self.set_text_color(30, 30, 30)
        fill = False
        for row in rows:
            if self.get_y() > 270:
                self.add_page()
                self.set_font("Helvetica", "B", 7.5)
                self.set_fill_color(30, 60, 120)
                self.set_text_color(255, 255, 255)
                for i, h in enumerate(headers):
                    self.cell(col_widths[i], 5.5, h, border=1, fill=True, align="C")
                self.ln()
                self.set_font("Helvetica", "", 7)
                self.set_text_color(30, 30, 30)
                fill = False
            if fill:
                self.set_fill_color(240, 245, 255)
            else:
                self.set_fill_color(255, 255, 255)
            max_h = 4.5
            for i, cell_val in enumerate(row):
                self.cell(col_widths[i], max_h, str(cell_val)[:60], border=1, fill=True)
            self.ln()
            fill = not fill
        self.ln(2)


def build_pdf():
    pdf = PDF()
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(auto=True, margin=20)
    pdf.add_page()

    # Title
    pdf.set_font("Helvetica", "B", 18)
    pdf.set_text_color(30, 60, 120)
    pdf.cell(0, 12, "Inventario Schema multibet", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 11)
    pdf.set_text_color(80, 80, 80)
    pdf.cell(0, 7, "Super Nova DB (PostgreSQL)", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(3)
    pdf.set_font("Helvetica", "", 9)
    pdf.cell(0, 5, "Data: 2026-03-30  |  Responsavel: Mateus Fabro (Squad Intelligence Engine)", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 5, "Objetivo: Mapear todos os objetos do schema multibet, fontes, finalidades e recorrencia.", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(3)

    # Resumo
    pdf.section_title("Resumo Quantitativo")
    pdf.table(
        ["Camada", "Tipo", "Quantidade"],
        [
            ["Bronze", "Tabelas (dados brutos Athena)", "24 (6 com dados, 18 vazias)"],
            ["Silver", "Tabelas fact/dim/agg (tratadas)", "~30"],
            ["Gold", "Views (agregacoes de leitura)", "8"],
            ["TOTAL", "", "~62 objetos"],
        ],
        [40, 100, 50],
    )

    # ===================== BRONZE =====================
    pdf.section_title("1. CAMADA BRONZE - Dados brutos do Athena, sem tratamento")
    pdf.body_text("Todas com prefixo bronze_. Replicam colunas selecionadas das tabelas Athena (Iceberg).")

    pdf.sub_title("1.1 Tabelas COM dados")
    pdf.table(
        ["Tabela", "Linhas", "Fonte Athena", "Descricao"],
        [
            ["bronze_ecr_flags", "1.1M", "bireports_ec2.tbl_ecr", "Flags jogador (test_user, etc.)"],
            ["bronze_bonus_sub_fund", "4.2M", "fund_ec2.tbl_bonus_sub_fund", "Sub-fund bonus por transacao"],
            ["bronze_ecr_banner", "336K", "ecr_ec2.tbl_ecr_banner", "Trackers, affiliates, click IDs"],
            ["bronze_instrument", "120K", "cashier_ec2.tbl_instrument", "Meios de pagamento (PIX, cartao)"],
            ["bronze_games_mapping_data", "2.7K", "bireports_ec2.tbl_vendor_games_mapping", "Catalogo jogos (game_id, vendor)"],
            ["bronze_fund_txn_type_mst", "157", "fund_ec2.tbl_fund_txn_type_mst", "Tipos transacao (27=Bet, 45=Win, 72=Roll)"],
        ],
        [45, 15, 70, 60],
    )

    pdf.sub_title("1.2 Tabelas VAZIAS (DDL criada, ETL de carga pendente)")
    pdf.table(
        ["Tabela", "Fonte Athena", "Descricao"],
        [
            ["bronze_ecr", "ecr_ec2.tbl_ecr", "Cadastro jogadores"],
            ["bronze_cashier_deposit", "cashier_ec2.tbl_cashier_deposit", "Depositos"],
            ["bronze_cashier_cashout", "cashier_ec2.tbl_cashier_cashout", "Saques"],
            ["bronze_real_fund_txn", "fund_ec2.tbl_real_fund_txn", "Transacoes gaming (bets/wins/rollbacks)"],
            ["bronze_realcash_sub_fund", "fund_ec2.tbl_realcash_sub_fund", "Sub-fund realcash"],
            ["bronze_daily_payment_summary", "cashier_ec2.tbl_daily_payment_summary", "Resumo pagamentos player/dia"],
            ["bronze_gaming_sessions", "bireports_ec2.tbl_ecr_gaming_sessions", "Sessoes de jogo"],
            ["bronze_sports_bets", "vendor_ec2.tbl_sports_book_bets_info", "Apostas esportivas"],
            ["bronze_bonus_details", "bonus_ec2.tbl_ecr_bonus_details", "Detalhes de bonus"],
            ["bronze_ccf_score", "risk_ec2.tbl_ecr_ccf_score", "Score de risco CCF"],
            ["bronze_kyc_level", "csm_ec2.tbl_ecr_kyc_level", "Nivel KYC jogador"],
            ["bronze_games_catalog", "bireports_ec2.tbl_vendor_games_master", "Catalogo completo jogos"],
            ["bronze_fund_txn_casino", "fund_ec2 (filtro CASINO)", "Subconjunto filtrado casino"],
            ["bronze_games_catalog_full", "-", "Possivelmente duplicata"],
            ["bronze_big_wins", "-", "Avaliacao: talvez deva ser Silver"],
            ["bronze_crm_campaigns", "BigQuery (Smartico)", "Fase 2 - nao criada"],
            ["bronze_crm_communications", "BigQuery (Smartico)", "Fase 2 - nao criada"],
            ["bronze_crm_player_responses", "BigQuery (Smartico)", "Fase 2 - nao criada"],
        ],
        [55, 70, 65],
    )

    pdf.sub_title("1.3 Tabelas que faltam criar (doc bronze_selects_kpis v2)")
    pdf.table(
        ["Tabela", "Fonte", "Justificativa"],
        [
            ["bronze_sports_bet_details", "vendor_ec2.tbl_sports_book_bet_details", "Unica fonte c_sport_type_name"],
            ["bronze_dim_game", "ps_bi.dim_game", "Dimensao jogos (RTP, volatilidade)"],
            ["bronze_dim_user", "ps_bi.dim_user", "Dimensao completa jogador"],
        ],
        [50, 70, 70],
    )

    # ===================== SILVER =====================
    pdf.section_title("2. CAMADA SILVER - Tabelas tratadas com regras de negocio")

    pdf.sub_title("2.1 Produto & Performance")
    pdf.table(
        ["Tabela", "Grao", "Fonte", "Estrategia", "Pipeline"],
        [
            ["fact_casino_rounds", "dia x jogo", "ps_bi.fct_casino_activity", "TRUNCATE+INSERT", "fact_casino_rounds.py"],
            ["fact_sports_bets", "dia x esporte", "vendor_ec2 (bets+details)", "TRUNCATE+INSERT", "fact_sports_bets.py"],
            ["fact_sports_open_bets", "snapshot x esporte", "vendor_ec2", "TRUNCATE+INSERT", "fact_sports_bets.py"],
            ["fact_live_casino", "dia x live game", "ps_bi", "TRUNCATE+INSERT", "fact_live_casino.py"],
            ["fact_jackpots", "mes x jogo", "ps_bi", "TRUNCATE+INSERT", "fact_jackpots.py"],
            ["fct_casino_activity", "dia", "ps_bi", "TRUNCATE+INSERT", "fct_casino_activity.py"],
            ["fct_sports_activity", "dia", "vendor_ec2", "TRUNCATE+INSERT", "fct_sports_activity.py"],
        ],
        [38, 30, 42, 35, 45],
    )

    pdf.sub_title("2.2 Player & Aquisicao")
    pdf.table(
        ["Tabela", "Grao", "Fonte", "Pipeline"],
        [
            ["fact_player_activity", "dia", "ps_bi", "fact_player_activity.py"],
            ["fact_gaming_activity_daily", "dia x tracker", "Athena multi", "fact_gaming_activity_daily.py"],
            ["fact_player_engagement_daily", "player (c_ecr_id)", "Athena multi", "fact_player_engagement_daily.py"],
            ["fact_redeposits", "player (c_ecr_id)", "cashier_ec2", "fact_redeposits.py"],
            ["fact_registrations", "dia", "ecr_ec2", "fact_registrations.py"],
            ["fact_ftd_deposits", "dia x tracker", "Athena + dim_user", "fact_ftd_deposits.py"],
            ["fact_attribution", "dia x tracker", "Athena multi", "fact_attribution.py"],
        ],
        [50, 35, 45, 60],
    )

    pdf.sub_title("2.3 CRM")
    pdf.table(
        ["Tabela", "Grao", "Fonte", "Pipeline", "Obs"],
        [
            ["fact_crm_daily_performance", "campanha x periodo", "BigQuery + Athena", "crm_daily_performance.py", "Principal (JSONB)"],
            ["dim_crm_friendly_names", "entity_id", "Manual/CRM", "crm_daily_performance.py", "De-Para nomes"],
            ["crm_campaign_daily", "campanha x dia", "BigQuery + Athena", "ddl_crm_report.py", "v1"],
            ["crm_campaign_segment_daily", "camp x seg x dia", "BigQuery + Athena", "ddl_crm_report.py", "v1"],
            ["crm_campaign_game_daily", "camp x jogo x dia", "BigQuery + Athena", "ddl_crm_report.py", "v1"],
            ["crm_campaign_comparison", "camp x periodo", "BigQuery + Athena", "ddl_crm_report.py", "v1"],
            ["crm_dispatch_budget", "mes x canal x prov", "Custos fixos", "crm_report_daily.py", "SMS/WhatsApp"],
            ["crm_vip_group_daily", "camp x VIP x dia", "BigQuery + Athena", "ddl_crm_report.py", "v1"],
            ["crm_recovery_daily", "camp x canal x dia", "BigQuery + Athena", "ddl_crm_report.py", "v1"],
            ["crm_player_vip_tier", "player x periodo", "Athena (NGR)", "ddl_crm_report.py", "Elite/Key/High"],
        ],
        [42, 28, 32, 45, 43],
    )

    pdf.sub_title("2.4 Dimensoes & Mapeamento")
    pdf.table(
        ["Tabela", "Grao", "Fonte", "Pipeline"],
        [
            ["dim_games_catalog", "game_id (PK)", "bireports_ec2", "dim_games_catalog.py"],
            ["game_image_mapping", "game (SERIAL PK)", "CDN provedores", "game_image_mapper.py"],
            ["dim_marketing_mapping", "tracker_id", "Athena + forense click IDs", "dim_marketing_mapping_canonical.py"],
            ["dim_campaign_affiliate", "campaign x affiliate", "Google Ads API", "sync_google_ads_spend.py"],
        ],
        [45, 38, 52, 55],
    )

    pdf.sub_title("2.5 Agregacoes")
    pdf.table(
        ["Tabela", "Grao", "Fonte", "Pipeline"],
        [
            ["agg_cohort_acquisition", "player x safra (mes FTD)", "Athena + dim_marketing_mapping", "agg_cohort_acquisition.py"],
            ["agg_game_performance", "semana x jogo", "ps_bi", "agg_game_performance.py"],
        ],
        [45, 40, 52, 53],
    )

    pdf.sub_title("2.6 Operacionais / ETL")
    pdf.table(
        ["Tabela", "Grao", "Fonte", "Pipeline", "Recorrencia"],
        [
            ["grandes_ganhos", "evento (big win)", "BigQuery Smartico", "grandes_ganhos.py", "Cron diario 00:30"],
            ["aquisicao_trafego_diario", "dia x canal x source", "Athena + BigQuery", "etl_aquisicao_trafego.py", "Cron horario 60min"],
            ["fact_google_ads_spend", "dia x campaign", "Google Ads API", "sync_google_ads_spend.py", "Manual"],
            ["etl_active_player_retention", "semana", "cashier_ec2", "vw_active_player_ret.py", "Sugerido diario"],
        ],
        [42, 30, 32, 42, 44],
    )

    pdf.sub_title("2.7 Tabelas auxiliares da Matriz Financeiro")
    pdf.body_text("Base das views matriz_financeiro_mensal e matriz_financeiro_semanal:")
    pdf.table(
        ["Tabela", "Descricao"],
        [
            ["tab_dep_with", "Depositos e saques por dia"],
            ["tab_user_ftd", "FTD metricas por dia"],
            ["tab_cassino", "KPIs casino por dia"],
            ["tab_sports", "KPIs sports por dia"],
            ["tab_ativos", "Jogadores ativos (betting) por dia"],
        ],
        [60, 130],
    )

    # ===================== GOLD =====================
    pdf.section_title("3. CAMADA GOLD - Views (agregacoes de leitura)")

    pdf.table(
        ["View", "Tabelas-fonte (multibet)", "Proposito"],
        [
            ["vw_active_player_retention_weekly", "etl_active_player_retention_weekly", "Retencao semanal depositantes"],
            ["vw_cohort_roi", "agg_cohort_acquisition", "ROI por cohort/safra FTD"],
            ["vw_attribution_metrics", "fact_attribution", "Metricas atribuicao por modelo"],
            ["vw_acquisition_channel", "dim_marketing_mapping + facts", "Canal aquisicao consolidado"],
            ["vw_aquisicao_trafego", "aquisicao_trafego_diario", "Trafego/aquisicao formatado"],
            ["vw_google_ads_spend_daily", "fact_google_ads_spend", "Google Ads spend diario"],
            ["matriz_financeiro_mensal", "tab_dep/ftd/cassino/sports/ativos", "KPIs financeiros por mes"],
            ["matriz_financeiro_semanal", "mesmas 5 tabelas", "KPIs financeiros por semana"],
        ],
        [55, 70, 65],
    )

    # ===================== PRODUCAO =====================
    pdf.section_title("4. Pipelines em producao (EC2)")

    pdf.table(
        ["Pipeline", "Cron", "Tabela destino", "Status"],
        [
            ["grandes_ganhos.py", "30 3 * * * (00:30 BRT)", "multibet.grandes_ganhos", "Ativo"],
            ["game_image_mapper.py", "Pre-req grandes_ganhos", "multibet.game_image_mapping", "Ativo"],
            ["etl_aquisicao_trafego.py", "10 * * * * (a cada 60min)", "multibet.aquisicao_trafego_diario", "Ativo"],
            ["anti_abuse_multiverso.py", "Loop 5min (systemd)", "- (monitoramento CRM)", "Ativo"],
        ],
        [45, 47, 60, 38],
    )

    # ===================== DEPENDENCIAS =====================
    pdf.section_title("5. Dependencias entre objetos")

    pdf.sub_title("Views Gold dependem de:")
    pdf.body_text(
        "matriz_financeiro_mensal  -->  tab_dep_with, tab_user_ftd, tab_cassino, tab_sports, tab_ativos\n"
        "matriz_financeiro_semanal -->  (mesmas 5 tabelas)\n"
        "vw_active_player_retention_weekly --> etl_active_player_retention_weekly\n"
        "vw_cohort_roi --> agg_cohort_acquisition\n"
        "vw_attribution_metrics --> fact_attribution\n"
        "vw_acquisition_channel --> dim_marketing_mapping + fact tables\n"
        "vw_aquisicao_trafego --> aquisicao_trafego_diario\n"
        "vw_google_ads_spend_daily --> fact_google_ads_spend"
    )

    pdf.sub_title("Pipelines dependem de:")
    pdf.body_text(
        "grandes_ganhos.py --> game_image_mapping (pre-requisito)\n"
        "agg_cohort_acquisition.py --> dim_marketing_mapping (lookup source)"
    )

    # ===================== NOTAS =====================
    pdf.section_title("6. Notas tecnicas")
    pdf.body_text(
        "- Upsert: maioria das tabelas usa INSERT...ON CONFLICT DO UPDATE para idempotencia\n"
        "- TRUNCATE+INSERT: tabelas fact de produto fazem full reload\n"
        "- JSONB: fact_crm_daily_performance usa colunas JSONB (funil, financeiro, comparativo)\n"
        "- LGPD: grandes_ganhos hasheia nomes de jogadores\n"
        "- Backfill: tabelas fact tem dados desde 2025-10-01\n"
        "- Fontes externas: Athena (Iceberg), BigQuery (Smartico CRM), Google Ads API"
    )

    out = "reports/inventario_schema_multibet.pdf"
    pdf.output(out)
    print(f"PDF gerado: {out}")

if __name__ == "__main__":
    build_pdf()
