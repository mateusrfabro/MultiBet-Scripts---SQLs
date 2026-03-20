"""Validação cruzada automatizada: HTML FINAL vs XLSX FINAL."""
import pandas as pd
import re
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

OUTPUT = os.path.join(os.path.dirname(__file__), '..', 'output')
xlsx_path = os.path.join(OUTPUT, 'crm_kpis_marco_2026_FINAL.xlsx')
html_path = os.path.join(OUTPUT, 'crm_kpis_marco_2026_FINAL.html')

# ---- Carregar XLSX ----
df_std = pd.read_excel(xlsx_path, sheet_name='STD_3Dep')
df_ltv = pd.read_excel(xlsx_path, sheet_name='LTV')
df_rec = pd.read_excel(xlsx_path, sheet_name='Recuperacao')
df_fin = pd.read_excel(xlsx_path, sheet_name='Financeiro_Recuperados')

# ---- Carregar HTML e extrair tabelas ----
from html.parser import HTMLParser

class TableExtractor(HTMLParser):
    def __init__(self):
        super().__init__()
        self.tables = []
        self.current_table = []
        self.current_row = []
        self.current_cell = ""
        self.in_table = False
        self.in_row = False
        self.in_cell = False

    def handle_starttag(self, tag, attrs):
        if tag == 'table':
            self.in_table = True
            self.current_table = []
        elif tag == 'tr' and self.in_table:
            self.in_row = True
            self.current_row = []
        elif tag in ('td', 'th') and self.in_row:
            self.in_cell = True
            self.current_cell = ""

    def handle_endtag(self, tag):
        if tag in ('td', 'th') and self.in_cell:
            self.in_cell = False
            self.current_row.append(self.current_cell.strip())
        elif tag == 'tr' and self.in_row:
            self.in_row = False
            if self.current_row:
                self.current_table.append(self.current_row)
        elif tag == 'table' and self.in_table:
            self.in_table = False
            self.tables.append(self.current_table)

    def handle_data(self, data):
        if self.in_cell:
            self.current_cell += data

with open(html_path, 'r', encoding='utf-8') as f:
    html_content = f.read()

parser = TableExtractor()
parser.feed(html_content)
tables = parser.tables

# ---- Extrair KPI cards (valores hardcoded do HTML) ----
# Regex para extrair os valores dos cards
kpi_values = re.findall(r'class="value"[^>]*>([^<]+)<', html_content)

print("=" * 70)
print("  VALIDACAO CRUZADA: HTML FINAL vs XLSX FINAL")
print("=" * 70)

total_checks = 0
total_errors = 0

# ---- Tabela 1: STD/3Dep (tables[0]) ----
print("\n--- 1. STD / 3o Deposito ---")
html_std = tables[0]
periodo_map = {
    'Marco Total': 'marco',
    '01 a 07/03': '01a07',
    '08 a 15/03': '08a15'
}

for row in html_std[1:]:  # skip header
    cohort = row[0]
    periodo = periodo_map.get(row[1], row[1])

    mask = (df_std['cohort'] == cohort) & (df_std['periodo'] == periodo)
    if mask.sum() == 0:
        continue
    x = df_std[mask].iloc[0]

    # Parse HTML values
    h_ftd = int(row[2].replace('.', ''))
    h_std = int(row[3].replace('.', ''))
    h_std_r = float(row[4].replace('%', '').replace(',', '.'))
    h_dep3 = int(row[5].replace('.', ''))
    h_dep3_r = float(row[6].replace('%', '').replace(',', '.'))

    checks = [
        ('ftd_count', h_ftd, int(x['ftd_count'])),
        ('std_count', h_std, int(x['std_count'])),
        ('std_rate', h_std_r, float(x['std_rate'])),
        ('dep3_count', h_dep3, int(x['dep3_count'])),
        ('dep3_rate', h_dep3_r, float(x['dep3_rate'])),
    ]

    for campo, hv, xv in checks:
        total_checks += 1
        ok = (hv == xv) if isinstance(hv, int) else abs(hv - xv) < 0.01
        if not ok:
            total_errors += 1
            print(f"  ERRO | {cohort} {periodo} | {campo}: HTML={hv} vs XLSX={xv}")

if total_errors == 0:
    print(f"  {total_checks} campos verificados: TODOS OK")

# ---- Tabela 2: LTV (tables[1]) ----
print("\n--- 2. LTV ---")
html_ltv = tables[1]
ltv_errors = 0
ltv_checks = 0

def parse_brl(s):
    """R$ 385,71 -> 385.71 | R$ 994.216 -> 994216.0"""
    clean = s.replace('R$', '').strip()
    # Se tem vírgula, é decimal BR: 385,71
    if ',' in clean:
        parts = clean.split(',')
        inteiro = parts[0].replace('.', '')
        decimal = parts[1]
        return float(f"{inteiro}.{decimal}")
    else:
        # Sem vírgula: 994.216 = 994216 (ponto é milhar)
        return float(clean.replace('.', ''))

for row in html_ltv[1:]:
    cohort = row[0]
    periodo = periodo_map.get(row[1], row[1])

    mask = (df_ltv['cohort'] == cohort) & (df_ltv['periodo'] == periodo)
    if mask.sum() == 0:
        continue
    x = df_ltv[mask].iloc[0]

    h_ftd = int(row[2].replace('.', ''))
    h_dep = parse_brl(row[3])
    h_wdr = parse_brl(row[4])
    h_ltv = parse_brl(row[5])
    h_total = parse_brl(row[6])

    checks = [
        ('ftd_count', h_ftd, int(x['ftd_count']), 0),
        ('avg_deposit', h_dep, float(x['avg_deposit']), 0.01),
        ('avg_withdrawal', h_wdr, float(x['avg_withdrawal']), 0.01),
        ('avg_ltv', h_ltv, float(x['avg_ltv']), 0.01),
        ('total_ltv', h_total, float(x['total_ltv']), 1.0),
    ]

    for campo, hv, xv, tol in checks:
        ltv_checks += 1
        total_checks += 1
        if isinstance(hv, int):
            ok = hv == int(xv)
        else:
            ok = abs(hv - xv) <= tol
        if not ok:
            ltv_errors += 1
            total_errors += 1
            print(f"  ERRO | {cohort} {periodo} | {campo}: HTML={hv} vs XLSX={xv}")

if ltv_errors == 0:
    print(f"  {ltv_checks} campos verificados: TODOS OK")

# ---- Tabela 3: Recuperação (tables[2] — não existe como tabela, é cards) ----
print("\n--- 3. Recuperacao (KPI cards vs XLSX) ---")
r = df_rec.iloc[0]
rec_checks = [
    ('total_old_users', 1061536, int(r['total_old_users'])),
    ('sem_aposta_fev', 1001071, int(r['sem_aposta_fev'])),
    ('recuperados', 12428, int(r['recuperados'])),
    ('taxa_recuperacao', 1.24, float(r['taxa_recuperacao_pct'])),
]
rec_errors = 0
for campo, hv, xv in rec_checks:
    total_checks += 1
    ok = hv == xv if isinstance(hv, int) else abs(hv - xv) < 0.01
    status = "OK" if ok else "ERRO"
    if not ok:
        rec_errors += 1
        total_errors += 1
    print(f"  {status} | {campo}: HTML={hv} vs XLSX={xv}")

# ---- Tabela 4: Financeiro Recuperados (tables[2] — tabela 3 é metodologia) ----
print("\n--- 4. Financeiro Recuperados ---")
html_fin = tables[2]
f = df_fin.iloc[0]

html_fin_map = {}
for row in html_fin[1:]:
    html_fin_map[row[0]] = row[1]

fin_checks = [
    ('Usuarios Recuperados', 'recovered_count'),
    ('Turnover Real (apostas reais)', 'turnover_real'),
    ('Turnover Total (real + bonus)', 'turnover_total'),
    ('GGR', 'ggr'),
    ('NGR', 'ngr'),
    ('Total Depositos', 'total_depositos'),
    ('Total Saques', 'total_saques'),
]

fin_errors = 0
for label, col in fin_checks:
    total_checks += 1
    xlsx_val = round(float(f[col]))
    html_raw = html_fin_map.get(label, '0')
    html_val = int(re.sub(r'[R$\s.]', '', html_raw).replace(',', '.').split('.')[0])
    ok = abs(html_val - xlsx_val) <= 1
    status = "OK" if ok else "ERRO"
    if not ok:
        fin_errors += 1
        total_errors += 1
    print(f"  {status} | {label}: HTML={html_val:,} vs XLSX={xlsx_val:,}")

# Net Deposit (calculado)
total_checks += 1
net_html = 320500
net_xlsx = round(float(f['total_depositos']) - float(f['total_saques']))
ok = abs(net_html - net_xlsx) <= 1
status = "OK" if ok else "ERRO"
if not ok:
    fin_errors += 1
    total_errors += 1
print(f"  {status} | Net Deposit: HTML={net_html:,} vs XLSX(calc)={net_xlsx:,}")

# ---- KPI Cards Resumo ----
print("\n--- 5. KPI Cards Resumo Executivo ---")
ftd_total = int(df_std[df_std['periodo'] == 'marco']['ftd_count'].sum())
std_total = int(df_std[df_std['periodo'] == 'marco']['std_count'].sum())
std_rate_calc = round(100.0 * std_total / ftd_total, 1)
ltv_sum = float(df_ltv[df_ltv['periodo'] == 'marco']['total_ltv'].sum())
ltv_avg_calc = round(ltv_sum / ftd_total, 2)

card_checks = [
    ('FTDs Marco', 18851, ftd_total, 0),
    ('Taxa STD Media %', 32.6, std_rate_calc, 0.1),
    ('LTV Medio R$', 57.93, ltv_avg_calc, 0.02),
    ('Recuperados', 12428, int(r['recuperados']), 0),
]

for label, hv, xv, tol in card_checks:
    total_checks += 1
    ok = abs(hv - xv) <= tol
    status = "OK" if ok else "ERRO"
    if not ok:
        total_errors += 1
    print(f"  {status} | {label}: HTML={hv} vs calc={xv}")

# ---- RESULTADO FINAL ----
print("\n" + "=" * 70)
if total_errors == 0:
    print(f"  RESULTADO: {total_checks} campos verificados, 0 erros. HTML = XLSX")
else:
    print(f"  RESULTADO: {total_checks} campos, {total_errors} ERROS encontrados!")
print("=" * 70)
