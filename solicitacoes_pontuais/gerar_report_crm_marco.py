"""
Gera report HTML + mensagem WhatsApp para entrega ao CRM.
Usa os dados já extraídos do script corrigido.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '..', 'output')


def gerar_html():
    """Gera report HTML standalone com os resultados do CRM."""

    html = """<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>KPIs CRM - Marco 2026 | MultiBet</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #0f1117;
            color: #e4e4e7;
            padding: 24px;
            line-height: 1.5;
        }
        .container { max-width: 960px; margin: 0 auto; }
        .header {
            background: linear-gradient(135deg, #1e1b4b, #312e81);
            border-radius: 12px;
            padding: 32px;
            margin-bottom: 24px;
            border: 1px solid #3730a3;
        }
        .header h1 { font-size: 24px; font-weight: 700; color: #c7d2fe; }
        .header p { color: #a5b4fc; font-size: 14px; margin-top: 8px; }
        .badge {
            display: inline-block;
            background: #22c55e20;
            color: #4ade80;
            border: 1px solid #22c55e40;
            border-radius: 6px;
            padding: 2px 10px;
            font-size: 12px;
            font-weight: 600;
            margin-top: 12px;
        }
        .badge.warn {
            background: #f59e0b20;
            color: #fbbf24;
            border-color: #f59e0b40;
        }
        .section {
            background: #18181b;
            border: 1px solid #27272a;
            border-radius: 12px;
            padding: 24px;
            margin-bottom: 20px;
        }
        .section h2 {
            font-size: 16px;
            font-weight: 600;
            color: #a5b4fc;
            margin-bottom: 16px;
            display: flex;
            align-items: center;
            gap: 8px;
        }
        .section h2 .icon { font-size: 18px; }
        table {
            width: 100%;
            border-collapse: collapse;
            font-size: 13px;
        }
        th {
            text-align: left;
            padding: 10px 12px;
            background: #27272a;
            color: #a1a1aa;
            font-weight: 600;
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        td {
            padding: 10px 12px;
            border-bottom: 1px solid #27272a;
        }
        tr:hover td { background: #27272a40; }
        .num { text-align: right; font-variant-numeric: tabular-nums; }
        .highlight { color: #a5b4fc; font-weight: 600; }
        .green { color: #4ade80; }
        .yellow { color: #fbbf24; }
        .red { color: #f87171; }
        .row-total td {
            background: #1e1b4b20;
            font-weight: 600;
            border-top: 2px solid #3730a3;
        }
        .kpi-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 16px;
            margin-bottom: 16px;
        }
        .kpi-card {
            background: #1e1b4b20;
            border: 1px solid #3730a340;
            border-radius: 8px;
            padding: 16px;
        }
        .kpi-card .label {
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            color: #a1a1aa;
        }
        .kpi-card .value {
            font-size: 28px;
            font-weight: 700;
            color: #c7d2fe;
            margin-top: 4px;
        }
        .kpi-card .sub {
            font-size: 12px;
            color: #71717a;
            margin-top: 4px;
        }
        .note {
            background: #27272a;
            border-left: 3px solid #f59e0b;
            padding: 12px 16px;
            border-radius: 0 8px 8px 0;
            font-size: 13px;
            color: #a1a1aa;
            margin-top: 16px;
        }
        .footer {
            text-align: center;
            color: #52525b;
            font-size: 12px;
            margin-top: 32px;
            padding: 16px;
        }
        @media (max-width: 640px) {
            body { padding: 12px; }
            .header { padding: 20px; }
            .section { padding: 16px; }
            table { font-size: 12px; }
            th, td { padding: 8px 6px; }
        }
    </style>
</head>
<body>
    <div class="container">

        <!-- HEADER -->
        <div class="header">
            <h1>KPIs CRM — Marco 2026</h1>
            <p>Periodo: 01/03 a 19/03/2026 | Fonte: Athena ps_bi | Extraido: 20/03/2026</p>
            <span class="badge">DADOS CORRIGIDOS</span>
            <span class="badge warn">Excluidos ~13K users com ftd_date recalculado pelo dbt</span>
        </div>

        <!-- RESUMO EXECUTIVO -->
        <div class="section">
            <h2><span class="icon">1.</span> Resumo Executivo</h2>
            <div class="kpi-grid">
                <div class="kpi-card">
                    <div class="label">FTDs Marco (limpos)</div>
                    <div class="value">18.851</div>
                    <div class="sub">16.710 New + 2.141 Old</div>
                </div>
                <div class="kpi-card">
                    <div class="label">Taxa STD Media</div>
                    <div class="value">32,6%</div>
                    <div class="sub">New 33,29% | Old 27,51%</div>
                </div>
                <div class="kpi-card">
                    <div class="label">LTV Medio (Net Deposit)</div>
                    <div class="value">R$ 57,93</div>
                    <div class="sub">New R$ 59,50 | Old R$ 45,69</div>
                </div>
                <div class="kpi-card">
                    <div class="label">Recuperados</div>
                    <div class="value">12.428</div>
                    <div class="sub">Taxa: 1,24% | NGR: R$ 247K</div>
                </div>
            </div>
        </div>

        <!-- STD e 3o DEPOSITO -->
        <div class="section">
            <h2><span class="icon">2.</span> Taxa de STD e 3&ordm; Deposito</h2>
            <p style="font-size:12px; color:#71717a; margin-bottom:12px;">
                Regra: FTD no periodo &rarr; conversao ate current_date | Split: New_Reg (reg marco) vs Old_Reg (reg &lt; marco)
            </p>
            <table>
                <thead>
                    <tr>
                        <th>Cohort</th>
                        <th>Periodo</th>
                        <th class="num">FTDs</th>
                        <th class="num">STD</th>
                        <th class="num">Taxa STD</th>
                        <th class="num">3&ordm; Dep</th>
                        <th class="num">Taxa 3&ordm; Dep</th>
                    </tr>
                </thead>
                <tbody>
                    <tr class="row-total">
                        <td class="highlight">New_Reg</td>
                        <td class="highlight">Marco Total</td>
                        <td class="num highlight">16.710</td>
                        <td class="num">5.563</td>
                        <td class="num green">33,29%</td>
                        <td class="num">2.838</td>
                        <td class="num">16,98%</td>
                    </tr>
                    <tr>
                        <td>New_Reg</td>
                        <td>01 a 07/03</td>
                        <td class="num">5.120</td>
                        <td class="num">1.621</td>
                        <td class="num">31,66%</td>
                        <td class="num">836</td>
                        <td class="num">16,33%</td>
                    </tr>
                    <tr>
                        <td>New_Reg</td>
                        <td>08 a 15/03</td>
                        <td class="num">7.817</td>
                        <td class="num">2.609</td>
                        <td class="num">33,38%</td>
                        <td class="num">1.323</td>
                        <td class="num">16,92%</td>
                    </tr>
                    <tr class="row-total">
                        <td class="highlight">Old_Reg</td>
                        <td class="highlight">Marco Total</td>
                        <td class="num highlight">2.141</td>
                        <td class="num">589</td>
                        <td class="num yellow">27,51%</td>
                        <td class="num">260</td>
                        <td class="num">12,14%</td>
                    </tr>
                    <tr>
                        <td>Old_Reg</td>
                        <td>01 a 07/03</td>
                        <td class="num">888</td>
                        <td class="num">244</td>
                        <td class="num">27,48%</td>
                        <td class="num">100</td>
                        <td class="num">11,26%</td>
                    </tr>
                    <tr>
                        <td>Old_Reg</td>
                        <td>08 a 15/03</td>
                        <td class="num">857</td>
                        <td class="num">236</td>
                        <td class="num">27,54%</td>
                        <td class="num">112</td>
                        <td class="num">13,07%</td>
                    </tr>
                </tbody>
            </table>
        </div>

        <!-- LTV -->
        <div class="section">
            <h2><span class="icon">3.</span> LTV (Net Deposit = Depositos &minus; Saques)</h2>
            <table>
                <thead>
                    <tr>
                        <th>Cohort</th>
                        <th>Periodo</th>
                        <th class="num">FTDs</th>
                        <th class="num">Dep Medio</th>
                        <th class="num">Saque Medio</th>
                        <th class="num">LTV Medio</th>
                        <th class="num">LTV Total</th>
                    </tr>
                </thead>
                <tbody>
                    <tr class="row-total">
                        <td class="highlight">New_Reg</td>
                        <td class="highlight">Marco Total</td>
                        <td class="num highlight">16.710</td>
                        <td class="num">R$ 385,71</td>
                        <td class="num">R$ 326,21</td>
                        <td class="num green">R$ 59,50</td>
                        <td class="num">R$ 994.216</td>
                    </tr>
                    <tr>
                        <td>New_Reg</td>
                        <td>01 a 07/03</td>
                        <td class="num">5.120</td>
                        <td class="num">R$ 419,35</td>
                        <td class="num">R$ 347,92</td>
                        <td class="num">R$ 71,43</td>
                        <td class="num">R$ 365.721</td>
                    </tr>
                    <tr>
                        <td>New_Reg</td>
                        <td>08 a 15/03</td>
                        <td class="num">7.817</td>
                        <td class="num">R$ 392,27</td>
                        <td class="num">R$ 353,41</td>
                        <td class="num">R$ 38,86</td>
                        <td class="num">R$ 303.743</td>
                    </tr>
                    <tr class="row-total">
                        <td class="highlight">Old_Reg</td>
                        <td class="highlight">Marco Total</td>
                        <td class="num highlight">2.141</td>
                        <td class="num">R$ 439,41</td>
                        <td class="num">R$ 393,72</td>
                        <td class="num yellow">R$ 45,69</td>
                        <td class="num">R$ 97.825</td>
                    </tr>
                    <tr>
                        <td>Old_Reg</td>
                        <td>01 a 07/03</td>
                        <td class="num">888</td>
                        <td class="num">R$ 507,57</td>
                        <td class="num">R$ 487,07</td>
                        <td class="num">R$ 20,50</td>
                        <td class="num">R$ 18.200</td>
                    </tr>
                    <tr>
                        <td>Old_Reg</td>
                        <td>08 a 15/03</td>
                        <td class="num">857</td>
                        <td class="num">R$ 431,44</td>
                        <td class="num">R$ 388,23</td>
                        <td class="num">R$ 43,21</td>
                        <td class="num">R$ 37.031</td>
                    </tr>
                </tbody>
            </table>
            <div class="note">
                LTV negativo pode ocorrer para FTDs recentes com saques altos — e estatisticamente esperado, nao erro de dado.
            </div>
        </div>

        <!-- RECUPERACAO -->
        <div class="section">
            <h2><span class="icon">4.</span> Taxa de Recuperacao</h2>
            <p style="font-size:12px; color:#71717a; margin-bottom:12px;">
                Usuarios cadastrados antes de marco, sem aposta em fevereiro, com aposta em marco
            </p>
            <div class="kpi-grid">
                <div class="kpi-card">
                    <div class="label">Users Antigos Total</div>
                    <div class="value" style="font-size:22px;">1.061.536</div>
                </div>
                <div class="kpi-card">
                    <div class="label">Sem Aposta em Fevereiro</div>
                    <div class="value" style="font-size:22px;">1.001.071</div>
                    <div class="sub">94,3% inativos em fev</div>
                </div>
                <div class="kpi-card">
                    <div class="label">Recuperados em Marco</div>
                    <div class="value" style="font-size:22px; color:#4ade80;">12.428</div>
                </div>
                <div class="kpi-card">
                    <div class="label">Taxa de Recuperacao</div>
                    <div class="value" style="font-size:22px;">1,24%</div>
                </div>
            </div>
        </div>

        <!-- FINANCEIRO RECUPERADOS -->
        <div class="section">
            <h2><span class="icon">5.</span> Financeiro dos Recuperados</h2>
            <table>
                <thead>
                    <tr>
                        <th>Metrica</th>
                        <th class="num">Valor</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td>Usuarios Recuperados</td>
                        <td class="num highlight">12.428</td>
                    </tr>
                    <tr>
                        <td>Turnover Real (apostas reais)</td>
                        <td class="num">R$ 12.021.028</td>
                    </tr>
                    <tr>
                        <td>Turnover Total (real + bonus)</td>
                        <td class="num">R$ 12.053.555</td>
                    </tr>
                    <tr>
                        <td>GGR</td>
                        <td class="num green">R$ 336.048</td>
                    </tr>
                    <tr>
                        <td>NGR</td>
                        <td class="num green">R$ 247.137</td>
                    </tr>
                    <tr>
                        <td>Total Depositos</td>
                        <td class="num">R$ 3.973.756</td>
                    </tr>
                    <tr>
                        <td>Total Saques</td>
                        <td class="num">R$ 3.653.256</td>
                    </tr>
                    <tr>
                        <td>Net Deposit (dep - saque)</td>
                        <td class="num highlight">R$ 320.500</td>
                    </tr>
                </tbody>
            </table>
        </div>

        <!-- METODOLOGIA -->
        <div class="section" style="border-color: #3730a3;">
            <h2><span class="icon">6.</span> Metodologia e Ressalvas</h2>
            <table>
                <tbody>
                    <tr>
                        <td style="width:180px; color:#a1a1aa;">Fonte de dados</td>
                        <td>Athena ps_bi (dbt BI mart) — valores em BRL, ja convertidos</td>
                    </tr>
                    <tr>
                        <td style="color:#a1a1aa;">Periodo</td>
                        <td>01/03 a 19/03/2026 (dados completos ate D-1)</td>
                    </tr>
                    <tr>
                        <td style="color:#a1a1aa;">Entrada no funil</td>
                        <td>FTD (primeiro deposito) no periodo analisado</td>
                    </tr>
                    <tr>
                        <td style="color:#a1a1aa;">Cohorts</td>
                        <td>New_Reg (registrado em marco) | Old_Reg (registrado antes de marco)</td>
                    </tr>
                    <tr>
                        <td style="color:#a1a1aa;">Janelas parciais</td>
                        <td>FTD na janela, conversao (STD/3&ordm;dep) ate current_date</td>
                    </tr>
                    <tr>
                        <td style="color:#a1a1aa;">LTV</td>
                        <td>Net Deposit = total depositado - total sacado (por FTD)</td>
                    </tr>
                    <tr>
                        <td style="color:#a1a1aa;">Recuperacao</td>
                        <td>Reg &lt; marco + sem aposta em fev + com aposta em marco (bet_count &gt; 0)</td>
                    </tr>
                    <tr>
                        <td style="color:#a1a1aa;">Filtros</td>
                        <td>is_test = false | Excluidos ~13K users com ftd_date recalculado pelo dbt</td>
                    </tr>
                </tbody>
            </table>
        </div>

        <div class="footer">
            Squad Intelligence Engine | Dados: Athena ps_bi | Gerado: 20/03/2026
        </div>

    </div>
</body>
</html>"""

    output_path = os.path.join(OUTPUT_DIR, 'crm_kpis_marco_2026_FINAL.html')
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"HTML gerado: {os.path.abspath(output_path)}")
    return output_path


def gerar_mensagem_whatsapp():
    """Gera mensagem formatada para WhatsApp/Slack."""

    msg = """*KPIs CRM - Marco 2026 (01 a 19/03)*
_Fonte: Athena ps_bi | Squad Intelligence Engine_

*1. Taxa de STD (2o deposito)*
New_Reg (reg marco): *33,29%* (5.563 / 16.710 FTDs)
Old_Reg (reg < marco): *27,51%* (589 / 2.141 FTDs)

Janela 01-07: New 31,66% | Old 27,48%
Janela 08-15: New 33,38% | Old 27,54%

*2. Taxa de 3o Deposito*
New_Reg: *16,98%* | Old_Reg: *12,14%*

*3. LTV (Net Deposit medio por FTD)*
New_Reg: *R$ 59,50* (total R$ 994K)
Old_Reg: *R$ 45,69* (total R$ 97K)

*4. Recuperacao*
Base elegivel: 1.001.071 (sem aposta em fev)
Recuperados: *12.428* (*1,24%*)
GGR: *R$ 336K* | NGR: *R$ 247K*
Turnover: R$ 12M | Net Deposit: R$ 320K

_Obs: excluidos ~13K users com ftd_date recalculado pelo dbt (investigacao em andamento)_
_Excel + HTML detalhado em anexo_"""

    output_path = os.path.join(OUTPUT_DIR, 'crm_kpis_marco_2026_msg.txt')
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(msg)
    print(f"Mensagem gerada: {os.path.abspath(output_path)}")
    return msg


if __name__ == '__main__':
    print("=" * 50)
    print("  Gerando entregaveis CRM")
    print("=" * 50)

    gerar_html()
    print()
    msg = gerar_mensagem_whatsapp()

    print()
    print("--- PREVIEW MENSAGEM WHATSAPP ---")
    print(msg)
    print()
    print("Entregaveis prontos em output/:")
    print("  1. crm_kpis_marco_2026_FINAL.xlsx  (dados)")
    print("  2. crm_kpis_marco_2026_FINAL.html  (report visual)")
    print("  3. crm_kpis_marco_2026_msg.txt     (mensagem WhatsApp)")
