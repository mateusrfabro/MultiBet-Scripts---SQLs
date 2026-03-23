/**
 * Dashboard Google Ads — JavaScript
 *
 * Responsavel por:
 * - Carregar dados da API (/api/data)
 * - Renderizar KPI cards, graficos, insights e tabela
 * - Gerenciar filtro de periodo:
 *     "hourly" = Hoje ate agora vs Ontem mesmo horario (PRINCIPAL)
 *     "d1"     = D-1 vs D-2 (dias fechados, secundario)
 * - Auto-refresh a cada 30 minutos
 *
 * NOTA PARA O DESIGNER:
 * Este arquivo controla a LOGICA. Nao mexa aqui.
 * Para alterar cores e tamanhos, edite theme.css.
 */

// =========================================================================
// ESTADO GLOBAL
// =========================================================================
let dashboardData = null;
let currentCompare = 'hourly'; // 'hourly' (hoje vs ontem mesmo horario) ou 'd1' (D-1 vs D-2)

// Cores dos graficos (lidas do CSS para manter consistencia)
const COLORS = {
    casino:  getComputedStyle(document.documentElement).getPropertyValue('--cor-grafico-cassino').trim() || '#7C4DFF',
    sport:   getComputedStyle(document.documentElement).getPropertyValue('--cor-grafico-sport').trim() || '#00BFA5',
    ngr:     getComputedStyle(document.documentElement).getPropertyValue('--cor-grafico-ngr').trim() || '#FFB020',
    grid:    getComputedStyle(document.documentElement).getPropertyValue('--cor-grafico-grid').trim() || '#1E3044',
    text:    getComputedStyle(document.documentElement).getPropertyValue('--cor-texto-secundario').trim() || '#8B9CAF',
    aff1:    getComputedStyle(document.documentElement).getPropertyValue('--cor-aff-1').trim() || '#4A9EFF',
    aff2:    getComputedStyle(document.documentElement).getPropertyValue('--cor-aff-2').trim() || '#7C4DFF',
    aff3:    getComputedStyle(document.documentElement).getPropertyValue('--cor-aff-3').trim() || '#00BFA5',
};


// =========================================================================
// FETCH DADOS DA API
// =========================================================================
async function loadData() {
    try {
        const resp = await fetch('/api/data');
        if (resp.status === 401 || resp.status === 403) {
            window.location.href = '/login';
            return;
        }
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        dashboardData = await resp.json();

        if (dashboardData.error) {
            showError(dashboardData.error);
            return;
        }

        renderAll();
    } catch (err) {
        showError('Erro ao carregar dados: ' + err.message);
    }
}

function showError(msg) {
    document.getElementById('loading-state').innerHTML =
        `<p style="color: var(--cor-negativo);">${msg}</p>`;
}


// =========================================================================
// RENDER PRINCIPAL
// =========================================================================
function renderAll() {
    document.getElementById('loading-state').style.display = 'none';
    document.getElementById('dashboard-content').style.display = 'flex';
    document.getElementById('dashboard-content').style.flexDirection = 'column';
    document.getElementById('dashboard-content').style.gap = 'var(--gap-secoes)';

    renderUpdatedAt();
    renderSummary();
    renderKPIs();
    renderInsights();
    renderChartGGR();
    renderChartNGR();
    renderChartFunnel();
    renderChartDonut();
    renderTable();
}


// =========================================================================
// TIMESTAMP DE ATUALIZACAO
// =========================================================================
function renderUpdatedAt() {
    const el = document.getElementById('header-updated');
    const ts = dashboardData.query_started_at || dashboardData.updated_at || '';
    const h = dashboardData.hourly;
    if (h) {
        el.textContent = `Dados de: ${ts} | Comparativo ate ${h.hora_corte_brt}h BRT`;
    } else {
        el.textContent = `Dados de: ${ts}`;
    }
}


// =========================================================================
// HELPERS — dados do modo selecionado
// =========================================================================

/**
 * Retorna hora de corte do comparativo horario (ou null se indisponivel).
 */
function getHoraCorte() {
    const h = dashboardData.hourly;
    return (h && h.hora_corte_brt != null) ? h.hora_corte_brt : null;
}

/**
 * Metricas que existem no endpoint hourly.
 * Chave = nome no hourly.metrics, valor = true.
 */
const HOURLY_KEYS = {
    reg: true,
    ftd: true,
    ftd_deposit: true,
    dep_amount: true,
    dep_count: true,
    ggr_cassino: true,
    ggr_sport: true,
    ngr: true,
    saques: true,
};

/**
 * Dado uma metrica key, retorna { value, varPct, direction, subtitle }
 * conforme o modo ativo (hourly ou d1).
 *
 * - No modo hourly: usa hourly.metrics quando disponivel; se nao,
 *   faz fallback para today (parcial) sem variacao.
 * - No modo d1: usa d1 com variations_d1 (comportamento original).
 */
function getKpiData(key, format) {
    if (currentCompare === 'hourly') {
        const h = dashboardData.hourly;
        const corte = getHoraCorte();

        // Se a metrica existe no hourly
        if (h && h.metrics && h.metrics[key]) {
            const m = h.metrics[key];
            const v = m.var || { pct: 0, direction: 'neutral' };
            return {
                value: m.hoje,
                varPct: v.pct,
                direction: v.direction,
                showVar: true,
                label: `Hoje ate ${corte}h`,
                subtitle: `Ontem ate ${corte}h: ${fmtValue(m.ontem, format)}`,
                compareLabel: `vs ontem ${corte}h`,
            };
        }

        // Metrica SEM dados hourly — mostra today parcial sem variacao
        const todayVal = dashboardData.today[key];
        return {
            value: todayVal,
            varPct: 0,
            direction: 'neutral',
            showVar: false,
            label: 'Hoje (parcial)',
            subtitle: '',
            compareLabel: '',
        };
    }

    // Modo d1: dia fechado D-1 vs D-2
    const d1 = dashboardData.d1;
    const vars = dashboardData.variations_d1 || {};
    const v = vars[key] || { pct: 0, direction: 'neutral' };
    const today = dashboardData.today;

    return {
        value: d1[key],
        varPct: v.pct,
        direction: v.direction,
        showVar: true,
        label: 'Ontem (fechado)',
        subtitle: `Hoje (parcial): ${fmtValue(today[key], format)}`,
        compareLabel: 'vs anteontem',
    };
}

/**
 * Formata um valor numerico conforme o tipo (brl ou int).
 */
function fmtValue(val, format) {
    if (val == null || isNaN(val)) return '-';
    if (format === 'brl') return `R$ ${formatBRL(val)}`;
    return Number(val).toLocaleString('pt-BR');
}


// =========================================================================
// RESUMO EXECUTIVO
// =========================================================================
function renderSummary() {
    if (currentCompare === 'hourly') {
        renderSummaryHourly();
    } else {
        // Qualquer outro modo (d1, d7, etc.) usa D-1 fechado
        currentCompare = 'd1';
        renderSummaryD1();
    }
}

function renderSummaryHourly() {
    const h = dashboardData.hourly;
    const today = dashboardData.today;
    const corte = getHoraCorte();

    if (!h || !h.metrics) {
        // Fallback se hourly nao disponivel
        document.getElementById('summary-text').innerHTML =
            `<strong>Hoje (parcial):</strong> ` +
            `${today.ftd} FTDs, ` +
            `R$ ${formatBRL(today.dep_amount)} depositados, ` +
            `NGR R$ ${formatBRL(today.ngr)}. Dia em andamento.`;
        return;
    }

    const dep = h.metrics.dep_amount;
    const ggr = h.metrics.ggr_cassino;

    // Variacao depositos
    let depVar = '';
    if (dep && dep.var) {
        const arrow = dep.var.direction === 'up' ? '↑' : (dep.var.direction === 'down' ? '↓' : '→');
        const cls = dep.var.direction === 'up' ? 'highlight-positive' : (dep.var.direction === 'down' ? 'highlight-negative' : '');
        depVar = ` <span class="${cls}">${arrow} ${Math.abs(dep.var.pct)}% vs ontem mesmo horario</span>`;
    }

    // GGR cassino
    let ggrText = '';
    if (ggr) {
        ggrText = `, GGR Casino R$ <span class="highlight-number">${formatBRL(ggr.hoje)}</span>`;
    }

    document.getElementById('summary-text').innerHTML =
        `<strong>Hoje ate ${corte}h BRT:</strong> ` +
        `R$ <span class="highlight-number">${formatBRL(dep ? dep.hoje : today.dep_amount)}</span> depositados` +
        depVar + ggrText + `. Dia ainda em andamento.`;
}

function renderSummaryD1() {
    const d1 = dashboardData.d1;
    const today = dashboardData.today;
    const vars = dashboardData.variations_d1 || {};

    const ngrVar = vars.ngr || { pct: 0, direction: 'neutral' };
    const ngrClass = ngrVar.direction === 'up' ? 'highlight-positive' : 'highlight-negative';
    const ngrArrow = ngrVar.direction === 'up' ? '↑' : (ngrVar.direction === 'down' ? '↓' : '→');

    document.getElementById('summary-text').innerHTML =
        `<strong>Ontem (fechado):</strong> ` +
        `<span class="highlight-number">${d1.ftd}</span> FTDs, ` +
        `R$ <span class="highlight-number">${formatBRL(d1.dep_amount)}</span> depositados, ` +
        `NGR R$ <span class="highlight-number">${formatBRL(d1.ngr)}</span> ` +
        `<span class="${ngrClass}">${ngrArrow} ${Math.abs(ngrVar.pct)}%</span> vs anteontem. ` +
        `<br><strong>Hoje (parcial):</strong> ` +
        `${today.ftd} FTDs ate agora, ` +
        `R$ ${formatBRL(today.dep_amount)} depositados, ` +
        `NGR R$ ${formatBRL(today.ngr)}.`;
}


// =========================================================================
// KPI CARDS
// =========================================================================
function renderKPIs() {
    const metrics = [
        { key: 'reg',         label: 'REG (Cadastros)',        format: 'int' },
        { key: 'ftd',         label: 'FTD (1o Deposito)',      format: 'int' },
        { key: 'ftd_deposit', label: 'Valor FTD',              format: 'brl' },
        { key: 'dep_amount',  label: 'Total Depositado',       format: 'brl' },
        { key: 'ggr_cassino', label: 'GGR Casino',             format: 'brl' },
        { key: 'ggr_sport',   label: 'GGR Sport',              format: 'brl' },
        { key: 'ngr',         label: 'NGR (Receita Liquida)',   format: 'brl', highlight: true },
        { key: 'saques',      label: 'Saques',                 format: 'brl' },
    ];

    const grid = document.getElementById('kpi-grid');
    grid.innerHTML = '';

    metrics.forEach(m => {
        const kpi = getKpiData(m.key, m.format);
        const cardClass = m.highlight ? 'kpi-card destaque-ngr' : 'kpi-card';

        const fmtVal = fmtValue(kpi.value, m.format);

        // Variacao (seta + percentual)
        let varHtml = '';
        if (kpi.showVar) {
            const arrow = kpi.direction === 'up' ? '↑' : (kpi.direction === 'down' ? '↓' : '→');
            const varClass = `variacao-${kpi.direction}`;
            varHtml = `
                <span class="variacao ${varClass}">${arrow} ${Math.abs(kpi.varPct)}%</span>
                <span class="variacao-label">${kpi.compareLabel}</span>
            `;
        } else {
            varHtml = `<span class="variacao variacao-neutral">live</span>`;
        }

        // Subtitulo (contexto adicional)
        let subtitleHtml = '';
        if (kpi.subtitle) {
            subtitleHtml = `
                <div style="margin-top: 8px; font-size: 12px; color: var(--cor-texto-discreto);">
                    ${kpi.subtitle}
                </div>
            `;
        }

        grid.innerHTML += `
            <div class="${cardClass}">
                <div class="kpi-label">${kpi.label} — ${m.label}</div>
                <div class="kpi-value">${fmtVal}</div>
                ${varHtml}
                ${subtitleHtml}
            </div>
        `;
    });
}


// =========================================================================
// INSIGHTS
// =========================================================================
function renderInsights() {
    const list = document.getElementById('insights-list');

    if (currentCompare === 'hourly') {
        renderInsightsHourly(list);
    } else {
        renderInsightsD1(list);
    }
}

/**
 * Insights baseados no comparativo horario (hoje vs ontem mesmo horario).
 */
function renderInsightsHourly(list) {
    const h = dashboardData.hourly;
    if (!h || !h.metrics) {
        list.innerHTML = '<div class="insight-item insight-positive"><span class="insight-icon">i</span> Dados hourly indisponiveis. Insights baseados em dados parciais.</div>';
        return;
    }

    const corte = h.hora_corte_brt;
    const insights = [];

    // Analise por metrica hourly
    const metricsConfig = [
        { key: 'dep_amount', label: 'Depositos', positiveDir: 'up' },
        { key: 'dep_count', label: 'Qtd depositos', positiveDir: 'up' },
        { key: 'ggr_cassino', label: 'GGR Casino', positiveDir: 'up' },
        { key: 'saques', label: 'Saques', positiveDir: 'down' },
    ];

    metricsConfig.forEach(cfg => {
        const m = h.metrics[cfg.key];
        if (!m || !m.var) return;

        const pct = m.var.pct;
        const dir = m.var.direction;
        const absPct = Math.abs(pct);

        // So gera insight se a variacao for significativa (>= 10%)
        if (absPct < 10) return;

        const arrow = dir === 'up' ? '↑' : '↓';
        const isPositive = (dir === cfg.positiveDir);

        if (absPct >= 30) {
            // Variacao muito forte
            insights.push({
                level: isPositive ? 'positive' : 'critical',
                message: `${cfg.label}: ${arrow} ${absPct}% vs ontem ate ${corte}h ` +
                    `(R$ ${formatBRL(m.hoje)} vs R$ ${formatBRL(m.ontem)}). ` +
                    (isPositive ? 'Excelente performance!' : 'Atencao: queda acentuada.'),
            });
        } else if (absPct >= 10) {
            insights.push({
                level: isPositive ? 'positive' : 'warning',
                message: `${cfg.label}: ${arrow} ${absPct}% vs ontem ate ${corte}h ` +
                    `(R$ ${formatBRL(m.hoje)} vs R$ ${formatBRL(m.ontem)}).`,
            });
        }
    });

    // Insight de saques vs depositos (proporcao)
    const depM = h.metrics.dep_amount;
    const saqM = h.metrics.saques;
    if (depM && saqM && depM.hoje > 0) {
        const ratio = saqM.hoje / depM.hoje;
        if (ratio > 0.8) {
            insights.push({
                level: 'warning',
                message: `Saques representam ${(ratio * 100).toFixed(0)}% dos depositos ate ${corte}h. Monitorar fluxo de caixa.`,
            });
        }
    }

    if (insights.length === 0) {
        insights.push({
            level: 'positive',
            message: `Ate ${corte}h BRT: metricas dentro da normalidade comparando com ontem no mesmo horario.`,
        });
    }

    const icons = { critical: '!', warning: '!', positive: '✓', info: 'i' };
    list.innerHTML = insights.map(i => `
        <div class="insight-item insight-${i.level}">
            <span class="insight-icon">${icons[i.level] || 'i'}</span>
            <span>${i.message}</span>
        </div>
    `).join('');
}

/**
 * Insights baseados no comparativo D-1 vs D-2 (dias fechados).
 * Usa os insights pre-calculados da API quando disponiveis.
 */
function renderInsightsD1(list) {
    const insights = dashboardData.insights || [];

    if (insights.length === 0) {
        list.innerHTML = '<div class="insight-item insight-positive"><span class="insight-icon">✓</span> Tudo normal por enquanto.</div>';
        return;
    }

    const icons = { critical: '!', warning: '!', positive: '✓', info: 'i' };

    list.innerHTML = insights.map(i => `
        <div class="insight-item insight-${i.level}">
            <span class="insight-icon">${icons[i.level] || 'i'}</span>
            <span>${i.message}</span>
        </div>
    `).join('');
}


// =========================================================================
// GRAFICO: GGR Casino vs Sport (barras empilhadas, 7 dias)
// =========================================================================
let chartGGR = null;
function renderChartGGR() {
    const trend = dashboardData.trend || [];
    const labels = trend.map(d => formatDateShort(d.date));

    if (chartGGR) chartGGR.destroy();
    chartGGR = new Chart(document.getElementById('chart-ggr'), {
        type: 'bar',
        data: {
            labels,
            datasets: [
                {
                    label: 'GGR Casino',
                    data: trend.map(d => d.ggr_cassino),
                    backgroundColor: COLORS.casino,
                    borderRadius: 4,
                },
                {
                    label: 'GGR Sport',
                    data: trend.map(d => d.ggr_sport),
                    backgroundColor: COLORS.sport,
                    borderRadius: 4,
                },
            ],
        },
        options: chartOptions({ stacked: true }),
    });
}


// =========================================================================
// GRAFICO: NGR tendencia (linha, 7 dias)
// =========================================================================
let chartNGR = null;
function renderChartNGR() {
    const trend = dashboardData.trend || [];
    const labels = trend.map(d => formatDateShort(d.date));

    if (chartNGR) chartNGR.destroy();
    chartNGR = new Chart(document.getElementById('chart-ngr'), {
        type: 'line',
        data: {
            labels,
            datasets: [{
                label: 'NGR',
                data: trend.map(d => d.ngr),
                borderColor: COLORS.ngr,
                backgroundColor: COLORS.ngr + '20',
                fill: true,
                tension: 0.3,
                pointRadius: 4,
                pointBackgroundColor: COLORS.ngr,
            }],
        },
        options: chartOptions({}),
    });
}


// =========================================================================
// GRAFICO: Funil REG -> FTD (barras horizontais)
// =========================================================================
let chartFunnel = null;
function renderChartFunnel() {
    const d = dashboardData.today;

    if (chartFunnel) chartFunnel.destroy();
    chartFunnel = new Chart(document.getElementById('chart-funnel'), {
        type: 'bar',
        data: {
            labels: ['Cadastros (REG)', 'Primeiro Deposito (FTD)', 'Players Ativos'],
            datasets: [{
                data: [d.reg, d.ftd, d.players_ativos],
                backgroundColor: [COLORS.aff1, COLORS.aff2, COLORS.aff3],
                borderRadius: 6,
            }],
        },
        options: {
            indexAxis: 'y',
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                tooltip: {
                    callbacks: {
                        label: ctx => `  ${ctx.raw.toLocaleString('pt-BR')}`,
                    },
                },
            },
            scales: {
                x: {
                    grid: { color: COLORS.grid },
                    ticks: { color: COLORS.text },
                },
                y: {
                    grid: { display: false },
                    ticks: { color: COLORS.text, font: { size: 13 } },
                },
            },
        },
    });
}


// =========================================================================
// GRAFICO: Donut distribuicao por affiliate
// =========================================================================
let chartDonut = null;
function renderChartDonut() {
    const affs = dashboardData.by_affiliate || [];

    if (chartDonut) chartDonut.destroy();
    chartDonut = new Chart(document.getElementById('chart-donut'), {
        type: 'doughnut',
        data: {
            labels: affs.map(a => `Affiliate ${a.affiliate_id}`),
            datasets: [{
                data: affs.map(a => Math.max(a.ngr, 0)),
                backgroundColor: [COLORS.aff1, COLORS.aff2, COLORS.aff3],
                borderWidth: 0,
            }],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            cutout: '60%',
            plugins: {
                legend: {
                    position: 'right',
                    labels: { color: COLORS.text, padding: 16, font: { size: 13 } },
                },
                tooltip: {
                    callbacks: {
                        label: ctx => `  R$ ${formatBRL(ctx.raw)} (NGR)`,
                    },
                },
            },
        },
    });
}


// =========================================================================
// TABELA POR AFFILIATE
// =========================================================================
function renderTable() {
    const affs = dashboardData.by_affiliate || [];
    const today = dashboardData.today;
    const tbody = document.getElementById('affiliate-tbody');

    let rows = affs.map(a => `
        <tr>
            <td>${a.affiliate_id}</td>
            <td class="col-right">${a.reg.toLocaleString('pt-BR')}</td>
            <td class="col-right">${a.ftd.toLocaleString('pt-BR')}</td>
            <td class="col-right">${a.conv_pct}%</td>
            <td class="col-right">R$ ${formatBRL(a.ftd_deposit)}</td>
            <td class="col-right">R$ ${formatBRL(a.dep_amount)}</td>
            <td class="col-right">R$ ${formatBRL(a.ggr_cassino)}</td>
            <td class="col-right">R$ ${formatBRL(a.ggr_sport)}</td>
            <td class="col-right">R$ ${formatBRL(a.ngr)}</td>
            <td class="col-right">R$ ${formatBRL(a.saques)}</td>
        </tr>
    `).join('');

    // Linha total
    rows += `
        <tr class="row-total">
            <td>TOTAL</td>
            <td class="col-right">${today.reg.toLocaleString('pt-BR')}</td>
            <td class="col-right">${today.ftd.toLocaleString('pt-BR')}</td>
            <td class="col-right">${today.conv_pct}%</td>
            <td class="col-right">R$ ${formatBRL(today.ftd_deposit)}</td>
            <td class="col-right">R$ ${formatBRL(today.dep_amount)}</td>
            <td class="col-right">R$ ${formatBRL(today.ggr_cassino)}</td>
            <td class="col-right">R$ ${formatBRL(today.ggr_sport)}</td>
            <td class="col-right">R$ ${formatBRL(today.ngr)}</td>
            <td class="col-right">R$ ${formatBRL(today.saques)}</td>
        </tr>
    `;

    tbody.innerHTML = rows;
}


// =========================================================================
// UTILIDADES
// =========================================================================
function formatBRL(value) {
    if (value == null || isNaN(value)) return '0,00';
    return Number(value).toLocaleString('pt-BR', {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
    });
}

function formatDateShort(dateStr) {
    if (!dateStr) return '';
    const parts = dateStr.split('-');
    return `${parts[2]}/${parts[1]}`;
}

function chartOptions({ stacked = false }) {
    return {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
            legend: {
                labels: { color: COLORS.text, padding: 16 },
            },
            tooltip: {
                callbacks: {
                    label: ctx => `  R$ ${formatBRL(ctx.raw)}`,
                },
            },
        },
        scales: {
            x: {
                stacked,
                grid: { color: COLORS.grid },
                ticks: { color: COLORS.text },
            },
            y: {
                stacked,
                grid: { color: COLORS.grid },
                ticks: {
                    color: COLORS.text,
                    callback: val => `R$ ${(val / 1000).toFixed(0)}k`,
                },
            },
        },
    };
}


// =========================================================================
// FILTRO DE PERIODO
// =========================================================================
document.querySelectorAll('.period-btn').forEach(btn => {
    btn.addEventListener('click', () => {
        document.querySelectorAll('.period-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        currentCompare = btn.dataset.compare;
        if (dashboardData) renderAll();
    });
});


// =========================================================================
// BOTAO ATUALIZAR
// =========================================================================
document.getElementById('btn-refresh').addEventListener('click', async () => {
    const btn = document.getElementById('btn-refresh');
    btn.textContent = 'Atualizando...';
    btn.disabled = true;

    try {
        await fetch('/api/refresh', { method: 'POST' });
        await loadData();
    } finally {
        btn.textContent = 'Atualizar';
        btn.disabled = false;
    }
});


// =========================================================================
// AUTO-REFRESH (30 minutos)
// =========================================================================
setInterval(() => {
    loadData();
}, 30 * 60 * 1000);


// =========================================================================
// INIT
// =========================================================================
loadData();