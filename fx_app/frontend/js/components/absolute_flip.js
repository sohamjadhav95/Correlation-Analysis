/* ── Absolute Flip UI Component ───────────────────────────────────
 *
 * Handles all rendering for the Absolute Flip section.
 * Mirrors the existing single-analysis view but displays
 * distance-confirmed flip metrics and marks confirmed flip
 * events on the charts.
 * ─────────────────────────────────────────────────────────────── */

const AbsoluteFlipUI = {

    /* ── Sidebar config reader ─────────────────────────────────── */
    getConfig() {
        return {
            symbol1:    document.getElementById('af-symbol-1')?.value.trim() || '',
            symbol2:    document.getElementById('af-symbol-2')?.value.trim() || '',
            timeframe:  document.getElementById('af-timeframe')?.value || '1min',
            startDate:  document.getElementById('af-start-date')?.value || '',
            startTime:  document.getElementById('af-start-time')?.value || '00:00',
            endDate:    document.getElementById('af-end-date')?.value || '',
            endTime:    document.getElementById('af-end-time')?.value || '23:59',
            distanceN:  parseFloat(document.getElementById('af-distance-n')?.value || '5'),
        };
    },

    /* ── Entry point called from app.js ────────────────────────── */
    async run() {
        const cfg = this.getConfig();

        if (!cfg.symbol1 || !cfg.symbol2) {
            Toast.show('Please enter both asset symbols', 'error');
            return;
        }
        if (!cfg.startDate || !cfg.endDate) {
            Toast.show('Please select start and end dates', 'error');
            return;
        }
        if (isNaN(cfg.distanceN) || cfg.distanceN <= 0) {
            Toast.show('Distance N must be a positive number', 'error');
            return;
        }

        const start = DateTimeUtil.toISO(cfg.startDate, cfg.startTime);
        const end   = DateTimeUtil.toISO(cfg.endDate,   cfg.endTime);

        AppState.setLoading(true);
        Progress.show('Running Absolute Flip analysis...');
        Progress.update(20, `Fetching ticks for ${cfg.symbol1} & ${cfg.symbol2}...`);

        try {
            const result = await API.runAbsoluteFlip(
                'forex',
                cfg.symbol1, cfg.symbol2,
                cfg.timeframe, start, end,
                cfg.distanceN,
            );

            AppState.absoluteFlipResult = result;
            AppState.absoluteFlipConfig  = cfg;

            Progress.update(80, 'Rendering...');
            AppState.setView('absolute-flip');

            requestAnimationFrame(() => {
                this._render(result, cfg);
                Progress.update(100, 'Done');
                Toast.show(
                    `Absolute Flip: ${result.confirmed_flips.length} confirmed flip(s) across ${Format.integer(result.total_bars)} bars`,
                    'success',
                );
            });

        } catch (e) {
            Toast.show(`Absolute Flip failed: ${e.message}`, 'error');
        } finally {
            AppState.setLoading(false);
            setTimeout(() => Progress.hide(), 1500);
        }
    },

    /* ── Main render ────────────────────────────────────────────── */
    _render(result, cfg) {
        const { symbol1, symbol2, distanceN } = cfg;

        // ── Metrics ──
        this._renderMetrics(result.metrics, symbol1, symbol2, distanceN);

        // ── Charts ──
        this._renderCharts(result, symbol1, symbol2);

        // ── Confirmed flips detail table ──
        this._renderFlipEvents(result.confirmed_flips, symbol1, symbol2);

        // ── Bar-level data table ──
        this._renderDataTable(result.data);

        // ── CSV download hookup ──
        const dlBtn = document.getElementById('af-btn-download-csv');
        if (dlBtn) dlBtn.onclick = () => this._downloadCSV(result.data, symbol1, symbol2);
    },

    /* ── Metrics card ───────────────────────────────────────────── */
    _renderMetrics(metrics, sym1, sym2, n) {
        const container = document.getElementById('af-metrics-container');
        if (!container) return;

        const rows = Object.entries(metrics).map(([label, val]) => `
            <tr>
                <td class="metric-label">${label}</td>
                <td class="metric-value">${val}</td>
            </tr>
        `).join('');

        container.innerHTML = `
            <div class="metrics-header">
                <span class="metrics-pair">${sym1} / ${sym2}</span>
                <span class="metrics-badge af-badge">📏 Distance N = ${n}</span>
            </div>
            <table class="metrics-table">
                <tbody>${rows}</tbody>
            </table>
        `;
    },

    /* ── Charts ─────────────────────────────────────────────────── */
    _renderCharts(result, sym1, sym2) {
        const data  = result.data;
        const key1  = `${sym1}_index`;
        const key2  = `${sym2}_index`;
        const flips = data.filter(d => d.flip_occurred);

        // Index correlation chart
        Charts.drawLineChart('af-chart-correlation', data, {
            key1, key2,
            label1: sym1, label2: sym2,
            baseline: 1000, height: 400, yDecimals: 2,
        });

        // Spread chart — confirmed flips marked as vertical markers
        Charts.drawAreaChart('af-chart-spread', data, {
            key: 'index_spread', height: 300, yDecimals: 4,
            flips,
        });

        // Flip loss bar chart
        const flipLossData = data.filter(d => d.flip_loss > 0);
        Charts.drawBarChart('af-chart-flip-loss', flipLossData, {
            key: 'flip_loss', height: 250, yDecimals: 4,
        });

        // Spread distribution histogram
        const spreadVals = data.map(d => d.index_spread).filter(v => v != null);
        Charts.drawHistogram('af-chart-spread-dist', spreadVals, {
            height: 250, bins: 50, color: '#f59e0b',
        });

        // Position donut
        const positions = {};
        for (const d of data) {
            const p = d.current_position;
            positions[p] = (positions[p] || 0) + 1;
        }
        Charts.drawDonutChart('af-chart-position',
            Object.keys(positions),
            Object.values(positions),
            { height: 250 },
        );
    },

    /* ── Confirmed flip events table ────────────────────────────── */
    _renderFlipEvents(confirmedFlips, sym1, sym2) {
        const container = document.getElementById('af-flip-events-container');
        if (!container) return;

        if (!confirmedFlips || confirmedFlips.length === 0) {
            container.innerHTML = `
                <div class="af-no-flips">
                    <span class="af-no-flips-icon">✅</span>
                    <p>No confirmed flips — the spread never travelled the required distance past zero.</p>
                    <p class="af-hint">Try reducing Distance N to see more flips.</p>
                </div>`;
            return;
        }

        const rows = confirmedFlips.map((f, idx) => `
            <tr>
                <td>${idx + 1}</td>
                <td class="mono">${f.timestamp}</td>
                <td class="${f.spread_at_confirmation >= 0 ? 'val-pos' : 'val-neg'}">
                    ${f.spread_at_confirmation >= 0 ? '+' : ''}${f.spread_at_confirmation.toFixed(4)}
                </td>
                <td class="val-warn">${f.flip_loss.toFixed(4)}</td>
                <td class="pos-label">${f.new_position}</td>
            </tr>
        `).join('');

        container.innerHTML = `
            <table class="data-table af-events-table">
                <thead>
                    <tr>
                        <th>#</th>
                        <th>Confirmed At</th>
                        <th>Spread at Confirm</th>
                        <th>Flip Loss</th>
                        <th>New Position</th>
                    </tr>
                </thead>
                <tbody>${rows}</tbody>
            </table>
        `;
    },

    /* ── Bar-level data table (same format as standard analysis) ── */
    _renderDataTable(data) {
        const head = document.getElementById('af-data-table-head');
        const body = document.getElementById('af-data-table-body');
        if (!head || !body || !data.length) return;

        const cols = Object.keys(data[0]);

        head.innerHTML = `<tr>${cols.map(c => `<th>${c}</th>`).join('')}</tr>`;

        const PAGE = 200;
        let page   = 0;
        let filtered = data;

        const renderPage = () => {
            const slice = filtered.slice(page * PAGE, (page + 1) * PAGE);
            body.innerHTML = slice.map(row => `
                <tr class="${row.flip_occurred ? 'row-flip' : ''}">
                    ${cols.map(c => {
                        const v = row[c];
                        if (c === 'flip_occurred') return `<td>${v ? '🔴 YES' : ''}</td>`;
                        if (c === 'flip_loss')     return `<td class="${v > 0 ? 'val-warn' : ''}">${v > 0 ? v : ''}</td>`;
                        return `<td>${v ?? ''}</td>`;
                    }).join('')}
                </tr>
            `).join('');

            this._renderPagination(filtered.length, PAGE, page, (p) => { page = p; renderPage(); });
        };

        renderPage();

        // Search filter
        const searchInput = document.getElementById('af-table-search');
        if (searchInput) {
            searchInput.oninput = (e) => {
                const q = e.target.value.toLowerCase();
                filtered = q ? data.filter(r => Object.values(r).some(v => String(v).toLowerCase().includes(q))) : data;
                page = 0;
                renderPage();
            };
        }
    },

    _renderPagination(total, pageSize, current, onPage) {
        const pg = document.getElementById('af-pagination');
        if (!pg) return;

        const totalPages = Math.ceil(total / pageSize);
        if (totalPages <= 1) { pg.innerHTML = ''; return; }

        const pages = [];
        for (let i = 0; i < totalPages; i++) {
            pages.push(`
                <button class="page-btn ${i === current ? 'active' : ''}" data-page="${i}">
                    ${i + 1}
                </button>`);
        }

        pg.innerHTML = pages.join('');
        pg.querySelectorAll('.page-btn').forEach(btn => {
            btn.onclick = () => onPage(parseInt(btn.dataset.page));
        });
    },

    /* ── CSV download ───────────────────────────────────────────── */
    _downloadCSV(data, sym1, sym2) {
        if (!data || !data.length) { Toast.show('No data to download', 'error'); return; }
        const cols    = Object.keys(data[0]);
        const csvRows = [cols.join(',')];
        for (const row of data) csvRows.push(cols.map(c => row[c] ?? '').join(','));
        const blob = new Blob([csvRows.join('\n')], { type: 'text/csv' });
        const url  = URL.createObjectURL(blob);
        const a    = document.createElement('a');
        a.href     = url;
        a.download = `absolute_flip_${sym1}_${sym2}.csv`;
        a.click();
        URL.revokeObjectURL(url);
        Toast.show('CSV downloaded', 'success');
    },
};
