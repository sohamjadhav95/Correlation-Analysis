/* ── Main Application ─────────────────────────────────── */

const Toast = {
    show(message, type = 'info') {
        const container = document.getElementById('toast-container');
        const toast = document.createElement('div');
        toast.className = `toast ${type}`;
        toast.innerHTML = `<span>${type === 'success' ? '✅' : type === 'error' ? '❌' : 'ℹ️'}</span> ${message}`;
        container.appendChild(toast);
        setTimeout(() => toast.remove(), 4000);
    },
};

const App = {
    init() {
        Sidebar.init();
        Modal.init();
        this._bindButtons();
        this._bindViewSwitching();

        // Set initial view
        AppState.setView('welcome');

        console.log('🚀 FX Correlation Platform initialized');
    },

    _bindButtons() {
        document.getElementById('btn-fetch').onclick = () => this.handleFetch();
        document.getElementById('btn-run').onclick = () => this.handleRun();
        document.getElementById('btn-download-csv').onclick = () => this.handleDownload();
    },

    _bindViewSwitching() {
        AppState.on('viewChanged', (view) => {
            document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
            const el = document.getElementById(`view-${view}`);
            if (el) el.classList.add('active');
        });
    },

    /* ── Fetch Data ───────────────────────────────────── */
    async handleFetch() {
        const cfg = Sidebar.getConfig();

        if (!cfg.symbol1 || !cfg.symbol2) {
            Toast.show('Please enter both asset symbols', 'error');
            return;
        }
        if (!cfg.startDate || !cfg.endDate) {
            Toast.show('Please select start and end dates', 'error');
            return;
        }

        const start = DateTimeUtil.toISO(cfg.startDate, cfg.startTime);
        const end = DateTimeUtil.toISO(cfg.endDate, cfg.endTime);

        AppState.setLoading(true);
        Progress.show('Fetching data for Asset 1...');

        try {
            // Fetch asset 1
            Progress.update(20, `Fetching ${cfg.symbol1}...`);
            const r1 = await API.fetchData(cfg.domain, cfg.symbol1, start, end);

            // Fetch asset 2
            Progress.update(60, `Fetching ${cfg.symbol2}...`);
            const r2 = await API.fetchData(cfg.domain, cfg.symbol2, start, end);

            Progress.update(100, 'Data fetched successfully');
            Toast.show(
                `Fetched ${Format.integer(r1.ticks_fetched)} + ${Format.integer(r2.ticks_fetched)} ticks`,
                'success'
            );

            // If compare mode, also fetch Set B
            if (AppState.mode === 'compare') {
                const cfgB = Sidebar.getSetBConfig();
                const startB = DateTimeUtil.toISO(cfgB.startDate || cfg.startDate, cfgB.startTime);
                const endB = DateTimeUtil.toISO(cfgB.endDate || cfg.endDate, cfgB.endTime);

                Progress.show('Fetching Set B data...');
                await API.fetchData(cfg.domain, cfgB.symbol1, startB, endB);
                await API.fetchData(cfg.domain, cfgB.symbol2, startB, endB);
                Progress.update(100, 'Set B data fetched');
            }
        } catch (e) {
            Toast.show(`Fetch failed: ${e.message}`, 'error');
        } finally {
            AppState.setLoading(false);
            setTimeout(() => Progress.hide(), 1500);
        }
    },

    /* ── Run Analysis ─────────────────────────────────── */
    async handleRun() {
        const cfg = Sidebar.getConfig();
        const mode = AppState.mode;

        if (!cfg.symbol1 || !cfg.symbol2) {
            Toast.show('Please enter both asset symbols', 'error');
            return;
        }

        const start = DateTimeUtil.toISO(cfg.startDate, cfg.startTime);
        const end = DateTimeUtil.toISO(cfg.endDate, cfg.endTime);

        AppState.setLoading(true);
        Progress.show('Running analysis...');

        try {
            if (mode === 'single') {
                await this._runSingleAnalysis(cfg, start, end);
            } else if (mode === 'compare') {
                await this._runComparison(cfg, start, end);
            } else if (mode === 'super') {
                await this._runSuperTest(cfg);
            }
        } catch (e) {
            Toast.show(`Analysis failed: ${e.message}`, 'error');
        } finally {
            AppState.setLoading(false);
            Progress.hide();
        }
    },

    async _runSingleAnalysis(cfg, start, end) {
        Progress.update(30, 'Computing correlation...');

        const result = await API.runAnalysis(
            cfg.domain, cfg.symbol1, cfg.symbol2,
            cfg.timeframe, start, end,
        );

        AppState.analysisResult = result;
        Progress.update(80, 'Rendering charts...');

        // Switch view FIRST so canvas elements have dimensions
        AppState.setView('analysis');

        // Render after layout completes
        requestAnimationFrame(() => {
            // Render metrics
            Tables.renderMetrics('metrics-table-container', result.metrics,
                `${cfg.symbol1} / ${cfg.symbol2}`);

            // Render data table
            Tables.renderDataTable(result.data);

            // Render charts — isolated so chart errors don't kill the whole view
            try {
                this._renderAnalysisCharts(result.data, cfg.symbol1, cfg.symbol2);
            } catch (chartErr) {
                console.warn('Chart render error (non-fatal):', chartErr);
                Toast.show('Charts could not render — data is still shown below', 'info');
            }
        });

        Progress.update(100, 'Done');
        Toast.show(`Analysis complete: ${Format.integer(result.total_bars)} bars`, 'success');
    },

    async _runComparison(cfg, start, end) {
        const cfgB = Sidebar.getSetBConfig();
        const startB = DateTimeUtil.toISO(cfgB.startDate || cfg.startDate, cfgB.startTime);
        const endB = DateTimeUtil.toISO(cfgB.endDate || cfg.endDate, cfgB.endTime);

        Progress.update(30, 'Running Set A...');

        const result = await API.runCompare(
            {
                domain: cfg.domain, symbol_1: cfg.symbol1, symbol_2: cfg.symbol2,
                timeframe: cfg.timeframe, start, end,
            },
            {
                domain: cfg.domain, symbol_1: cfgB.symbol1, symbol_2: cfgB.symbol2,
                timeframe: cfgB.timeframe, start: startB, end: endB,
            },
        );

        AppState.compareResult = result;
        Progress.update(80, 'Rendering comparison...');

        // Switch view FIRST so canvas elements have dimensions
        AppState.setView('compare');

        requestAnimationFrame(() => {
            Tables.renderCompareMetrics(
                'compare-metrics-container',
                result.set_a.metrics, result.set_b.metrics,
                `Set A (${cfg.symbol1}/${cfg.symbol2})`,
                `Set B (${cfgB.symbol1}/${cfgB.symbol2})`,
            );
            this._renderComparisonCharts(result, cfg, cfgB);
        });

        Toast.show('Comparison complete', 'success');
    },

    async _runSuperTest(cfg) {
        const stCfg = Sidebar.getSuperTestConfig();

        if (!stCfg.date) {
            Toast.show('Please select a test date', 'error');
            return;
        }

        AppState.setView('super-test');
        SuperTestUI.showProgress(true);
        SuperTestUI.updateProgress(0, 1);

        // Submit job
        const job = await API.startSuperTest(
            cfg.domain, cfg.symbol1, cfg.symbol2,
            cfg.timeframe, stCfg.date, stCfg.startTime,
            stCfg.endTime, stCfg.intervalMinutes,
        );

        Toast.show(`Super Test started: ${job.total_intervals} intervals`, 'info');

        // Poll for results via WebSocket
        WS.connect(job.job_id,
            (data) => {
                if (data.progress != null) {
                    SuperTestUI.updateProgress(data.progress, data.total || job.total_intervals);
                }
                if (data.status === 'completed') {
                    WS.disconnect();
                    this._loadSuperTestResult(job.job_id);
                }
                if (data.status === 'failed') {
                    WS.disconnect();
                    Toast.show('Super Test failed', 'error');
                    SuperTestUI.showProgress(false);
                }
            },
            () => {
                // On WebSocket close, try to get result
                setTimeout(() => this._loadSuperTestResult(job.job_id), 500);
            }
        );
    },

    async _loadSuperTestResult(jobId) {
        try {
            const result = await API.getSuperTestResult(jobId);
            if (result.status === 'running') {
                // Still running, poll again
                setTimeout(() => this._loadSuperTestResult(jobId), 1000);
                return;
            }
            AppState.superTestResult = result;
            SuperTestUI.renderResults(result);
            Toast.show(`Super Test complete: ${result.completed_intervals || 0} intervals`, 'success');
        } catch (e) {
            Toast.show('Failed to load results: ' + e.message, 'error');
        }
    },

    /* ── Chart Rendering ──────────────────────────────── */
    _renderAnalysisCharts(data, sym1, sym2) {
        const key1 = `${sym1}_index`;
        const key2 = `${sym2}_index`;

        // Correlation chart
        Charts.drawLineChart('chart-correlation', data, {
            key1, key2,
            label1: sym1, label2: sym2,
            baseline: 1000, height: 400, yDecimals: 2,
        });

        // Spread chart
        Charts.drawAreaChart('chart-spread', data, {
            key: 'index_spread', height: 300, yDecimals: 4,
            flips: data.filter(d => d.flip_occurred),
        });

        // Flip loss bar chart
        const flipLossData = data.filter(d => d.flip_loss > 0);
        Charts.drawBarChart('chart-flip-loss', flipLossData, {
            key: 'flip_loss', height: 250, yDecimals: 4,
        });

        // Spread distribution
        const spreadValues = data.map(d => d.index_spread).filter(v => v != null);
        Charts.drawHistogram('chart-spread-dist', spreadValues, {
            height: 250, bins: 50, color: '#6395ff',
        });

        // Position donut
        const positions = {};
        for (const d of data) {
            const pos = d.current_position;
            positions[pos] = (positions[pos] || 0) + 1;
        }
        Charts.drawDonutChart('chart-position',
            Object.keys(positions),
            Object.values(positions),
            { height: 250 },
        );
    },

    _renderComparisonCharts(result, cfgA, cfgB) {
        const dataA = result.set_a.data;
        const dataB = result.set_b.data;

        if (dataA.length > 0) {
            Charts.drawLineChart('chart-corr-a', dataA, {
                key1: `${cfgA.symbol1}_index`, key2: `${cfgA.symbol2}_index`,
                label1: cfgA.symbol1, label2: cfgA.symbol2,
                baseline: 1000, height: 350, yDecimals: 2,
            });
            Charts.drawAreaChart('chart-spread-a', dataA, {
                key: 'index_spread', height: 250, yDecimals: 4,
            });
        }

        if (dataB.length > 0) {
            Charts.drawLineChart('chart-corr-b', dataB, {
                key1: `${cfgB.symbol1}_index`, key2: `${cfgB.symbol2}_index`,
                label1: cfgB.symbol1, label2: cfgB.symbol2,
                baseline: 1000, height: 350, yDecimals: 2,
            });
            Charts.drawAreaChart('chart-spread-b', dataB, {
                key: 'index_spread', height: 250, yDecimals: 4,
            });
        }
    },

    /* ── CSV Download ─────────────────────────────────── */
    handleDownload() {
        const result = AppState.analysisResult;
        if (!result || !result.data || result.data.length === 0) {
            Toast.show('No data to download', 'error');
            return;
        }

        const cols = Object.keys(result.data[0]);
        const csvRows = [cols.join(',')];
        for (const row of result.data) {
            csvRows.push(cols.map(c => row[c] ?? '').join(','));
        }

        const blob = new Blob([csvRows.join('\n')], { type: 'text/csv' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = 'correlation_output.csv';
        a.click();
        URL.revokeObjectURL(url);

        Toast.show('CSV downloaded', 'success');
    },
};

// ── Boot ─────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => App.init());
