/* ── Divergence Scanner UI ────────────────────────────── */

const DivergenceUI = {

    /* ── Progress ────────────────────────────────────────── */
    showProgress(show = true) {
        document.getElementById('divergence-progress').classList.toggle('hidden', !show);
    },

    updateProgress(completed, total) {
        const pct = total > 0 ? (completed / total) * 100 : 0;
        document.getElementById('div-progress-bar').style.width = pct + '%';
        document.getElementById('div-progress-text').textContent =
            `Scanning pair ${completed} of ${total} (${pct.toFixed(0)}%)`;
    },

    /* ── Main Results Renderer ───────────────────────────── */
    renderResults(result) {
        this.showProgress(false);
        this._renderSummaryCard(result);
        this._renderRankingsTable(result.rankings || []);
    },

    /* ── Panel 0: Summary Card ───────────────────────────── */
    _renderSummaryCard(result) {
        const s = result.summary || {};
        const el = document.getElementById('divergence-summary');
        el.classList.remove('hidden');

        el.innerHTML = `
            <div class="st-summary-grid">
                <div class="st-stat-card st-meta">
                    <div class="st-stat-label">📅 Scan Range</div>
                    <div class="st-stat-value">${result.scan_date || '—'} · ${result.scan_start_time || ''} → ${result.scan_end_time || ''} UTC</div>
                    <div class="st-stat-sub">${result.timeframe || '—'} · Window: ${result.window_bars || '—'} bars · ${s.total_pairs || 0} pairs scanned</div>
                </div>
                <div class="st-stat-card st-best">
                    <div class="st-stat-label">🏆 Best Pair</div>
                    <div class="st-stat-value st-highlight-green">${s.best_pair || '—'}</div>
                    <div class="st-stat-sub">Score ${Format.number(s.best_score || 0, 6)}</div>
                </div>
                <div class="st-stat-card">
                    <div class="st-stat-label">✅ Pairs With Data</div>
                    <div class="st-stat-value">${s.pairs_with_data || 0}</div>
                    <div class="st-stat-sub">of ${s.total_pairs || 0} total pairs</div>
                </div>
                <div class="st-stat-card">
                    <div class="st-stat-label">🪟 Avg % Clean Windows</div>
                    <div class="st-stat-value">${Format.number(s.avg_pct_clean_windows || 0, 1)}%</div>
                    <div class="st-stat-sub">Across all pairs (0 crossings)</div>
                </div>
                <div class="st-stat-card">
                    <div class="st-stat-label">⏱️ Elapsed</div>
                    <div class="st-stat-value">${Format.number(result.elapsed_seconds || 0, 1)}s</div>
                    <div class="st-stat-sub">${result.completed_pairs || 0} / ${result.total_pairs || 0} pairs succeeded</div>
                </div>
            </div>`;
    },

    /* ── Panel 1: Rankings Table ─────────────────────────── */
    _renderRankingsTable(rankings) {
        const container = document.getElementById('divergence-ranking-container');
        if (!container) return;

        if (!rankings || rankings.length === 0) {
            container.innerHTML = '<p class="empty-state">No successful pairs to rank.</p>';
            return;
        }

        const scores = rankings.map(r => r.score);
        const minS = Math.min(...scores);
        const maxS = Math.max(...scores);

        const scoreColor = (score) => {
            if (maxS === minS) return '';
            const pct = (score - minS) / (maxS - minS);
            return pct >= 0.8 ? 'st-cell-green' : pct >= 0.2 ? 'st-cell-yellow' : 'st-cell-red';
        };

        const cleanColor = (pct) => pct >= 80 ? 'st-cell-green' : pct >= 50 ? 'st-cell-yellow' : 'st-cell-red';
        const flipsColor = (avg) => avg < 1 ? 'st-cell-green' : avg <= 3 ? 'st-cell-yellow' : 'st-cell-red';
        const medalFor = (rank) => rank === 1 ? '🥇' : rank === 2 ? '🥈' : rank === 3 ? '🥉' : `#${rank}`;

        const rows = rankings.map(r => `
            <tr class="st-clickable-row" data-sym1="${r.sym1}" data-sym2="${r.sym2}"
                title="Click for full metrics — ${r.pair}">
                <td class="st-rank-cell">${medalFor(r.rank)}</td>
                <td><strong>${r.pair}</strong></td>
                <td class="${scoreColor(r.score)}">${Format.number(r.score, 6)}</td>
                <td class="${cleanColor(r.pct_zero_crossing_windows)}">${Format.number(r.pct_zero_crossing_windows, 1)}%</td>
                <td>${Format.number(r.avg_spread_growth, 4)}</td>
                <td>${Format.number(r.avg_spread_slope, 6)}</td>
                <td>${Format.number(r.avg_max_spread, 4)}</td>
                <td class="${flipsColor(r.avg_flips)}">${Format.number(r.avg_flips, 2)}</td>
            </tr>`).join('');

        container.innerHTML = `
            <h3 class="section-title" style="margin-top:0">🏆 Pair Rankings — Divergence Score</h3>
            <p class="st-table-note">Higher score = stronger, more consistent divergence. Click any row for full metrics.</p>
            <div class="table-container">
                <table class="data-table ranking-table">
                    <thead>
                        <tr>
                            <th>Rank</th>
                            <th>Pair</th>
                            <th>Score ↓</th>
                            <th>% Clean Windows</th>
                            <th>Avg Spread Growth</th>
                            <th>Avg Slope</th>
                            <th>Avg Max Spread</th>
                            <th>Avg Flips</th>
                        </tr>
                    </thead>
                    <tbody>${rows}</tbody>
                </table>
            </div>`;

        Tables.sortTable(container.querySelector('table'));
        this._bindRowClicks(container);
    },

    /* ── Row Click Delegation ────────────────────────────── */
    _bindRowClicks(container) {
        container.addEventListener('click', (e) => {
            const row = e.target.closest('.st-clickable-row');
            if (!row) return;
            const sym1 = row.dataset.sym1;
            const sym2 = row.dataset.sym2;
            if (sym1 && sym2) this.openDetail(sym1, sym2);
        });
    },

    /* ── Detail Modal ────────────────────────────────────── */
    openDetail(sym1, sym2) {
        const result = AppState.divergenceScanResult;
        if (!result) {
            Toast.show('No scan result — please re-run the scan', 'error');
            return;
        }

        const pairLabel = `${sym1}/${sym2}`;
        const pair = (result.rankings || []).find(r =>
            (r.sym1 === sym1 && r.sym2 === sym2) ||
            (r.sym1 === sym2 && r.sym2 === sym1)
        ) || (result.pairs || []).find(r =>
            (r.sym1 === sym1 && r.sym2 === sym2) ||
            (r.sym1 === sym2 && r.sym2 === sym1)
        );

        // Store current pair context for window drill-down
        AppState.divergenceScanContext = {
            ...(AppState.divergenceScanContext || {}),
            sym1,
            sym2,
        };

        const modal = document.getElementById('div-detail-modal');
        document.getElementById('div-detail-title').textContent = `📊 Full Metrics — ${pairLabel}`;
        document.getElementById('div-detail-subtitle').textContent =
            `${result.timeframe || ''} · ${result.window_bars || '—'} bar windows · ${result.scan_date || ''}`;

        const body = document.getElementById('div-detail-body');

        if (!pair || pair.status !== 'success') {
            body.innerHTML = `<p style="color:var(--accent-red);padding:32px 0;text-align:center">
                ❌ No data available for ${pairLabel}</p>`;
        } else {
            this._renderMetricsTable(body, pair);
            this._renderRobustnessNote(body, pair);
            this._renderAllWindowsTable(body, pair);
        }

        modal.classList.remove('hidden');

        // Bind close
        const close = () => {
            modal.classList.add('hidden');
            document.removeEventListener('keydown', esc);
        };
        document.getElementById('div-detail-close').onclick = close;
        document.getElementById('div-detail-backdrop').onclick = close;

        function esc(e) {
            // Only trigger if this modal is the top-most visible logic OR just hide the window and consume it
            // Actually, we should check if Window modal is NOT open, or just stop propagation.
            const winModal = document.getElementById('div-window-modal');
            if (e.key === 'Escape' && (!winModal || winModal.classList.contains('hidden'))) {
                close();
            }
        }
        // Remove any previous listener loosely laying around to be safe
        document.addEventListener('keydown', esc);
    },

    /* ── Panel 2: Full Metrics Table (Sectioned) ────────── */
    _renderMetricsTable(container, pair) {

        // ── Helper: build a metrics section ──────────────────
        const section = (title, rows) => `
            <div class="div-metrics-section">
                <div class="div-metrics-section-title">${title}</div>
                <table class="metrics-table" style="width:100%">
                    <tbody>
                        ${rows.map(([label, value, cls]) => `
                            <tr>
                                <td class="metric-name" style="color:var(--text-secondary)">${label}</td>
                                <td class="${cls || ''}" style="font-weight:700;color:var(--accent-blue);text-align:right">
                                    ${value ?? '—'}
                                </td>
                            </tr>`).join('')}
                    </tbody>
                </table>
            </div>`;

        // ── Color helpers ─────────────────────────────────────
        const ratioColor = (v) => v < 0.30 ? 'st-cell-green' : v < 0.60 ? 'st-cell-yellow' : v < 1.0 ? '' : 'st-cell-red';
        const viabilityLabel = {
            strong: '✅ Strong — flips cost <30% of avg spread',
            moderate: '⚠️ Moderate — flips cost 30-60% of avg spread',
            tight: '🟡 Tight — flips cost 60-100% of avg spread',
            not_viable: '❌ Not viable — flips exceed avg spread',
        }[pair.viability] || '—';
        const viabilityCls = {
            strong: 'st-cell-green', moderate: 'st-cell-yellow',
            tight: '', not_viable: 'st-cell-red',
        }[pair.viability] || '';

        // ── Flip distribution bar (visual bucket display) ─────
        const total = pair.windows_tested || 1;
        const distBar = (count, label, cls) => {
            const pct = Math.round(count / total * 100);
            return `<tr>
                <td style="color:var(--text-secondary);font-size:.82rem;padding:3px 0">${label}</td>
                <td style="width:160px;padding:3px 8px">
                    <div style="background:var(--bg-tertiary);border-radius:3px;height:10px;overflow:hidden">
                        <div class="${cls}" style="width:${pct}%;height:100%;background:currentColor;opacity:.7"></div>
                    </div>
                </td>
                <td style="font-size:.82rem;color:var(--text-muted);text-align:right">${count} (${pct}%)</td>
            </tr>`;
        };

        const flipDistHTML = `
            <div class="div-metrics-section">
                <div class="div-metrics-section-title">📊 Flip Distribution Across Windows</div>
                <table style="width:100%">
                    <tbody>
                        ${distBar(pair.flip_dist_zero, '0 flips (clean entry)', 'st-cell-green')}
                        ${distBar(pair.flip_dist_low, '1–3 flips (manageable)', 'st-cell-yellow')}
                        ${distBar(pair.flip_dist_mid, '4–7 flips (plan for this)', '')}
                        ${distBar(pair.flip_dist_high, '8+ flips (stress test)', 'st-cell-red')}
                    </tbody>
                </table>
                <p style="font-size:.78rem;color:var(--text-muted);margin-top:6px">
                    Size your minimum position so
                    <strong>${pair.max_flips_any_window}</strong> flips (worst window)
                    at 1x is still an acceptable entry cost.
                </p>
            </div>`;

        // ── Build all sections ────────────────────────────────
        container.innerHTML = '';

        // ── Profit % calculations (per-window) ────────────────
        const wins = pair.windows_data || [];
        const totalWins = wins.length || 1;
        const maxProfitCount = wins.filter(w => w.total_flip_loss < w.max_spread).length;
        const avgProfitCount = wins.filter(w => w.total_flip_loss < w.avg_spread).length;
        const maxProfitPct = (maxProfitCount / totalWins) * 100;
        const avgProfitPct = (avgProfitCount / totalWins) * 100;
        const profitCls = (pct) => pct >= 60 ? 'st-cell-green' : pct >= 30 ? 'st-cell-yellow' : 'st-cell-red';

        // Section 1 — Window overview
        container.innerHTML += section('📋 Window Overview', [
            ['Max Profit %', Format.number(maxProfitPct, 1) + '%', profitCls(maxProfitPct)],
            ['Avg Profit %', Format.number(avgProfitPct, 1) + '%', profitCls(avgProfitPct)],
            ['Windows Tested', pair.windows_tested],
            ['Windows with 0 Crossings', pair.windows_zero_crossings],
            ['% Zero-Crossing Windows', Format.number(pair.pct_zero_crossing_windows, 2) + '%'],
            ['Avg Spread Growth', Format.number(pair.avg_spread_growth, 4)],
            ['Avg Spread Slope', Format.number(pair.avg_spread_slope, 8)],
            ['Avg Max Spread', Format.number(pair.avg_max_spread, 4)],
            ['Avg Avg Spread', Format.number(pair.avg_avg_spread, 4)],
            ['Divergence Score', Format.number(pair.score, 6)],
        ]);

        // Section 2 — Phase 1 / Phase 2 analysis
        container.innerHTML += section('⚡ Phase Analysis (Entry Timing)', [
            ['Avg Phase 1 Length',
                `${Format.number(pair.avg_phase1_length, 1)} bars`,
                ''],
            ['Avg Phase 2 Length',
                `${Format.number(pair.avg_phase2_length, 1)} bars`,
                ''],
            ['Avg Post-Flip Spread Growth',
                Format.number(pair.avg_post_flip_growth, 4),
                pair.avg_post_flip_growth > 0 ? 'st-cell-green' : 'st-cell-red'],
            ['% Windows with Clean Phase 2',
                Format.number(pair.pct_clean_phase2, 1) + '%',
                pair.pct_clean_phase2 >= 60 ? 'st-cell-green' : pair.pct_clean_phase2 >= 30 ? 'st-cell-yellow' : 'st-cell-red'],
            ['Avg Flips / Window', Format.number(pair.avg_flips, 2)],
            ['Max Flips (any window)', pair.max_flips_any_window,
                pair.max_flips_any_window <= 5 ? 'st-cell-green' : pair.max_flips_any_window <= 10 ? 'st-cell-yellow' : 'st-cell-red'],
            ['Stop Scaling After',
                `${pair.stop_scaling_threshold} flips`,
                ''],
            ['Avg Flip Loss / Window', Format.number(pair.avg_flip_loss, 4)],
        ]);

        // Section 3 — Flip distribution visual
        container.innerHTML += flipDistHTML;

        // Section 4 — Viability ratios
        container.innerHTML += section('💰 Viability — Flip Cost vs Spread Potential', [
            ['Max Single Flip / Max Spread',
                Format.number(pair.ratio_maxflip_maxspread, 4),
                ratioColor(pair.ratio_maxflip_maxspread)],
            ['Total Flip Loss / Max Spread',
                Format.number(pair.ratio_totalflip_maxspread, 4),
                ratioColor(pair.ratio_totalflip_maxspread)],
            ['Total Flip Loss / Avg Spread  ← primary',
                Format.number(pair.ratio_totalflip_avgspread, 4),
                ratioColor(pair.ratio_totalflip_avgspread)],
            ['Viability Verdict', viabilityLabel, viabilityCls],
        ]);
    },

    /* ── Panel 3: All Windows Table ──────────────────────── */
    _renderAllWindowsTable(container, pair) {
        const windows = pair.windows_data;
        if (!windows || windows.length === 0) return;

        const medalFor = (rank) => rank === 1 ? '🥇' : rank === 2 ? '🥈' : rank === 3 ? '🥉' : `#${rank}`;

        // Find best window index for highlighting
        const bestScore = Math.max(...windows.map(w => w.window_score));

        const rows = windows.map(w => {
            const isBest = w.window_score === bestScore;
            const flipClass = w.total_flips === 0 ? 'st-cell-green' : w.total_flips <= 3 ? 'st-cell-yellow' : 'st-cell-red';
            const growthClass = w.spread_growth > 0 ? 'st-cell-green' : w.spread_growth < 0 ? 'st-cell-red' : '';

            // Format timestamp to HH:MM:SS only for readability
            const startLabel = w.window_start ? w.window_start.slice(11, 19) : '—';
            const endLabel = w.window_end ? w.window_end.slice(11, 19) : '—';

            return `
                <tr class="st-clickable-row ${isBest ? 'st-best-row' : ''}"
                    data-wstart="${w.window_start || ''}"
                    data-wend="${w.window_end || ''}"
                    title="Click to view full analysis for this window">
                    <td>${w.window_index + 1}</td>
                    <td>${startLabel}</td>
                    <td>${endLabel}</td>
                    <td>${w.total_bars}</td>
                    <td class="${flipClass}">${w.total_flips}</td>
                    <td>${Format.number(w.total_flip_loss, 4)}</td>
                    <td>${Format.number(w.avg_spread, 4)}</td>
                    <td>${Format.number(w.max_spread, 4)}</td>
                    <td>${Format.number(w.max_single_flip_loss, 4)}</td>
                    <td class="${growthClass}">${Format.number(w.spread_growth, 4)}</td>
                    <td>${Format.number(w.spread_slope, 6)}</td>
                    <td>${w.phase1_length ?? '—'}</td>
                    <td class="${(w.post_flip_growth > 0) ? 'st-cell-green' : 'st-cell-red'}">${Format.number(w.post_flip_growth, 4)}</td>
                    <td>${Format.number(w.window_score, 6)}</td>
                </tr>`;
        }).join('');

        const tableDiv = document.createElement('div');
        tableDiv.style.marginTop = '32px';
        tableDiv.innerHTML = `
            <h3 class="section-title" style="margin-top:0">📋 All Windows — Chronological</h3>
            <p class="st-table-note">
                ${windows.length} windows tested · 
                ${windows.filter(w => w.total_flips === 0).length} with zero crossings · 
                Best window highlighted in gold
            </p>
            <div class="table-container" style="max-height:420px;overflow-y:auto">
                <table class="data-table">
                    <thead>
                        <tr>
                            <th>#</th>
                            <th>Start</th>
                            <th>End</th>
                            <th>Bars</th>
                            <th>Flips</th>
                            <th>Flip Loss</th>
                            <th>Avg Spread</th>
                            <th>Max Spread</th>
                            <th>Max Single Loss</th>
                            <th>Spread Growth</th>
                            <th>Slope</th>
                            <th>Phase 1 Bars</th>
                            <th>Post-Flip Growth</th>
                            <th>Window Score</th>
                        </tr>
                    </thead>
                    <tbody>${rows}</tbody>
                </table>
            </div>`;

        container.appendChild(tableDiv);
        const tableEl = tableDiv.querySelector('table');
        Tables.sortTable(tableEl);

        // Bind window row clicks (to the table element, since sortTable might rebuild rows)
        tableEl.addEventListener('click', (e) => {
            const row = e.target.closest('.st-clickable-row');
            if (!row) return;
            const wstart = row.dataset.wstart;
            const wend = row.dataset.wend;
            if (wstart && wend) this.openWindowDetail(wstart, wend);
        });
    },

    /* ── Panel 4: Robustness Note ────────────────────────── */
    _renderRobustnessNote(container, pair) {
        const best = pair.best_window_score || 0;
        const avg = pair.avg_window_score || 0;
        const ratio = best !== 0 ? Math.min(avg / best, 1.0) : 0;

        const interp = ratio >= 0.7
            ? { label: 'Strong and consistent divergence', cls: 'st-cell-green' }
            : ratio >= 0.4
                ? { label: 'Moderate — some starting-point sensitivity', cls: 'st-cell-yellow' }
                : { label: 'Fragile — heavily dependent on start point', cls: 'st-cell-red' };

        const block = document.createElement('div');
        block.className = 'div-robustness-block';
        block.innerHTML = `
            <div class="div-robustness-header">🔬 Score Robustness</div>
            <p class="div-robustness-desc">
                This pair was scored across <strong>${pair.windows_tested}</strong> windows.
                A robust pair shows high scores across many windows, not just one lucky cut.
            </p>
            <div class="div-robustness-stats">
                <div class="div-rob-row"><span>Best window score</span><span>${Format.number(best, 6)}</span></div>
                <div class="div-rob-row"><span>Avg window score</span><span>${Format.number(avg, 6)}</span></div>
                <div class="div-rob-row">
                    <span>Ratio (avg / best)</span>
                    <span class="${interp.cls}" style="font-weight:700">${Format.number(ratio, 3)}</span>
                </div>
            </div>
            <div class="div-robustness-interp ${interp.cls}">
                ${ratio >= 0.7 ? '✅' : ratio >= 0.4 ? '⚠️' : '❌'} ${interp.label}
            </div>
            <div class="div-rob-legend">
                <span class="st-cell-green">≥ 0.7</span> Strong &amp; consistent &nbsp;·&nbsp;
                <span class="st-cell-yellow">0.4 – 0.7</span> Moderate &nbsp;·&nbsp;
                <span class="st-cell-red">&lt; 0.4</span> Fragile
            </div>`;
        container.appendChild(block);
    },

    /* ── Window Drill-Down ───────────────────────────────── */
    openWindowDetail(windowStart, windowEnd) {
        const scanCtx = AppState.divergenceScanContext;
        if (!scanCtx) {
            Toast.show('No scan context — please re-run the scan', 'error');
            return;
        }

        const modal = document.getElementById('div-window-modal');
        const loading = document.getElementById('div-window-loading');
        const results = document.getElementById('div-window-results');

        // Set header labels
        const startLabel = windowStart.slice(11, 19);
        const endLabel = windowEnd.slice(11, 19);
        document.getElementById('div-window-title').textContent =
            `Window Analysis: ${startLabel} → ${endLabel} UTC`;
        document.getElementById('div-window-subtitle').textContent =
            `${scanCtx.sym1.replace('__BASELINE__', '📏 Baseline')} / ${scanCtx.sym2.replace('__BASELINE__', '📏 Baseline')} · ${scanCtx.timeframe} · ${scanCtx.domain}`;

        // Reset
        loading.classList.remove('hidden');
        results.classList.add('hidden');
        document.getElementById('div-window-metrics').innerHTML = '';
        document.getElementById('div-window-table-head').innerHTML = '';
        document.getElementById('div-window-table-body').innerHTML = '';

        // Show modal
        modal.classList.remove('hidden');

        // Bind close
        const close = () => {
            modal.classList.add('hidden');
            document.removeEventListener('keydown', esc, true);
        };
        document.getElementById('div-window-close').onclick = close;
        document.getElementById('div-window-backdrop').onclick = close;

        function esc(e) {
            if (e.key === 'Escape') {
                e.stopImmediatePropagation();
                close();
            }
        }
        document.addEventListener('keydown', esc, true);

        // Fetch and render
        this._fetchAndRenderWindow(scanCtx, windowStart, windowEnd);
    },

    async _fetchAndRenderWindow(ctx, windowStart, windowEnd) {
        const loading = document.getElementById('div-window-loading');
        const results = document.getElementById('div-window-results');

        try {
            const result = await API.runAnalysis(
                ctx.domain, ctx.sym1, ctx.sym2,
                ctx.timeframe, windowStart, windowEnd
            );

            loading.classList.add('hidden');
            results.classList.remove('hidden');

            const data = result.data || [];
            const sym1 = ctx.sym1;
            const sym2 = ctx.sym2;

            // Summary metrics table
            Tables.renderMetrics('div-window-metrics', result.metrics, `${sym1} / ${sym2}`);

            // Download button
            document.getElementById('div-window-download').onclick = () => {
                if (!data.length) return;
                const cols = Object.keys(data[0]);
                const rows = [cols.join(','), ...data.map(r => cols.map(c => r[c] ?? '').join(','))];
                const blob = new Blob([rows.join('\n')], { type: 'text/csv' });
                const url = URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = url;
                a.download = `window_${windowStart.slice(11, 19)}_${windowEnd.slice(11, 19)}_${sym1}_${sym2}.csv`;
                a.click();
                URL.revokeObjectURL(url);
                Toast.show('CSV downloaded', 'success');
            };

            // Bar data table
            this._renderWindowTable(data);

            // Search on bar data table
            const searchInput = document.getElementById('div-window-search');
            if (searchInput) {
                searchInput.oninput = () => {
                    const q = searchInput.value.toLowerCase();
                    document.querySelectorAll('#div-window-table tbody tr').forEach(row => {
                        row.style.display = row.textContent.toLowerCase().includes(q) ? '' : 'none';
                    });
                };
            }

            // All charts — after paint
            requestAnimationFrame(() => {
                try {
                    // 1. Asset Index Correlation
                    Charts.drawLineChart('div-window-chart-corr', data, {
                        key1: `${sym1}_index`, key2: `${sym2}_index`,
                        label1: sym1, label2: sym2,
                        baseline: 1000, height: 260, yDecimals: 2,
                    });

                    // 2. Index Spread Over Time
                    Charts.drawAreaChart('div-window-chart-spread', data, {
                        key: 'index_spread', height: 260, yDecimals: 4,
                    });

                    // 3. Flip Loss Over Time (only bars where flip_occurred = true)
                    const flipData = data.filter(d => d.flip_occurred);
                    if (flipData.length > 0) {
                        Charts.drawBarChart('div-window-chart-fliploss', flipData, {
                            key: 'flip_loss', height: 220, yDecimals: 4,
                        });
                    }

                    // 4. Spread Distribution histogram
                    const spreadVals = data.map(d => d.index_spread).filter(v => v != null);
                    if (spreadVals.length > 0) {
                        Charts.drawHistogram('div-window-chart-hist', spreadVals, {
                            bins: 40, height: 220,
                        });
                    }

                    // 5. Position Breakdown donut
                    const positions = {};
                    data.forEach(d => {
                        const p = d.current_position || 'Unknown';
                        positions[p] = (positions[p] || 0) + 1;
                    });
                    const posLabels = Object.keys(positions);
                    const posValues = posLabels.map(k => positions[k]);
                    if (posLabels.length > 0) {
                        Charts.drawDonutChart('div-window-chart-donut', posLabels, posValues, {
                            height: 240,
                        });
                    }

                } catch (chartErr) {
                    console.warn('Window chart error:', chartErr);
                }
            });

        } catch (err) {
            loading.innerHTML = `
                <p style="color:var(--accent-red)">❌ Analysis failed: ${err.message}</p>
                <p style="color:var(--text-muted);font-size:.8rem;margin-top:8px">
                    Make sure data is available for this date range.</p>`;
        }
    },

    _renderWindowTable(data) {
        if (!data || data.length === 0) return;
        const cols = Object.keys(data[0]);
        const head = document.getElementById('div-window-table-head');
        const body = document.getElementById('div-window-table-body');
        if (!head || !body) return;

        head.innerHTML = `<tr>${cols.map(c => `<th>${c}</th>`).join('')}</tr>`;
        body.innerHTML = data.slice(0, 1000).map(row =>
            `<tr>${cols.map(c => {
                const v = row[c];
                return `<td>${typeof v === 'number' ? Format.number(v) : (v ?? '')}</td>`;
            }).join('')}</tr>`
        ).join('');

        Tables.sortTable(document.getElementById('div-window-table'));
    },
};
