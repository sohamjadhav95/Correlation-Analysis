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

        const close = () => modal.classList.add('hidden');
        document.getElementById('div-detail-close').onclick = close;
        document.getElementById('div-detail-backdrop').onclick = close;
        document.addEventListener('keydown', function esc(e) {
            if (e.key === 'Escape') { close(); document.removeEventListener('keydown', esc); }
        });
    },

    /* ── Panel 2: Full Metrics Table ────────────────────── */
    _renderMetricsTable(container, pair) {
        const rows = [
            ['Windows Tested', pair.windows_tested],
            ['Windows with 0 Crossings', pair.windows_zero_crossings],
            ['% Zero-Crossing Windows', Format.number(pair.pct_zero_crossing_windows, 2) + '%'],
            ['Avg Spread Growth', Format.number(pair.avg_spread_growth, 6)],
            ['Avg Spread Slope', Format.number(pair.avg_spread_slope, 8)],
            ['Avg Max Spread', Format.number(pair.avg_max_spread, 4)],
            ['Avg Avg Spread', Format.number(pair.avg_avg_spread, 4)],
            ['Avg Flips / Window', Format.number(pair.avg_flips, 3)],
            ['Avg Flip Loss / Window', Format.number(pair.avg_flip_loss, 6)],
            ['Best Window Score', Format.number(pair.best_window_score, 6)],
            ['Best Window Start Time', pair.best_window_start || '—'],
            ['Divergence Score', Format.number(pair.score, 6)],
        ].map(([k, v]) => `
            <tr>
                <td class="metric-name" style="color:var(--text-secondary)">${k}</td>
                <td style="font-weight:700;color:var(--accent-blue)">${v ?? '—'}</td>
            </tr>`).join('');

        const metricsDiv = document.createElement('div');
        metricsDiv.style.marginBottom = '24px';
        metricsDiv.innerHTML = `
            <table class="metrics-table" style="width:100%">
                <thead><tr><th>Metric</th><th>Value</th></tr></thead>
                <tbody>${rows}</tbody>
            </table>`;
        container.innerHTML = '';
        container.appendChild(metricsDiv);
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
                <tr class="${isBest ? 'st-best-row' : ''}">
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
                            <th>Window Score</th>
                        </tr>
                    </thead>
                    <tbody>${rows}</tbody>
                </table>
            </div>`;

        container.appendChild(tableDiv);
        Tables.sortTable(tableDiv.querySelector('table'));
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
};
