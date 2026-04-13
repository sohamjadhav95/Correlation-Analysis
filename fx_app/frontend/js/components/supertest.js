/* ── Super Test UI ────────────────────────────────────── */

const SuperTestUI = {

    showProgress(show = true) {
        document.getElementById('super-test-progress').classList.toggle('hidden', !show);
    },

    updateProgress(progress, total) {
        const pct = total > 0 ? (progress / total) * 100 : 0;
        document.getElementById('st-progress-bar').style.width = pct + '%';
        document.getElementById('st-progress-text').textContent =
            `Processing window ${progress} of ${total} (${pct.toFixed(0)}%)`;
    },

    renderResults(result) {
        this.showProgress(false);
        this._renderSummaryCard(result);
        this._renderRankingTable(result.rankings || [], result.intervals || []);
        this._renderAllWindowsTable(result.intervals || []);
    },

    /* ── Summary Card ─────────────────────────────────── */
    _renderSummaryCard(result) {
        const s = result.summary || {};
        const el = document.getElementById('super-test-summary');
        el.classList.remove('hidden');

        const best = result.rankings && result.rankings[0];
        const worst = result.rankings && result.rankings[result.rankings.length - 1];

        el.innerHTML = `
            <div class="st-summary-grid">
                <div class="st-stat-card st-meta">
                    <div class="st-stat-label">📅 Analysis Range</div>
                    <div class="st-stat-value">${s.range_start || '—'} → ${s.range_end || '—'} UTC</div>
                    <div class="st-stat-sub">${s.step_minutes || '—'} min step · ${s.total_windows || 0} windows · ${s.successful || 0} succeeded</div>
                </div>
                <div class="st-stat-card st-best">
                    <div class="st-stat-label">🏆 Best Start Time</div>
                    <div class="st-stat-value st-highlight-green">${s.best_start_time || '—'} UTC</div>
                    <div class="st-stat-sub">Score ${Format.number(s.best_score || 0, 4)} · Flips ${s.best_flips || 0} · Loss ${Format.number(s.best_flip_loss || 0, 4)}</div>
                </div>
                <div class="st-stat-card st-worst">
                    <div class="st-stat-label">⚠️ Worst Start Time</div>
                    <div class="st-stat-value st-highlight-red">${s.worst_start_time || '—'} UTC</div>
                    <div class="st-stat-sub">Score ${Format.number(s.worst_score || 0, 4)}</div>
                </div>
                <div class="st-stat-card">
                    <div class="st-stat-label">📊 Avg Flip Loss</div>
                    <div class="st-stat-value">${Format.number(s.avg_flip_loss || 0, 4)}</div>
                    <div class="st-stat-sub">Min ${Format.number(s.min_flip_loss || 0, 4)} · Max ${Format.number(s.max_flip_loss || 0, 4)}</div>
                </div>
                <div class="st-stat-card">
                    <div class="st-stat-label">🔄 Avg Flips / Window</div>
                    <div class="st-stat-value">${Format.number(s.avg_flips || 0, 1)}</div>
                    <div class="st-stat-sub">Avg |Spread| ${Format.number(s.avg_spread || 0, 4)}</div>
                </div>
                <div class="st-stat-card">
                    <div class="st-stat-label">⏱️ Elapsed</div>
                    <div class="st-stat-value">${Format.number(result.elapsed_seconds || 0, 1)}s</div>
                    <div class="st-stat-sub">${result.completed_intervals || 0} / ${result.total_intervals || 0} completed</div>
                </div>
            </div>`;
    },

    /* ── Ranking Table (top 20) ───────────────────────── */
    _renderRankingTable(rankings, allIntervals) {
        const container = document.getElementById('super-test-ranking-container');
        if (!container) return;

        if (!rankings || rankings.length === 0) {
            container.innerHTML = '<p class="empty-state">No rankings available.</p>';
            return;
        }

        const top = rankings.slice(0, 20);
        const scores = rankings.map(r => r.score);
        const minS = Math.min(...scores), maxS = Math.max(...scores);

        const scoreColor = (score) => {
            if (maxS === minS) return '';
            const pct = (score - minS) / (maxS - minS);
            return pct < 0.2 ? 'st-cell-green' : pct < 0.5 ? 'st-cell-yellow' : 'st-cell-red';
        };

        const medalFor = (rank) => rank === 1 ? '🥇' : rank === 2 ? '🥈' : rank === 3 ? '🥉' : `#${rank}`;

        const rows = top.map(r => `
            <tr class="st-clickable-row" data-start="${r.interval_start}" data-end="${r.interval_end}"
                title="Click to view full analysis for this window">
                <td class="st-rank-cell">${medalFor(r.rank)}</td>
                <td class="st-time-cell"><strong>${r.start_time_label || r.interval_start.slice(11, 16)}</strong></td>
                <td>${r.window_minutes || '—'} min</td>
                <td>${Format.integer(r.total_bars)}</td>
                <td class="${r.total_flips <= 5 ? 'st-cell-green' : r.total_flips <= 10 ? 'st-cell-yellow' : 'st-cell-red'}">${r.total_flips}</td>
                <td class="${r.total_flip_loss < 1 ? 'st-cell-green' : r.total_flip_loss < 3 ? 'st-cell-yellow' : 'st-cell-red'}">${Format.number(r.total_flip_loss, 4)}</td>
                <td>${Format.number(r.avg_spread || 0, 4)}</td>
                <td>${Format.number(r.max_spread || 0, 4)}</td>
                <td class="${scoreColor(r.score)}">${Format.number(r.score, 4)}</td>
            </tr>`).join('');

        container.innerHTML = `
            <h3 class="section-title" style="margin-top:0">🏆 Top Start Times Ranking</h3>
            <p class="st-table-note">Click any row to view the full analysis for that window. Showing top ${top.length} of ${rankings.length}.</p>
            <div class="table-container">
                <table class="data-table ranking-table">
                    <thead>
                        <tr>
                            <th>Rank</th>
                            <th>Start Time (UTC)</th>
                            <th>Window</th>
                            <th>Bars</th>
                            <th>Flips</th>
                            <th>Flip Loss</th>
                            <th>Avg |Spread|</th>
                            <th>Max |Spread|</th>
                            <th>Score ↑</th>
                        </tr>
                    </thead>
                    <tbody>${rows}</tbody>
                </table>
            </div>`;

        Tables.sortTable(container.querySelector('table'));
        this._bindRowClicks(container);
    },

    /* ── All Windows Table (chronological) ───────────── */
    _renderAllWindowsTable(intervals) {
        const container = document.getElementById('super-test-all-container');
        if (!container) return;

        const successful = intervals.filter(r => r.status === 'success');
        if (successful.length === 0) { container.innerHTML = ''; return; }

        const rows = intervals.map(r => {
            const isOk = r.status === 'success';
            const startLabel = r.interval_start ? r.interval_start.slice(11, 16) : '—';
            return `
                <tr class="${isOk ? 'st-clickable-row' : 'st-row-dim'}"
                    ${isOk ? `data-start="${r.interval_start}" data-end="${r.interval_end}"` : ''}
                    ${isOk ? 'title="Click to view full analysis"' : ''}>
                    <td>${r.window_index != null ? r.window_index + 1 : '—'}</td>
                    <td>${startLabel}</td>
                    <td>${r.window_minutes || '—'} min</td>
                    <td class="${!isOk ? 'st-cell-dim' : ''}">
                        ${isOk ? Format.integer(r.total_bars) : `<span class="st-status-badge st-badge-${r.status}">${r.status}</span>`}
                    </td>
                    <td>${isOk ? r.total_flips : '—'}</td>
                    <td>${isOk ? Format.number(r.total_flip_loss, 4) : '—'}</td>
                    <td>${isOk ? Format.number(r.avg_spread || 0, 4) : '—'}</td>
                    <td>${isOk ? Format.number(r.max_spread || 0, 4) : '—'}</td>
                    <td>${isOk ? Format.number(r.max_single_flip_loss || 0, 4) : '—'}</td>
                </tr>`;
        }).join('');

        container.innerHTML = `
            <h3 class="section-title">📋 All Windows — Chronological</h3>
            <p class="st-table-note">Click any successful row (non-dimmed) to view its full analysis.</p>
            <div class="table-container">
                <table class="data-table">
                    <thead>
                        <tr>
                            <th>#</th>
                            <th>Start (UTC)</th>
                            <th>Duration</th>
                            <th>Bars</th>
                            <th>Flips</th>
                            <th>Flip Loss</th>
                            <th>Avg |Spread|</th>
                            <th>Max |Spread|</th>
                            <th>Max Single Loss</th>
                        </tr>
                    </thead>
                    <tbody>${rows}</tbody>
                </table>
            </div>`;

        Tables.sortTable(container.querySelector('table'));
        this._bindRowClicks(container);
    },

    /* ── Row Click Delegation ─────────────────────────── */
    _bindRowClicks(container) {
        container.addEventListener('click', (e) => {
            const row = e.target.closest('.st-clickable-row');
            if (!row) return;
            const start = row.dataset.start;
            const end = row.dataset.end;
            if (start && end) this.openDetail(start, end);
        });
    },

    /* ── Detail Modal ─────────────────────────────────── */
    openDetail(windowStart, windowEnd) {
        const ctx = AppState.superTestContext;
        if (!ctx) { Toast.show('No super test context — please re-run Super Test', 'error'); return; }

        const modal = document.getElementById('st-detail-modal');
        const loading = document.getElementById('st-detail-loading');
        const results = document.getElementById('st-detail-results');

        // Set title
        const startLabel = windowStart.slice(11, 16);
        const endLabel = windowEnd.slice(11, 16);
        document.getElementById('st-detail-title').textContent =
            `Window Analysis: ${startLabel} → ${endLabel} UTC`;
        document.getElementById('st-detail-subtitle').textContent =
            `${ctx.sym1} / ${ctx.sym2} · ${ctx.timeframe} · ${ctx.domain}`;

        // Reset state
        loading.classList.remove('hidden');
        results.classList.add('hidden');
        document.getElementById('st-detail-metrics').innerHTML = '';
        document.getElementById('st-detail-table-head').innerHTML = '';
        document.getElementById('st-detail-table-body').innerHTML = '';

        // Show modal
        modal.classList.remove('hidden');

        // Bind close
        const close = () => { modal.classList.add('hidden'); };
        document.getElementById('st-detail-close').onclick = close;
        document.getElementById('st-detail-backdrop').onclick = close;
        document.addEventListener('keydown', function esc(e) {
            if (e.key === 'Escape') { close(); document.removeEventListener('keydown', esc); }
        });

        // Trigger analysis
        this._fetchAndRenderDetail(ctx, windowStart, windowEnd);
    },

    async _fetchAndRenderDetail(ctx, windowStart, windowEnd) {
        const loading = document.getElementById('st-detail-loading');
        const results = document.getElementById('st-detail-results');
        this._detailData = null;

        try {
            const result = await API.runAnalysis(
                ctx.domain, ctx.sym1, ctx.sym2,
                ctx.timeframe, windowStart, windowEnd,
            );

            this._detailData = result.data;
            loading.classList.add('hidden');
            results.classList.remove('hidden');

            // Render metrics
            Tables.renderMetrics('st-detail-metrics', result.metrics,
                `${ctx.sym1} / ${ctx.sym2}`);
            Tables.sortTable(document.querySelector('#st-detail-metrics table'));

            // Render data table
            this._renderDetailTable(result.data, ctx.sym1, ctx.sym2);

            // Download button
            document.getElementById('st-detail-download').onclick = () =>
                this._downloadDetailCSV(result.data, ctx.sym1, ctx.sym2, windowStart, windowEnd);

            // Charts — after a frame so modal is painted
            requestAnimationFrame(() => {
                try {
                    Charts.drawLineChart('st-detail-chart-corr', result.data, {
                        key1: `${ctx.sym1}_index`,
                        key2: `${ctx.sym2}_index`,
                        label1: ctx.sym1, label2: ctx.sym2,
                        baseline: 1000, height: 260, yDecimals: 2,
                    });
                    Charts.drawAreaChart('st-detail-chart-spread', result.data, {
                        key: 'index_spread', height: 260, yDecimals: 4,
                    });
                } catch (e) {
                    console.warn('Detail chart error:', e);
                }
            });

        } catch (err) {
            loading.innerHTML = `
                <p style="color:var(--accent-red)">❌ Analysis failed: ${err.message}</p>
                <p style="color:var(--text-muted);font-size:.8rem;margin-top:8px">
                    Make sure data is fetched for this date range.</p>`;
        }
    },

    _renderDetailTable(data, sym1, sym2) {
        if (!data || data.length === 0) return;
        const cols = Object.keys(data[0]);
        const head = document.getElementById('st-detail-table-head');
        const body = document.getElementById('st-detail-table-body');

        head.innerHTML = `<tr>${cols.map(c => `<th>${c}</th>`).join('')}</tr>`;
        body.innerHTML = data.slice(0, 500).map(row =>
            `<tr>${cols.map(c => {
                const v = row[c];
                return `<td>${typeof v === 'number' ? Format.number(v) : (v ?? '')}</td>`;
            }).join('')}</tr>`
        ).join('');

        Tables.sortTable(document.getElementById('st-detail-table'));
    },

    _downloadDetailCSV(data, sym1, sym2, start, end) {
        if (!data || data.length === 0) return;
        const cols = Object.keys(data[0]);
        const rows = [cols.join(','), ...data.map(r => cols.map(c => r[c] ?? '').join(','))];
        const blob = new Blob([rows.join('\n')], { type: 'text/csv' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `window_${start.slice(11, 16)}_${end.slice(11, 16)}_${sym1}_${sym2}.csv`;
        a.click();
        URL.revokeObjectURL(url);
        Toast.show('CSV downloaded', 'success');
    },
};
