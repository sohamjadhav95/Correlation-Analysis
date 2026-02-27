/* ── Super Test UI ────────────────────────────────────── */

const SuperTestUI = {
    showProgress(show = true) {
        document.getElementById('super-test-progress').classList.toggle('hidden', !show);
    },

    updateProgress(progress, total) {
        const pct = total > 0 ? (progress / total) * 100 : 0;
        document.getElementById('st-progress-bar').style.width = pct + '%';
        document.getElementById('st-progress-text').textContent =
            `Processing interval ${progress} of ${total} (${pct.toFixed(0)}%)`;
    },

    showSummary(result) {
        const el = document.getElementById('super-test-summary');
        el.classList.remove('hidden');
        el.innerHTML = `
            <div style="display:flex;gap:24px;margin-bottom:16px; flex-wrap:wrap;">
                <div class="feature-item">
                    <span class="feature-icon">📊</span>
                    <span><strong>${result.total_intervals || 0}</strong> Total Intervals</span>
                </div>
                <div class="feature-item">
                    <span class="feature-icon">✅</span>
                    <span><strong>${result.completed_intervals || 0}</strong> Completed</span>
                </div>
                <div class="feature-item">
                    <span class="feature-icon">⏱️</span>
                    <span><strong>${Format.number(result.elapsed_seconds || 0, 1)}</strong>s Elapsed</span>
                </div>
            </div>`;
    },

    renderResults(result) {
        this.showProgress(false);
        this.showSummary(result);
        Tables.renderRankingTable(result.rankings || []);
    },
};
