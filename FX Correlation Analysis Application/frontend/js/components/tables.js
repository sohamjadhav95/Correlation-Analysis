/* ── Interactive Data Tables ──────────────────────────── */

const Tables = {
    PAGE_SIZE: 100,

    /* ── Render Metrics Table ─────────────────────────── */
    renderMetrics(containerId, metrics, label = '') {
        const container = document.getElementById(containerId);
        if (!container) return;

        let rows = '';
        for (const [key, val] of Object.entries(metrics)) {
            rows += `<tr><td class="metric-name">${key}</td><td class="metric-value">${val}</td></tr>`;
        }

        container.innerHTML = `
            <table class="metrics-table">
                <thead><tr><th>Metric</th><th>${label || 'Value'}</th></tr></thead>
                <tbody>${rows}</tbody>
            </table>`;
    },

    /* ── Render Comparison Metrics ────────────────────── */
    renderCompareMetrics(containerId, m1, m2, label1, label2) {
        const container = document.getElementById(containerId);
        if (!container) return;

        let rows = '';
        for (const key of Object.keys(m1)) {
            rows += `<tr>
                <td class="metric-name">${key}</td>
                <td class="metric-value">${m1[key]}</td>
                <td class="metric-value">${m2[key]}</td>
            </tr>`;
        }

        container.innerHTML = `
            <table class="metrics-table">
                <thead><tr><th>Metric</th><th>${label1}</th><th>${label2}</th></tr></thead>
                <tbody>${rows}</tbody>
            </table>`;
    },

    /* ── Render Data Table (with pagination & sorting) ── */
    renderDataTable(data, columns = null) {
        if (!data || data.length === 0) {
            document.getElementById('data-table-head').innerHTML = '';
            document.getElementById('data-table-body').innerHTML =
                '<tr><td colspan="100" style="text-align:center;color:var(--text-muted);padding:40px;">No data</td></tr>';
            return;
        }

        this._tableData = data;
        this._tableCols = columns || Object.keys(data[0]);
        this._sortCol = null;
        this._sortAsc = true;
        this._page = 0;
        this._searchTerm = '';

        this._renderTablePage();

        // Search handler
        const searchInput = document.getElementById('table-search');
        if (searchInput) {
            searchInput.oninput = () => {
                this._searchTerm = searchInput.value.toLowerCase();
                this._page = 0;
                this._renderTablePage();
            };
        }
    },

    _renderTablePage() {
        const headEl = document.getElementById('data-table-head');
        const bodyEl = document.getElementById('data-table-body');
        const pagEl = document.getElementById('pagination');

        // Header
        let headerHtml = '<tr>';
        for (const col of this._tableCols) {
            const sortClass = this._sortCol === col ? (this._sortAsc ? 'sorted-asc' : 'sorted-desc') : '';
            headerHtml += `<th class="${sortClass}" data-col="${col}">${col}</th>`;
        }
        headerHtml += '</tr>';
        headEl.innerHTML = headerHtml;

        // Sorting click handlers
        headEl.querySelectorAll('th').forEach(th => {
            th.onclick = () => {
                const col = th.dataset.col;
                if (this._sortCol === col) this._sortAsc = !this._sortAsc;
                else { this._sortCol = col; this._sortAsc = true; }
                this._page = 0;
                this._renderTablePage();
            };
        });

        // Filter
        let filtered = this._tableData;
        if (this._searchTerm) {
            filtered = filtered.filter(row =>
                Object.values(row).some(v =>
                    String(v).toLowerCase().includes(this._searchTerm)
                )
            );
        }

        // Sort
        if (this._sortCol) {
            const col = this._sortCol;
            const dir = this._sortAsc ? 1 : -1;
            filtered = [...filtered].sort((a, b) => {
                const va = a[col];
                const vb = b[col];
                if (va == vb) return 0;
                if (typeof va === 'number' && typeof vb === 'number') return (va - vb) * dir;
                return String(va).localeCompare(String(vb)) * dir;
            });
        }

        // Paginate
        const totalPages = Math.ceil(filtered.length / this.PAGE_SIZE);
        const start = this._page * this.PAGE_SIZE;
        const pageData = filtered.slice(start, start + this.PAGE_SIZE);

        // Body
        let bodyHtml = '';
        for (const row of pageData) {
            bodyHtml += '<tr>';
            for (const col of this._tableCols) {
                const val = row[col];
                const isNum = typeof val === 'number';
                const cls = isNum ? 'val' : '';
                const display = isNum ? Format.number(val) : (val ?? '');
                bodyHtml += `<td class="${cls}">${display}</td>`;
            }
            bodyHtml += '</tr>';
        }
        bodyEl.innerHTML = bodyHtml || '<tr><td colspan="100" style="text-align:center;color:var(--text-muted)">No matches</td></tr>';

        // Pagination
        if (totalPages > 1) {
            let pagHtml = `<span class="page-info">${filtered.length} rows</span>`;
            pagHtml += `<button ${this._page === 0 ? 'disabled' : ''} onclick="Tables._page=0;Tables._renderTablePage()">«</button>`;
            pagHtml += `<button ${this._page === 0 ? 'disabled' : ''} onclick="Tables._page--;Tables._renderTablePage()">‹</button>`;
            pagHtml += `<span class="page-info">Page ${this._page + 1} / ${totalPages}</span>`;
            pagHtml += `<button ${this._page >= totalPages - 1 ? 'disabled' : ''} onclick="Tables._page++;Tables._renderTablePage()">›</button>`;
            pagHtml += `<button ${this._page >= totalPages - 1 ? 'disabled' : ''} onclick="Tables._page=${totalPages - 1};Tables._renderTablePage()">»</button>`;
            pagEl.innerHTML = pagHtml;
        } else {
            pagEl.innerHTML = `<span class="page-info">${filtered.length} rows</span>`;
        }
    },

    /* ── Super Test Ranking Table ─────────────────────── */
    renderRankingTable(rankings) {
        const headEl = document.getElementById('st-table-head');
        const bodyEl = document.getElementById('st-table-body');

        if (!rankings || rankings.length === 0) {
            headEl.innerHTML = '';
            bodyEl.innerHTML = '<tr><td colspan="100" style="text-align:center;color:var(--text-muted);padding:40px;">No results</td></tr>';
            return;
        }

        headEl.innerHTML = `<tr>
            <th>Rank</th>
            <th>Interval</th>
            <th>Bars</th>
            <th>Flips</th>
            <th>Total Flip Loss</th>
            <th>Max |Spread|</th>
            <th>Avg |Spread|</th>
            <th>Flip Rate</th>
            <th>Score</th>
        </tr>`;

        const maxScore = Math.max(...rankings.map(r => r.score || 0), 0.001);

        let bodyHtml = '';
        for (const r of rankings) {
            const rankClass = r.rank <= 3 ? `rank-${r.rank}` : '';
            const badgeClass = r.rank === 1 ? 'gold' : r.rank === 2 ? 'silver' : r.rank === 3 ? 'bronze' : 'normal';

            // Heatmap class based on score
            const scoreRatio = (r.score || 0) / maxScore;
            const heatClass = scoreRatio < 0.33 ? 'heat-cool' : scoreRatio < 0.66 ? 'heat-warm' : 'heat-hot';

            const start = r.interval_start ? r.interval_start.slice(11, 16) : '';
            const end = r.interval_end ? r.interval_end.slice(11, 16) : '';

            bodyHtml += `<tr class="${rankClass}">
                <td><span class="rank-badge ${badgeClass}">${r.rank}</span></td>
                <td>${start} – ${end}</td>
                <td>${Format.integer(r.total_bars)}</td>
                <td>${Format.integer(r.total_flips)}</td>
                <td>${Format.number(r.total_flip_loss)}</td>
                <td>${Format.number(r.max_spread)}</td>
                <td>${Format.number(r.avg_spread)}</td>
                <td>${Format.number(r.flip_rate, 6)}</td>
                <td class="${heatClass}">${Format.number(r.score, 6)}</td>
            </tr>`;
        }
        bodyEl.innerHTML = bodyHtml;
    },
};
