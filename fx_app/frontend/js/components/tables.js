/* ── Interactive Data Tables ──────────────────────────── */

const Tables = {
    PAGE_SIZE: 100,

    /* ── Universal Sort Enabler ───────────────────────── */
    /**
     * Attach click-to-sort on every <th> in any <table>.
     * Works on static innerHTML tables (supertest, metrics, compare).
     * Sorts the <tbody> rows in-place; toggles ASC ↔ DESC.
     */
    sortTable(tableEl) {
        if (!tableEl) return;
        const thead = tableEl.querySelector('thead');
        const tbody = tableEl.querySelector('tbody');
        if (!thead || !tbody) return;

        thead.querySelectorAll('th').forEach((th, colIdx) => {
            // Already wired up — skip
            if (th.dataset.sortable) return;
            th.dataset.sortable = '1';
            th.style.cursor = 'pointer';
            th.title = 'Click to sort';
            let asc = true;

            th.addEventListener('click', () => {
                // Clear other headers
                thead.querySelectorAll('th').forEach(t => {
                    if (t !== th) {
                        t.classList.remove('sorted-asc', 'sorted-desc');
                        delete t.dataset.sortDir;
                    }
                });

                // Toggle direction
                if (th.dataset.sortDir === 'asc') {
                    asc = false;
                    th.dataset.sortDir = 'desc';
                    th.classList.replace('sorted-asc', 'sorted-desc');
                } else {
                    asc = true;
                    th.dataset.sortDir = 'asc';
                    th.classList.remove('sorted-desc');
                    th.classList.add('sorted-asc');
                }

                // Sort rows
                const rows = Array.from(tbody.querySelectorAll('tr'));
                rows.sort((a, b) => {
                    const ta = a.cells[colIdx]?.textContent.trim() ?? '';
                    const tb = b.cells[colIdx]?.textContent.trim() ?? '';
                    const na = parseFloat(ta.replace(/[^0-9.\-]/g, ''));
                    const nb = parseFloat(tb.replace(/[^0-9.\-]/g, ''));
                    const numericBoth = !isNaN(na) && !isNaN(nb);
                    const cmp = numericBoth ? na - nb : ta.localeCompare(tb);
                    return asc ? cmp : -cmp;
                });

                rows.forEach(r => tbody.appendChild(r)); // re-insert in order
            });
        });
    },

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

        this.sortTable(container.querySelector('table'));
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

        this.sortTable(container.querySelector('table'));
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
};
