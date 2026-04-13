/* ── Sidebar Logic ────────────────────────────────────── */

const Sidebar = {
    init() {
        // Domain tabs
        document.querySelectorAll('.tab-btn').forEach(btn => {
            btn.onclick = () => {
                document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                AppState.setDomain(btn.dataset.domain);
                this._updateSymbolDefaults(btn.dataset.domain);
            };
        });

        // Mode buttons
        document.querySelectorAll('.mode-btn').forEach(btn => {
            btn.onclick = () => {
                document.querySelectorAll('.mode-btn').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                AppState.setMode(btn.dataset.mode);
                this._toggleSections(btn.dataset.mode);
            };
        });

        // Set defaults
        const today = DateTimeUtil.todayStr();
        document.getElementById('start-date').value = today;
        document.getElementById('end-date').value = today;
        document.getElementById('st-date').value = today;
    },

    _updateSymbolDefaults(domain) {
        const s1 = document.getElementById('symbol-1');
        const s2 = document.getElementById('symbol-2');
        if (domain === 'crypto') {
            s1.value = 'BTCUSDT';
            s2.value = 'ETHUSDT';
        } else {
            s1.value = 'XAUUSDm';
            s2.value = 'USDJPYm';
        }
    },

    _toggleSections(mode) {
        const setBConfig = document.getElementById('set-b-config');
        const stConfig = document.getElementById('super-test-config');

        setBConfig.classList.toggle('hidden', mode !== 'compare');
        stConfig.classList.toggle('hidden', mode !== 'super');
    },

    getConfig() {
        return {
            domain: AppState.domain,
            symbol1: document.getElementById('symbol-1').value.trim(),
            symbol2: document.getElementById('symbol-2').value.trim(),
            timeframe: document.getElementById('timeframe').value,
            startDate: document.getElementById('start-date').value,
            startTime: document.getElementById('start-time').value,
            endDate: document.getElementById('end-date').value,
            endTime: document.getElementById('end-time').value,
        };
    },

    getSetBConfig() {
        return {
            symbol1: document.getElementById('symbol-1b').value.trim(),
            symbol2: document.getElementById('symbol-2b').value.trim(),
            timeframe: document.getElementById('timeframe-b').value,
            startDate: document.getElementById('start-date-b').value,
            startTime: document.getElementById('start-time-b').value,
            endDate: document.getElementById('end-date-b').value,
            endTime: document.getElementById('end-time-b').value,
        };
    },

    getSuperTestConfig() {
        return {
            date: document.getElementById('st-date').value,
            startTime: document.getElementById('st-start').value,
            endTime: document.getElementById('st-end').value,
            intervalMinutes: parseInt(document.getElementById('st-interval').value) || 5,
        };
    },
};
