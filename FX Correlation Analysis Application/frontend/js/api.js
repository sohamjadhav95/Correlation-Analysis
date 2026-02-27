/* ── REST API Client ──────────────────────────────────── */

const API = {
    BASE: '',  // Same origin

    async _fetch(method, path, body = null) {
        const opts = {
            method,
            headers: { 'Content-Type': 'application/json' },
        };
        if (body) opts.body = JSON.stringify(body);

        const resp = await fetch(this.BASE + path, opts);
        const data = await resp.json();

        if (!resp.ok) {
            const msg = data.detail || data.message || `HTTP ${resp.status}`;
            throw new Error(msg);
        }
        return data;
    },

    // ── Data Fetch ──
    fetchForex(symbol, start, end) {
        return this._fetch('POST', '/api/forex/fetch', { symbol, start, end, use_cache: true });
    },

    fetchCrypto(symbol, start, end) {
        return this._fetch('POST', '/api/crypto/fetch', { symbol, start, end, use_cache: true });
    },

    fetchData(domain, symbol, start, end) {
        return domain === 'forex'
            ? this.fetchForex(symbol, start, end)
            : this.fetchCrypto(symbol, start, end);
    },

    getForexSymbols() {
        return this._fetch('GET', '/api/forex/symbols');
    },

    getCryptoSymbols() {
        return this._fetch('GET', '/api/crypto/symbols');
    },

    // ── Analysis ──
    runAnalysis(domain, symbol1, symbol2, timeframe, start, end) {
        return this._fetch('POST', '/api/analysis/run', {
            domain, symbol_1: symbol1, symbol_2: symbol2,
            timeframe, start, end,
        });
    },

    runCompare(setA, setB) {
        return this._fetch('POST', '/api/analysis/compare', { set_a: setA, set_b: setB });
    },

    // ── Super Test ──
    startSuperTest(domain, symbol1, symbol2, timeframe, date, startTime, endTime, intervalMin) {
        return this._fetch('POST', '/api/super-test/run', {
            domain, symbol_1: symbol1, symbol_2: symbol2,
            timeframe, date, start_time: startTime, end_time: endTime,
            interval_minutes: intervalMin,
        });
    },

    getSuperTestStatus(jobId) {
        return this._fetch('GET', `/api/super-test/status/${jobId}`);
    },

    getSuperTestResult(jobId) {
        return this._fetch('GET', `/api/super-test/result/${jobId}`);
    },

    // ── Config ──
    getConfig() {
        return this._fetch('GET', '/api/config');
    },

    getDataStatus() {
        return this._fetch('GET', '/api/data/status');
    },

    getTimeframes() {
        return this._fetch('GET', '/api/analysis/timeframes');
    },
};
