/* ── Application State Management ─────────────────────── */

const AppState = {
    domain: 'forex',       // 'forex' | 'crypto'
    mode: 'single',        // 'single' | 'compare' | 'super'
    loading: false,
    currentView: 'welcome', // 'welcome' | 'analysis' | 'compare' | 'super-test'

    // Analysis results
    analysisResult: null,
    compareResult: null,
    superTestResult: null,

    // Listeners
    _listeners: {},

    on(event, fn) {
        if (!this._listeners[event]) this._listeners[event] = [];
        this._listeners[event].push(fn);
    },

    emit(event, data) {
        (this._listeners[event] || []).forEach(fn => fn(data));
    },

    setDomain(d) {
        this.domain = d;
        this.emit('domainChanged', d);
    },

    setMode(m) {
        this.mode = m;
        this.emit('modeChanged', m);
    },

    setView(v) {
        this.currentView = v;
        this.emit('viewChanged', v);
    },

    setLoading(flag) {
        this.loading = flag;
        this.emit('loadingChanged', flag);
    },
};
