/* ── DateTime Utilities ───────────────────────────────── */

const DateTimeUtil = {
    toISO(dateStr, timeStr) {
        if (!dateStr) return null;
        const t = timeStr || '00:00';
        return `${dateStr}T${t}:00Z`;
    },

    formatTimestamp(ts) {
        if (!ts) return '—';
        const d = new Date(ts);
        return d.toISOString().replace('T', ' ').slice(0, 19);
    },

    formatTime(ts) {
        if (!ts) return '—';
        const d = new Date(ts);
        return d.toISOString().slice(11, 19);
    },

    formatDate(ts) {
        if (!ts) return '—';
        const d = new Date(ts);
        return d.toISOString().slice(0, 10);
    },

    todayStr() {
        return new Date().toISOString().slice(0, 10);
    },
};
