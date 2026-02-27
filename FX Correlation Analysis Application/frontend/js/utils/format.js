/* ── Formatting Utilities ─────────────────────────────── */

const Format = {
    number(val, decimals = 4) {
        if (val == null || isNaN(val)) return '—';
        return Number(val).toLocaleString(undefined, {
            minimumFractionDigits: decimals,
            maximumFractionDigits: decimals,
        });
    },

    integer(val) {
        if (val == null || isNaN(val)) return '—';
        return Number(val).toLocaleString();
    },

    percent(val, decimals = 2) {
        if (val == null || isNaN(val)) return '—';
        return Number(val).toFixed(decimals) + '%';
    },

    duration(ms) {
        if (ms < 1000) return `${Math.round(ms)}ms`;
        if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
        return `${(ms / 60000).toFixed(1)}m`;
    },

    fileSize(bytes) {
        if (bytes < 1024) return `${bytes} B`;
        if (bytes < 1048576) return `${(bytes / 1024).toFixed(1)} KB`;
        return `${(bytes / 1048576).toFixed(1)} MB`;
    },

    truncate(str, maxLen = 40) {
        if (!str || str.length <= maxLen) return str;
        return str.slice(0, maxLen - 3) + '...';
    },
};
