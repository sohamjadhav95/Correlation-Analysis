/* ── Canvas Chart Renderer ────────────────────────────── */

const Charts = {
    COLORS: {
        line1: '#a78bfa',
        line2: '#fb923c',
        spread: '#34d399',
        flipDot: '#f87171',
        bar: '#f87171',
        barBorder: '#ef4444',
        baseline: '#475569',
        grid: 'rgba(255,255,255,0.05)',
        axis: '#64748b',
        text: '#94a3b8',
        bg: 'transparent',
        fillPos: 'rgba(52,211,153,0.08)',
        fillNeg: 'rgba(248,113,113,0.08)',
    },
    PADDING: { top: 20, right: 60, bottom: 35, left: 70 },
    MIN_W: 200,

    /* ── Shared canvas setup ──────────────────────────── */
    _setup(canvasId, opts = {}) {
        const canvas = document.getElementById(canvasId);
        if (!canvas) return null;

        const ctx = canvas.getContext('2d');
        const dpr = window.devicePixelRatio || 1;
        const rect = canvas.parentElement.getBoundingClientRect();
        const w = Math.max(this.MIN_W, rect.width - 32);
        const h = opts.height || 400;

        canvas.width = w * dpr;
        canvas.height = h * dpr;
        canvas.style.width = w + 'px';
        canvas.style.height = h + 'px';
        ctx.scale(dpr, dpr);
        ctx.clearRect(0, 0, w, h);

        const P = this.PADDING;
        const plotW = Math.max(10, w - P.left - P.right);
        const plotH = Math.max(10, h - P.top - P.bottom);

        return { canvas, ctx, dpr, w, h, P, plotW, plotH };
    },

    /* ── Line Chart (dual series) ─────────────────────── */
    drawLineChart(canvasId, data, opts = {}) {
        if (!data || data.length === 0) return;
        const s = this._setup(canvasId, opts);
        if (!s) return;
        const { ctx, w, h, P, plotW, plotH } = s;

        const key1 = opts.key1 || 'y1';
        const key2 = opts.key2 || 'y2';
        const label1 = opts.label1 || 'Series 1';
        const label2 = opts.label2 || 'Series 2';

        const vals1 = data.map(d => d[key1]);
        const vals2 = data.map(d => d[key2]);
        const allVals = [...vals1, ...vals2].filter(v => v != null);
        if (allVals.length === 0) return;
        const yMin = Math.min(...allVals);
        const yMax = Math.max(...allVals);
        const yRange = yMax - yMin || 1;
        const yPad = yRange * 0.05;

        const toX = (i) => P.left + (i / Math.max(1, data.length - 1)) * plotW;
        const toY = (v) => P.top + plotH - ((v - (yMin - yPad)) / (yRange + yPad * 2)) * plotH;

        this._drawGrid(ctx, w, h, P, plotW, plotH);

        if (opts.baseline != null) {
            const by = toY(opts.baseline);
            ctx.beginPath();
            ctx.setLineDash([4, 4]);
            ctx.strokeStyle = this.COLORS.baseline;
            ctx.lineWidth = 1;
            ctx.moveTo(P.left, by);
            ctx.lineTo(P.left + plotW, by);
            ctx.stroke();
            ctx.setLineDash([]);
        }

        this._drawLine(ctx, data, toX, toY, key1, this.COLORS.line1, 2);
        this._drawLine(ctx, data, toX, toY, key2, this.COLORS.line2, 2);
        this._drawXLabels(ctx, data, toX, h, P, opts.timeKey || 'timestamp');
        this._drawYLabels(ctx, P, plotH, yMin - yPad, yMax + yPad, opts.yDecimals || 2);
        this._drawLegend(ctx, w, P, [
            { label: label1, color: this.COLORS.line1 },
            { label: label2, color: this.COLORS.line2 },
        ]);
    },

    /* ── Area Chart (spread) ──────────────────────────── */
    drawAreaChart(canvasId, data, opts = {}) {
        if (!data || data.length === 0) return;
        const s = this._setup(canvasId, { height: opts.height || 300 });
        if (!s) return;
        const { ctx, w, h, P, plotW, plotH } = s;

        const key = opts.key || 'y';
        const vals = data.map(d => d[key]).filter(v => v != null);
        if (vals.length === 0) return;
        const yMin = Math.min(...vals, 0);
        const yMax = Math.max(...vals, 0);
        const yRange = (yMax - yMin) || 1;
        const yPad = yRange * 0.1;

        const toX = (i) => P.left + (i / Math.max(1, data.length - 1)) * plotW;
        const toY = (v) => P.top + plotH - ((v - (yMin - yPad)) / (yRange + yPad * 2)) * plotH;

        this._drawGrid(ctx, w, h, P, plotW, plotH);

        const zeroY = toY(0);
        ctx.beginPath();
        ctx.setLineDash([5, 3]);
        ctx.strokeStyle = this.COLORS.baseline;
        ctx.lineWidth = 1;
        ctx.moveTo(P.left, zeroY);
        ctx.lineTo(P.left + plotW, zeroY);
        ctx.stroke();
        ctx.setLineDash([]);

        ctx.beginPath();
        ctx.moveTo(toX(0), zeroY);
        for (let i = 0; i < data.length; i++) {
            ctx.lineTo(toX(i), toY(data[i][key] ?? 0));
        }
        ctx.lineTo(toX(data.length - 1), zeroY);
        ctx.closePath();

        const gradient = ctx.createLinearGradient(0, P.top, 0, P.top + plotH);
        gradient.addColorStop(0, 'rgba(52,211,153,0.12)');
        gradient.addColorStop(0.5, 'rgba(52,211,153,0.02)');
        gradient.addColorStop(1, 'rgba(248,113,113,0.12)');
        ctx.fillStyle = gradient;
        ctx.fill();

        this._drawLine(ctx, data, toX, toY, key, this.COLORS.spread, 1.5);
        this._drawXLabels(ctx, data, toX, h, P, opts.timeKey || 'timestamp');
        this._drawYLabels(ctx, P, plotH, yMin - yPad, yMax + yPad, opts.yDecimals || 4);
    },

    /* ── Bar Chart (flip loss) ────────────────────────── */
    drawBarChart(canvasId, data, opts = {}) {
        if (!data || data.length === 0) return;
        const s = this._setup(canvasId, { height: opts.height || 250 });
        if (!s) return;
        const { ctx, w, h, P, plotW, plotH } = s;

        const key = opts.key || 'y';
        const vals = data.map(d => d[key]);
        const yMax = Math.max(...vals, 0.001);
        const yPad = yMax * 0.1;

        const barW = Math.max(1, (plotW / data.length) * 0.7);
        const gap = (plotW / data.length) * 0.3;

        const toX = (i) => P.left + (i / data.length) * plotW + gap / 2;
        const toY = (v) => P.top + plotH - (v / (yMax + yPad)) * plotH;

        this._drawGrid(ctx, w, h, P, plotW, plotH);

        for (let i = 0; i < data.length; i++) {
            const x = toX(i);
            const y = toY(vals[i]);
            const bh = P.top + plotH - y;

            ctx.fillStyle = this.COLORS.bar;
            ctx.globalAlpha = 0.7;
            ctx.fillRect(x, y, barW, bh);
            ctx.globalAlpha = 1;

            ctx.strokeStyle = this.COLORS.barBorder;
            ctx.lineWidth = 0.5;
            ctx.strokeRect(x, y, barW, bh);
        }

        this._drawYLabels(ctx, P, plotH, 0, yMax + yPad, opts.yDecimals || 4);
    },

    /* ── Histogram ────────────────────────────────────── */
    drawHistogram(canvasId, values, opts = {}) {
        if (!values || values.length === 0) return;
        const s = this._setup(canvasId, { height: opts.height || 250 });
        if (!s) return;
        const { ctx, P, plotW, plotH } = s;

        const bins = opts.bins || 40;
        const min = Math.min(...values);
        const max = Math.max(...values);
        const range = max - min || 1;
        const binWidth = range / bins;

        const counts = new Array(bins).fill(0);
        for (const v of values) {
            let idx = Math.floor((v - min) / binWidth);
            if (idx >= bins) idx = bins - 1;
            counts[idx]++;
        }

        const maxCount = Math.max(...counts, 1);
        const barW = plotW / bins;

        for (let i = 0; i < bins; i++) {
            const x = P.left + i * barW;
            const bh = (counts[i] / maxCount) * plotH;
            const y = P.top + plotH - bh;

            ctx.fillStyle = opts.color || this.COLORS.line1;
            ctx.globalAlpha = 0.6;
            ctx.fillRect(x, y, barW - 1, bh);
            ctx.globalAlpha = 1;
        }

        this._drawYLabels(ctx, P, plotH, 0, maxCount, 0);
    },

    /* ── Donut Chart (position breakdown) ─────────────── */
    drawDonutChart(canvasId, labels, values, opts = {}) {
        if (!values || values.length === 0) return;
        const s = this._setup(canvasId, { height: opts.height || 250 });
        if (!s) return;
        const { ctx, w, h } = s;

        const total = values.reduce((a, b) => a + b, 0);
        if (total === 0) return;

        const cx = w / 2;
        const cy = h / 2;
        const outerR = Math.max(10, Math.min(w, h) / 2 - 30);
        const innerR = outerR * 0.55;
        const colors = [this.COLORS.line1, this.COLORS.line2, '#22d3ee', '#f472b6'];

        let startAngle = -Math.PI / 2;
        for (let i = 0; i < values.length; i++) {
            const sliceAngle = (values[i] / total) * Math.PI * 2;

            ctx.beginPath();
            ctx.arc(cx, cy, outerR, startAngle, startAngle + sliceAngle);
            ctx.arc(cx, cy, innerR, startAngle + sliceAngle, startAngle, true);
            ctx.closePath();
            ctx.fillStyle = colors[i % colors.length];
            ctx.fill();

            const midAngle = startAngle + sliceAngle / 2;
            const labelR = outerR + 16;
            const lx = cx + Math.cos(midAngle) * labelR;
            const ly = cy + Math.sin(midAngle) * labelR;

            ctx.fillStyle = this.COLORS.text;
            ctx.font = '11px Inter';
            ctx.textAlign = midAngle > Math.PI / 2 && midAngle < Math.PI * 1.5 ? 'right' : 'left';
            ctx.fillText(`${labels[i]} (${((values[i] / total) * 100).toFixed(1)}%)`, lx, ly);

            startAngle += sliceAngle;
        }
    },

    /* ── Internal Helpers ─────────────────────────────── */
    _drawLine(ctx, data, toX, toY, key, color, width) {
        ctx.beginPath();
        ctx.strokeStyle = color;
        ctx.lineWidth = width;
        ctx.lineJoin = 'round';

        let started = false;
        for (let i = 0; i < data.length; i++) {
            const v = data[i][key];
            if (v == null) continue;
            const x = toX(i);
            const y = toY(v);
            if (!started) { ctx.moveTo(x, y); started = true; }
            else ctx.lineTo(x, y);
        }
        ctx.stroke();
    },

    _drawGrid(ctx, w, h, P, plotW, plotH) {
        ctx.strokeStyle = this.COLORS.grid;
        ctx.lineWidth = 1;

        for (let i = 0; i <= 5; i++) {
            const y = P.top + (plotH / 5) * i;
            ctx.beginPath();
            ctx.moveTo(P.left, y);
            ctx.lineTo(P.left + plotW, y);
            ctx.stroke();
        }
    },

    _drawXLabels(ctx, data, toX, h, P, timeKey) {
        ctx.fillStyle = this.COLORS.axis;
        ctx.font = '10px Inter';
        ctx.textAlign = 'center';

        const maxLabels = 8;
        const step = Math.max(1, Math.floor(data.length / maxLabels));

        for (let i = 0; i < data.length; i += step) {
            const x = toX(i);
            const label = data[i][timeKey] || '';
            const short = label.length > 10 ? label.slice(11, 16) : label.slice(0, 10);
            ctx.fillText(short, x, h - P.bottom + 18);
        }
    },

    _drawYLabels(ctx, P, plotH, yMin, yMax, decimals) {
        ctx.fillStyle = this.COLORS.axis;
        ctx.font = '10px Inter';
        ctx.textAlign = 'right';

        for (let i = 0; i <= 5; i++) {
            const y = P.top + (plotH / 5) * i;
            const val = yMax - ((yMax - yMin) / 5) * i;
            ctx.fillText(val.toFixed(decimals), P.left - 8, y + 4);
        }
    },

    _drawLegend(ctx, w, P, items) {
        const startX = P.left;
        let x = startX;

        ctx.font = '11px Inter';

        for (const item of items) {
            ctx.fillStyle = item.color;
            ctx.fillRect(x, P.top - 14, 14, 3);
            x += 18;

            ctx.fillStyle = this.COLORS.text;
            ctx.textAlign = 'left';
            ctx.fillText(item.label, x, P.top - 8);
            x += ctx.measureText(item.label).width + 20;
        }
    },
};
