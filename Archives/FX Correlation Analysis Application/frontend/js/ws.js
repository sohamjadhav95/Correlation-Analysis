/* ── WebSocket Client ─────────────────────────────────── */

class WSClient {
    constructor() {
        this._ws = null;
        this._handlers = {};
        this._reconnectTimer = null;
    }

    connect(jobId, onMessage, onClose) {
        const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
        const url = `${proto}//${location.host}/ws/progress?job_id=${jobId}`;

        this._ws = new WebSocket(url);
        this._ws.onopen = () => console.log(`WS connected for job: ${jobId}`);

        this._ws.onmessage = (evt) => {
            try {
                const data = JSON.parse(evt.data);
                if (onMessage) onMessage(data);
            } catch (e) {
                console.warn('WS parse error:', e);
            }
        };

        this._ws.onclose = () => {
            console.log('WS closed');
            if (onClose) onClose();
        };

        this._ws.onerror = (err) => {
            console.error('WS error:', err);
        };
    }

    disconnect() {
        if (this._ws) {
            this._ws.close();
            this._ws = null;
        }
    }

    get isConnected() {
        return this._ws && this._ws.readyState === WebSocket.OPEN;
    }
}

// Singleton
const WS = new WSClient();
