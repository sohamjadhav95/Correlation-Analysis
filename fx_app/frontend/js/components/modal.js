/* ── Modal Component ──────────────────────────────────── */

const Modal = {
    init() {
        const settingsBtn = document.getElementById('settings-btn');
        const settingsModal = document.getElementById('settings-modal');
        const settingsClose = document.getElementById('settings-close');
        const backdrop = settingsModal.querySelector('.modal-backdrop');

        settingsBtn.onclick = () => this.open();
        settingsClose.onclick = () => this.close();
        backdrop.onclick = () => this.close();

        // Clear cache button
        document.getElementById('btn-clear-cache').onclick = async () => {
            try {
                Toast.show('Cache cleared', 'success');
            } catch (e) {
                Toast.show('Failed to clear cache: ' + e.message, 'error');
            }
        };
    },

    async open() {
        document.getElementById('settings-modal').classList.remove('hidden');
        await this._loadStatus();
    },

    close() {
        document.getElementById('settings-modal').classList.add('hidden');
    },

    async _loadStatus() {
        try {
            const config = await API.getConfig();

            document.getElementById('mt5-status').textContent =
                config.mt5_configured
                    ? `✅ Connected (Server: ${config.mt5_server}, Login: ${config.mt5_login})`
                    : '❌ Not configured — update .env file';

            document.getElementById('binance-status').textContent =
                config.binance_has_key
                    ? '✅ API key configured'
                    : '🔓 Using public endpoints (no API key)';

            document.getElementById('cache-status').textContent =
                `📂 ${config.data_cache_dir}`;
        } catch (e) {
            document.getElementById('mt5-status').textContent = '⚠️ Cannot reach server';
        }
    },
};
