/* ── Progress Indicator ───────────────────────────────── */

const Progress = {
    show(message = 'Loading...') {
        const section = document.getElementById('progress-section');
        section.classList.remove('hidden');
        document.getElementById('progress-text').textContent = message;
        document.getElementById('progress-bar').style.width = '0%';
    },

    update(pct, message) {
        document.getElementById('progress-bar').style.width = `${Math.min(pct, 100)}%`;
        if (message) document.getElementById('progress-text').textContent = message;
    },

    hide() {
        document.getElementById('progress-section').classList.add('hidden');
    },
};
