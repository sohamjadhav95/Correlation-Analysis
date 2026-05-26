"""
Pairs Trading Backtest Engine  —  backend.py
=============================================
Strategy:  Kalman Filter (dynamic hedge ratio)
         + Ornstein-Uhlenbeck (spread modeling + half-life)
         + HMM Regime Detection
         + Engle-Granger Cointegration Validation

Design:  Same strategy classes run in backtesting AND live deployment.
         No look-ahead bias: every trade enters at the NEXT candle's open.

Run:     python backend.py
         →  http://localhost:8000
"""

import io, json, warnings
from typing import Dict
import numpy  as np
import pandas as pd
from statsmodels.tsa.stattools          import adfuller, coint
from statsmodels.regression.linear_model import OLS
from statsmodels.tools                  import add_constant
from fastapi                            import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors            import CORSMiddleware
from fastapi.responses                  import HTMLResponse
from pydantic                           import BaseModel

warnings.filterwarnings('ignore')

# ═══════════════════════════════════════════════════════════════════════════
#   STRATEGY CORE  — identical code in live trading
# ═══════════════════════════════════════════════════════════════════════════

class KalmanFilter:
    """
    Bayesian adaptive hedge-ratio estimator.
    Updates beta (Y = beta*X + alpha) on every price tick.
    No look-back window needed — all history is encoded in the covariance matrix.
    """

    def __init__(self, delta: float = 1e-4, ve: float = 0.001):
        self.Vw    = delta / (1 - delta) * np.eye(2)   # process noise
        self.Ve    = ve                                  # observation noise
        self.beta  = np.zeros(2)                         # [hedge_ratio, intercept]
        self.P     = np.eye(2)                           # error covariance
        self.ready = False

    def update(self, px_x: float, px_y: float):
        """
        Feed one (x, y) price pair.
        Returns (spread, hedge_ratio).
        spread = y  -  beta[0]*x  -  beta[1]
        """
        F = np.array([px_x, 1.0])

        if not self.ready:
            self.beta  = np.array([px_y / max(px_x, 1e-10), 0.0])
            self.ready = True
            return float(px_y - F @ self.beta), float(self.beta[0])

        Pp    = self.P + self.Vw                        # predicted covariance
        innov = px_y - float(F @ self.beta)             # innovation = spread
        S     = float(F @ Pp @ F) + self.Ve             # innovation variance
        K     = (Pp @ F) / S                            # Kalman gain
        self.beta = self.beta + K * innov               # state update
        self.P    = (np.eye(2) - np.outer(K, F)) @ Pp  # covariance update
        return float(innov), float(self.beta[0])

    def reset(self):
        self.beta = np.zeros(2); self.P = np.eye(2); self.ready = False


class OUModel:
    """
    Fits an Ornstein-Uhlenbeck process to the spread.
    The key output is half_life: the number of candles the spread takes to
    cover half the distance back to its mean.
    This drives:  z-score rolling window  +  time-stop  +  pair suitability check.
    """

    def __init__(self):
        self.half_life = 20
        self.mu = 0.035; self.theta = 0.0; self.sigma = 1.0

    def fit(self, spread: np.ndarray) -> dict:
        s = np.asarray(spread, float)
        s = s[~np.isnan(s)]
        if len(s) < 30:
            return self._default()

        lag  = s[:-1]
        diff = np.diff(s)
        try:
            lam = float(OLS(diff, add_constant(lag)).fit().params[1])
        except Exception:
            lam = -0.035

        lam = min(-1e-6, lam)                          # must be negative
        hl  = int(np.clip(-np.log(2) / lam, 2, 300))

        b     = max(0.01, min(0.999, 1 + lam))
        mu    = -np.log(b)
        theta = float(np.mean(s))
        resid = diff - (lam * lag + (np.mean(diff) - lam * np.mean(lag)))
        sigma = float(np.std(resid))

        self.half_life = hl
        self.mu = round(mu, 6); self.theta = round(theta, 6); self.sigma = round(sigma, 6)
        return {'half_life': hl, 'mu': self.mu, 'theta': self.theta, 'sigma': self.sigma}

    def _default(self):
        return {'half_life': 20, 'mu': 0.035, 'theta': 0.0, 'sigma': 1.0}


class RegimeDetector:
    """
    Two-state HMM on spread innovations classifies market into:
       State 0 — mean-reverting  →  TRADE
       State 1 — trending        →  SKIP
    Falls back to ADX-only gating if hmmlearn is unavailable.
    """

    def __init__(self):
        self._model   = None
        self._trend_s = 1
        try:
            from hmmlearn import hmm as _hmm
            self._hmm = _hmm
        except ImportError:
            self._hmm = None

    def fit(self, spread: np.ndarray):
        if self._hmm is None or len(spread) < 100:
            return
        diff = np.diff(spread)
        vol  = pd.Series(diff).rolling(10).std().bfill().values
        X    = np.column_stack([diff, vol])
        try:
            m = self._hmm.GaussianHMM(2, 'full', n_iter=100, random_state=42)
            m.fit(X)
            states        = m.predict(X)
            state_vols    = [np.std(diff[states == s]) for s in range(2)]
            self._model   = m
            self._trend_s = int(np.argmax(state_vols))  # higher-vol state = trending
        except Exception:
            self._model = None

    def predict(self, recent: np.ndarray) -> int:
        """0 = mean-reverting (OK), 1 = trending (skip)."""
        if self._model is None or len(recent) < 12:
            return 0
        diff = np.diff(recent[-21:])
        vol  = pd.Series(diff).rolling(5).std().bfill().values
        X    = np.column_stack([diff, vol])
        try:
            return int(self._model.predict(X)[-1] == self._trend_s)
        except Exception:
            return 0


def _compute_adx(high, low, close, p=14):
    h, l, c = map(np.asarray, (high, low, close))
    n = len(c)
    tr = pdm = ndm = np.zeros(n)
    for i in range(1, n):
        tr[i]  = max(h[i]-l[i], abs(h[i]-c[i-1]), abs(l[i]-c[i-1]))
        up, dn = h[i]-h[i-1], l[i-1]-l[i]
        pdm[i] = up if up > dn and up > 0 else 0
        ndm[i] = dn if dn > up and dn > 0 else 0

    def sm(x):
        r = np.zeros(n); r[p] = x[1:p+1].sum()
        for i in range(p+1, n): r[i] = r[i-1] - r[i-1]/p + x[i]
        return r

    atr = sm(tr)
    pdi = 100 * sm(pdm) / np.maximum(atr, 1e-9)
    ndi = 100 * sm(ndm) / np.maximum(atr, 1e-9)
    dx  = 100 * np.abs(pdi-ndi) / np.maximum(pdi+ndi, 1e-9)
    adx = np.zeros(n)
    adx[2*p] = dx[p:2*p+1].mean()
    for i in range(2*p+1, n):
        adx[i] = (adx[i-1]*(p-1) + dx[i]) / p
    return adx


class CointegrationTest:
    @staticmethod
    def run(a, b) -> dict:
        a, b = np.asarray(a, float), np.asarray(b, float)
        adf_a = adfuller(a, autolag='AIC')
        adf_b = adfuller(b, autolag='AIC')
        try:
            _, pv, _ = coint(a, b)
        except Exception:
            pv = 1.0
        corr = float(np.corrcoef(a, b)[0, 1])
        try:
            beta_ols = float(OLS(b, add_constant(a)).fit().params[1])
        except Exception:
            beta_ols = 1.0
        ok = bool(pv < 0.05)
        return dict(
            is_cointegrated = ok,
            eg_pvalue       = round(float(pv), 6),
            adf_a_pvalue    = round(float(adf_a[1]), 6),
            adf_b_pvalue    = round(float(adf_b[1]), 6),
            correlation     = round(corr, 4),
            static_beta     = round(beta_ols, 4),
            verdict         = "Tradeable ✓" if ok else "Not cointegrated ✗",
        )


# ═══════════════════════════════════════════════════════════════════════════
#   CONFIGURATION MODEL
# ═══════════════════════════════════════════════════════════════════════════

class BacktestConfig(BaseModel):
    timeframe:          str   = '1H'       # resample target
    entry_z:            float = 2.0        # z-score threshold to enter
    exit_z:             float = 0.3        # z-score threshold to exit (take profit)
    stop_z:             float = 3.5        # z-score stop loss
    kalman_delta:       float = 1e-4       # Kalman process noise sensitivity
    formation_pct:      float = 0.30       # fraction of data used for formation
    risk_pct:           float = 0.01       # account fraction risked per trade
    initial_capital:    float = 10_000.0
    fee_pct:            float = 0.001      # fee per leg (0.1%)
    slippage_pct:       float = 0.0005     # slippage per leg (0.05%)
    min_half_life:      int   = 2          # minimum half-life to trade (candles)
    max_half_life:      int   = 100        # maximum half-life to trade (candles)
    adx_threshold:      float = 30.0       # ADX above this = skip
    recalc_ou_every:    int   = 100        # recalculate OU params every N candles
    recalc_coint_every: int   = 500        # retest cointegration every N candles
    use_hmm:            bool  = True       # enable HMM regime detection
    corr_min:           float = 0.60       # min rolling correlation to trade
    max_chart_pts:      int   = 3000       # downsample chart data to this many points


# ═══════════════════════════════════════════════════════════════════════════
#   BACKTEST ENGINE
# ═══════════════════════════════════════════════════════════════════════════

class BacktestEngine:

    def __init__(self, cfg: BacktestConfig):
        self.cfg    = cfg
        self.kf     = KalmanFilter(delta=cfg.kalman_delta)
        self.ou     = OUModel()
        self.regime = RegimeDetector()
        self.ct     = CointegrationTest()

    # ── helpers ──────────────────────────────────────────────────────────

    def _align(self, a: pd.DataFrame, b: pd.DataFrame) -> pd.DataFrame:
        return a.join(b, how='inner', lsuffix='_a', rsuffix='_b').dropna().sort_index()

    def _close_trade(self, pos: dict, exit_a: float, exit_b: float,
                     ts, reason: str, equity: float) -> dict:
        cfg     = self.cfg
        ea, eb  = pos['entry_a'], pos['entry_b']
        da, db  = pos['dollar_a'], pos['dollar_b']
        ua, ub  = da / max(ea, 1e-10), db / max(eb, 1e-10)

        if pos['direction'] == 'LONG':   # long A, short B
            pnl_a = ua * (exit_a - ea)
            pnl_b = ub * (eb     - exit_b)
        else:                            # short A, long B
            pnl_a = ua * (ea     - exit_a)
            pnl_b = ub * (exit_b - eb)

        gross     = pnl_a + pnl_b
        exit_cost = (da + db) * (cfg.fee_pct + cfg.slippage_pct)
        net       = gross - exit_cost

        return dict(
            direction    = pos['direction'],
            entry_time   = str(pos['entry_ts']),
            exit_time    = str(ts),
            duration     = pos['duration'],
            entry_z      = pos['entry_z'],
            half_life    = pos['half_life'],
            beta         = round(pos['beta'], 6),
            entry_a      = round(ea, 6),  exit_a = round(exit_a, 6),
            entry_b      = round(eb, 6),  exit_b = round(exit_b, 6),
            dollar_a     = round(da, 2),  dollar_b = round(db, 2),
            pnl_a        = round(pnl_a, 4),
            pnl_b        = round(pnl_b, 4),
            gross_pnl    = round(gross, 4),
            fees         = round(pos['entry_cost'] + exit_cost, 4),
            net_pnl      = round(net, 4),
            return_pct   = round(net / max(pos['equity_entry'], 1) * 100, 4),
            exit_reason  = reason,
            equity_after = round(equity + net, 2),
            win          = bool(net > 0),
        )

    @staticmethod
    def _ds(lst, n: int):
        """Downsample list to at most n evenly-spaced points."""
        if len(lst) <= n: return lst
        step = max(1, len(lst) // n)
        return lst[::step]

    # ── main loop ────────────────────────────────────────────────────────

    def run(self, df_a: pd.DataFrame, df_b: pd.DataFrame) -> dict:
        cfg = self.cfg
        df  = self._align(df_a, df_b)
        n   = len(df)

        if n < 200:
            raise ValueError(f"Only {n} aligned candles after resampling — need ≥ 200. "
                             f"Try a shorter timeframe or ensure both CSVs cover the same period.")

        form_n = max(100, int(n * cfg.formation_pct))

        # ── Formation period: fit all models ────────────────────────────
        self.kf.reset()
        form_spreads = []
        for i in range(form_n):
            sp, _ = self.kf.update(float(df['close_a'].iloc[i]),
                                   float(df['close_b'].iloc[i]))
            form_spreads.append(sp)

        coint_res = self.ct.run(df['close_a'].iloc[:form_n].values,
                                df['close_b'].iloc[:form_n].values)
        ou_res    = self.ou.fit(np.array(form_spreads))
        half_life = ou_res['half_life']

        if cfg.use_hmm:
            self.regime.fit(np.array(form_spreads))

        # ── Full-pass Kalman (continues from formation — no reset) ───────
        # Re-run from scratch so the hedge ratio evolves continuously
        self.kf.reset()
        spreads = np.empty(n, float)
        betas   = np.empty(n, float)
        for i in range(n):
            sp, bt     = self.kf.update(float(df['close_a'].iloc[i]),
                                        float(df['close_b'].iloc[i]))
            spreads[i] = sp
            betas[i]   = bt

        # Pre-compute ADX and rolling correlation
        adx_arr   = _compute_adx(df['high_a'].values, df['low_a'].values, df['close_a'].values)
        roll_corr = (pd.Series(df['close_a'].values)
                       .rolling(30).corr(pd.Series(df['close_b'].values)).values)

        # ── Trading loop ─────────────────────────────────────────────────
        equity   = cfg.initial_capital
        position = None
        trades   = []
        eq_curve = []   # {ts, equity, drawdown}
        sp_log   = []   # {ts, spread, z, beta, regime}

        for i in range(form_n, n):
            ts   = df.index[i]
            ca   = float(df['close_a'].iloc[i])
            cb   = float(df['close_b'].iloc[i])
            nxt  = min(i + 1, n - 1)
            oa   = float(df['open_a'].iloc[nxt])   # entry/exit at NEXT open
            ob   = float(df['open_b'].iloc[nxt])
            sp   = float(spreads[i])
            beta = float(betas[i])
            rel  = i - form_n

            # Periodically refresh OU parameters
            if rel > 0 and rel % cfg.recalc_ou_every == 0:
                win = spreads[max(0, i - 300):i]
                if len(win) >= 30:
                    res       = self.ou.fit(win)
                    half_life = res['half_life']

            # Periodically retest cointegration
            if rel > 0 and rel % cfg.recalc_coint_every == 0:
                wa = df['close_a'].iloc[max(0, i-300):i].values
                wb = df['close_b'].iloc[max(0, i-300):i].values
                cr = self.ct.run(wa, wb)
                if not cr['is_cointegrated'] and position is not None:
                    t      = self._close_trade(position, ca, cb, ts, 'coint_break', equity)
                    equity = t['equity_after']
                    trades.append(t)
                    position = None

            # Z-score rolling window.
            # Floor at 20 candles for statistical validity: fewer than ~20 points
            # makes the z-score numerically bounded below 1.5 by geometry alone.
            z_window = max(20, min(int(half_life), cfg.max_half_life))
            if i >= z_window:
                sl = spreads[i - z_window: i + 1]
                mu_s, sd_s = np.mean(sl), np.std(sl)
                z = float((sp - mu_s) / sd_s) if sd_s > 1e-10 else 0.0
            else:
                z = 0.0

            # Regime detection
            regime = 0
            if cfg.use_hmm and rel > 20:
                regime = self.regime.predict(spreads[max(0, i-50): i+1])
            if adx_arr[i] > cfg.adx_threshold:
                regime = 1
            if not np.isnan(roll_corr[i]) and roll_corr[i] < cfg.corr_min:
                regime = 1

            sp_log.append({'ts': str(ts), 'spread': round(sp, 6),
                           'z': round(z, 4), 'beta': round(beta, 6),
                           'regime': int(regime)})

            # ── Manage open position ──────────────────────────────────
            if position is not None:
                position['duration'] += 1
                max_hold = position['half_life'] * 2
                reason   = None
                if   abs(z)                      <= cfg.exit_z:  reason = 'target'
                elif abs(z)                      >= cfg.stop_z:  reason = 'stop_loss'
                elif position['duration']        >= max_hold:    reason = 'time_stop'
                elif regime == 1 and position['duration'] > 3:   reason = 'regime_change'

                if reason:
                    t      = self._close_trade(position, oa, ob, ts, reason, equity)
                    equity = t['equity_after']
                    trades.append(t)
                    position = None

            # ── Entry signal ──────────────────────────────────────────
            if position is None:
                tradeable = (abs(z) > cfg.entry_z
                             and regime == 0
                             and cfg.min_half_life <= int(half_life) <= cfg.max_half_life)
                if tradeable:
                    direction  = 'LONG' if z < 0 else 'SHORT'
                    dr         = cfg.risk_pct * equity
                    da, db     = abs(beta) * dr, dr
                    entry_cost = (da + db) * (cfg.fee_pct + cfg.slippage_pct)
                    equity    -= entry_cost
                    position   = dict(
                        direction   = direction,
                        entry_ts    = ts,  entry_idx = i,
                        entry_a     = oa,  entry_b   = ob,
                        entry_z     = round(z, 4),
                        beta        = beta,
                        half_life   = int(half_life),
                        dollar_a    = da,   dollar_b  = db,
                        entry_cost  = entry_cost,
                        equity_entry= equity,
                        duration    = 0,
                    )

            eq_curve.append({'ts': str(ts), 'equity': round(equity, 2), 'drawdown': 0.0})

        # Close any position still open at end of data
        if position is not None:
            t      = self._close_trade(position,
                                       float(df['close_a'].iloc[-1]),
                                       float(df['close_b'].iloc[-1]),
                                       df.index[-1], 'end_of_data', equity)
            equity = t['equity_after']
            trades.append(t)

        # Compute drawdown column
        peak = cfg.initial_capital
        for e in eq_curve:
            peak = max(peak, e['equity'])
            e['drawdown'] = round((e['equity'] - peak) / peak * 100, 4)

        metrics = self._calc_metrics(trades, eq_curve, cfg.initial_capital, n - form_n)

        return dict(
            config        = cfg.dict(),
            cointegration = coint_res,
            ou_params     = ou_res,
            formation_n   = form_n,
            total_candles = n,
            metrics       = metrics,
            equity_curve  = self._ds(eq_curve, cfg.max_chart_pts),
            spread_data   = self._ds(sp_log,   cfg.max_chart_pts),
            trades        = trades,
        )

    # ── metrics ──────────────────────────────────────────────────────────

    def _calc_metrics(self, trades, eq_curve, ic, n_trading):
        if not trades:
            return {'error': 'No trades generated. Try lowering entry_z or checking data alignment.'}

        ev  = [e['equity'] for e in eq_curve]
        ret = np.diff(ev) / np.maximum(np.array(ev[:-1]), 1e-10)

        fe  = ev[-1]
        TR  = (fe - ic) / ic * 100

        # Conservative annualisation — user can interpret based on timeframe
        # Using hourly candles as reference: 8760 periods/year
        ppy = 8760 / max(1, n_trading / max(len(ev), 1))
        AR  = ((fe / ic) ** (ppy / max(len(ev), 1)) - 1) * 100 if fe > 0 else -100

        std_r = np.std(ret)
        SR    = float(np.mean(ret) / std_r * np.sqrt(8760)) if std_r > 0 else 0.0
        neg   = ret[ret < 0]
        SO    = float(np.mean(ret) / np.std(neg) * np.sqrt(8760)) if len(neg) > 0 and np.std(neg) > 0 else 0.0

        dd  = [e['drawdown'] for e in eq_curve]
        mdd = float(min(dd)) if dd else 0.0

        W  = [t for t in trades if     t['win']]
        L  = [t for t in trades if not t['win']]
        wr = len(W) / len(trades) * 100
        aw = float(np.mean([t['net_pnl'] for t in W])) if W else 0.0
        al = float(np.mean([t['net_pnl'] for t in L])) if L else 0.0
        gp = sum(t['net_pnl'] for t in W)
        gl = abs(sum(t['net_pnl'] for t in L)) or 1e-10
        pf = gp / gl

        er = {}
        for t in trades:
            er[t['exit_reason']] = er.get(t['exit_reason'], 0) + 1

        avg_dur = float(np.mean([t['duration'] for t in trades])) if trades else 0

        return dict(
            total_trades    = len(trades),
            win_trades      = len(W),
            loss_trades     = len(L),
            win_rate        = round(wr, 2),
            total_return    = round(TR, 2),
            ann_return      = round(AR, 2),
            sharpe          = round(SR, 3),
            sortino         = round(SO, 3),
            max_drawdown    = round(mdd, 2),
            profit_factor   = round(pf, 3),
            avg_win         = round(aw, 4),
            avg_loss        = round(al, 4),
            best_trade      = round(max(t['net_pnl'] for t in trades), 4),
            worst_trade     = round(min(t['net_pnl'] for t in trades), 4),
            final_equity    = round(fe, 2),
            initial_capital = ic,
            total_fees      = round(sum(t['fees'] for t in trades), 4),
            avg_duration    = round(avg_dur, 1),
            exit_reasons    = er,
        )


# ═══════════════════════════════════════════════════════════════════════════
#   DATA UTILITIES
# ═══════════════════════════════════════════════════════════════════════════

def parse_csv(raw: bytes, label: str = '?') -> pd.DataFrame:
    """
    Flexible OHLCV CSV parser.
    Auto-detects and handles:
      • Comma-separated  (Binance, Bybit, Kraken, Coinbase, Yahoo Finance, custom)
      • Tab-separated    (MT4 / MT5 — columns wrapped in <ANGLE_BRACKETS>)
      • Separate DATE + TIME columns  (MT4/MT5 format: '2026.01.27' + '20:36:00')
      • Combined datetime column
    Volume column is always optional.
    """
    # ── detect separator ────────────────────────────────────────────────
    sample = raw[:2048].decode('utf-8', errors='replace')
    sep = '\t' if sample.count('\t') > sample.count(',') else ','

    df = pd.read_csv(io.BytesIO(raw), sep=sep)

    # ── strip <ANGLE_BRACKETS> from MT4/MT5 column names ────────────────
    df.columns = (df.columns.str.strip()
                             .str.replace(r'[<>]', '', regex=True)
                             .str.lower())

    # ── rename OHLCV columns (fuzzy match) ──────────────────────────────
    m = {}
    for c in df.columns:
        cl = c.lower()
        if   'open'  in cl and 'open'   not in m.values(): m[c] = 'open'
        elif 'high'  in cl and 'high'   not in m.values(): m[c] = 'high'
        elif 'low'   in cl and 'low'    not in m.values(): m[c] = 'low'
        elif 'close' in cl and 'close'  not in m.values(): m[c] = 'close'
        elif 'vol'   in cl and 'volume' not in m.values(): m[c] = 'volume'
    df = df.rename(columns=m)

    # ── build timestamp ──────────────────────────────────────────────────
    # Case 1: MT4/MT5 — separate 'date' and 'time' columns
    if 'date' in df.columns and 'time' in df.columns:
        # Date may use dots: '2026.01.27' — normalise to dashes
        date_str = df['date'].astype(str).str.replace('.', '-', regex=False)
        time_str = df['time'].astype(str)
        combined = date_str + ' ' + time_str
        ts = pd.to_datetime(combined, format='mixed', errors='coerce')

    # Case 2: single combined timestamp column
    else:
        ts_candidates = ('timestamp', 'datetime', 'open_time', 'close_time', 't', 'dt', 'date', 'time')
        ts_col = next((c for c in ts_candidates if c in df.columns), df.columns[0])
        raw_ts = df[ts_col].astype(str).str.replace('.', '-', regex=False)
        ts = pd.to_datetime(raw_ts, format='mixed', errors='coerce')

    df.index = ts
    df.index.name = 'timestamp'
    df.index = ts
    df.index.name = 'timestamp'
    df = df[df.index.notna()].sort_index()

    # ── validate OHLC present ────────────────────────────────────────────
    for col in ('open', 'high', 'low', 'close'):
        if col not in df.columns:
            raise ValueError(
                f"Column '{col}' not found in {label}.\n"
                f"Detected columns: {list(df.columns)}\n"
                f"Supported formats: Binance/Bybit/Kraken CSV, MT4/MT5 tab-export, Yahoo Finance.")

    if 'volume' not in df.columns:
        df['volume'] = 0.0

    df[['open','high','low','close','volume']] = (
        df[['open','high','low','close','volume']]
          .apply(pd.to_numeric, errors='coerce'))

    return df[['open','high','low','close','volume']].dropna(
        subset=['open','high','low','close'])


TF_MAP = {
    '1min':'1min', '1T':'1min', '5min':'5min', '5T':'5min',
    '15min':'15min','15T':'15min','30min':'30min','30T':'30min',
    '1H':'1h','1h':'1h','4H':'4h','4h':'4h',   # pandas 2.x: uppercase H removed
    '1D':'1D','D':'1D','1d':'1D',
}

def resample_ohlcv(df: pd.DataFrame, tf: str) -> pd.DataFrame:
    rule = TF_MAP.get(tf, tf)
    return df.resample(rule).agg(
        open   = ('open',  'first'),
        high   = ('high',  'max'),
        low    = ('low',   'min'),
        close  = ('close', 'last'),
        volume = ('volume','sum'),
    ).dropna(subset=['open','close'])


# ═══════════════════════════════════════════════════════════════════════════
#   FASTAPI APPLICATION
# ═══════════════════════════════════════════════════════════════════════════

app = FastAPI(title="Pairs Trading Backtest Engine")
app.add_middleware(CORSMiddleware, allow_origins=['*'],
                   allow_methods=['*'], allow_headers=['*'])

_store: Dict[str, bytes] = {}   # simple in-memory store for uploaded files


@app.post('/api/upload')
async def upload(asset_a: UploadFile = File(...), asset_b: UploadFile = File(...)):
    """Validate and store both asset CSVs. Returns data preview."""
    ca, cb = await asset_a.read(), await asset_b.read()
    try:
        da = parse_csv(ca, asset_a.filename)
        db = parse_csv(cb, asset_b.filename)
    except ValueError as e:
        raise HTTPException(400, str(e))

    _store.update({'a': ca, 'b': cb,
                   'na': asset_a.filename, 'nb': asset_b.filename})

    ov_s = max(da.index[0], db.index[0])
    ov_e = min(da.index[-1], db.index[-1])
    days = max(0, (ov_e - ov_s).days)

    return {
        'asset_a': {'name': asset_a.filename, 'rows': len(da),
                    'start': str(da.index[0]), 'end': str(da.index[-1])},
        'asset_b': {'name': asset_b.filename, 'rows': len(db),
                    'start': str(db.index[0]), 'end': str(db.index[-1])},
        'overlap': {'start': str(ov_s), 'end': str(ov_e), 'days': days},
        'min_rows_1H': max(len(da), len(db)) // 60,
    }


@app.post('/api/backtest')
async def backtest(cfg: BacktestConfig):
    """Run full backtest. Requires prior /api/upload call."""
    if 'a' not in _store:
        raise HTTPException(400, 'Upload both CSVs first via /api/upload')
    try:
        da  = resample_ohlcv(parse_csv(_store['a'], _store.get('na','A')), cfg.timeframe)
        db  = resample_ohlcv(parse_csv(_store['b'], _store.get('nb','B')), cfg.timeframe)
        eng = BacktestEngine(cfg)
        res = eng.run(da, db)
        res['name_a'] = _store.get('na', 'Asset A')
        res['name_b'] = _store.get('nb', 'Asset B')
        return res
    except Exception as e:
        import traceback
        raise HTTPException(500, f"{e}\n\n{traceback.format_exc()}")


@app.get('/')
async def root():
    try:
        return HTMLResponse(open('dashboard.html', encoding='utf-8').read())
    except FileNotFoundError:
        return HTMLResponse('<h2>dashboard.html not found — place it in the same directory as backend.py</h2>', 404)


if __name__ == '__main__':
    import uvicorn
    print("\n🚀  Pairs Backtest Engine starting at  http://localhost:8000\n")
    uvicorn.run(app, host='0.0.0.0', port=8000, reload=False)
