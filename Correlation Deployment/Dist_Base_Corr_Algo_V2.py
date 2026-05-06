"""
Absolute Flip — Live MT5 Trading Script V2
========================================
Distance-confirmed correlation flip detection, executed live via MetaTrader 5.

Algorithm mirrors absolute_flip.py from the tester exactly:
  - Cumulative % index (base 1000) for each asset, updated tick-by-tick
  - Spread = idx1 - idx2
  - State machine: zero-cross → PENDING → confirmed only if spread travels
    >= DISTANCE_N index points in new direction before re-crossing
  - On confirmed flip: close both positions, open both reversed

V2 Improvements:
  - Supports Start and Stop time bounds (Local machine time).
  - Supports 0 lot size for either asset (will only trade the one with lot > 0).

Position structure (hedge mode, always 2 open tickets if both lots > 0):
  LONG1  state → BUY  SYMBOL_1  +  SELL SYMBOL_2
  SHORT1 state → SELL SYMBOL_1  +  BUY  SYMBOL_2
"""

# ══════════════════════════════════════════════════════════════════════
#  SECTION 1 — USER CONFIG  (edit only this section)
# ══════════════════════════════════════════════════════════════════════

# ── MT5 Credentials ───────────────────────────────────────────────────
MT5_LOGIN    = 415559990
MT5_PASSWORD = "Soham@987"
MT5_SERVER   = "Exness-MT5Trial14"
MT5_PATH     = r"C:\Program Files\MetaTrader 5\terminal64.exe"

# ── Asset Configuration ───────────────────────────────────────────────
SYMBOL_1 = "XAUUSDm"   # Asset 1 — BUY on LONG1, SELL on SHORT1
SYMBOL_2 = "USDJPYm"   # Asset 2 — always opposite to Asset 1
LOT_1    = 0.01        # Lot size for SYMBOL_1 (0 to disable trading this asset)
LOT_2    = 0.00        # Lot size for SYMBOL_2 (0 to disable trading this asset)

# ── Time Configuration (Local Machine Time) ───────────────────────────
START_TIME = "23:08"   # e.g. "09:00". Empty string "" to run 24/7
STOP_TIME  = "23:15"   # e.g. "23:00". Empty string "" to run 24/7

# ── Flip Algorithm ────────────────────────────────────────────────────
DISTANCE_N = 1.0       # Minimum index points past zero to confirm a flip

# ── Execution Settings ────────────────────────────────────────────────
DEVIATION        = 20       # Max slippage in points
MAGIC            = 234567   # Magic number — tags all orders from this script
POLL_INTERVAL_MS = 100      # Tick poll interval in milliseconds (100 = 10 polls/sec)

# ── Logging ───────────────────────────────────────────────────────────
LOG_LEVEL = "INFO"                    # "DEBUG" for every tick detail
LOG_FILE  = "absolute_flip_live_v2.log"  # Set to "" to disable file logging

# ══════════════════════════════════════════════════════════════════════
#  SECTION 2 — IMPORTS
# ══════════════════════════════════════════════════════════════════════

import logging
import logging.handlers
import sys
import time
import signal
import glob
import pathlib
import datetime
from typing import Optional

try:
    import MetaTrader5 as mt5
except ImportError:
    print("ERROR: MetaTrader5 package not installed.")
    print("       Run: pip install MetaTrader5")
    sys.exit(1)

# ══════════════════════════════════════════════════════════════════════
#  SECTION 3 — LOGGING SETUP
# ══════════════════════════════════════════════════════════════════════

def _setup_logger() -> logging.Logger:
    logger = logging.getLogger("absflip")
    logger.setLevel(logging.getLevelName(LOG_LEVEL))

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")

    # Console handler — always on
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # File handler — optional
    if LOG_FILE:
        fh = logging.handlers.RotatingFileHandler(
            LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
        )
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    return logger

logger = _setup_logger()

# ══════════════════════════════════════════════════════════════════════
#  SECTION 4 — MT5 CONNECTION
# ══════════════════════════════════════════════════════════════════════

def _discover_terminal_paths() -> list:
    """Scan common install locations for terminal64.exe."""
    candidates = [
        r"C:\Program Files\MetaTrader 5\terminal64.exe",
        r"C:\Program Files (x86)\MetaTrader 5\terminal64.exe",
    ]
    candidates += glob.glob(r"C:\Program Files\*\terminal64.exe")
    candidates += glob.glob(r"C:\Program Files (x86)\*\terminal64.exe")
    return [c for c in candidates if pathlib.Path(c).exists()]


def connect_mt5() -> None:
    """
    Initialize MT5 terminal and authenticate.
    Strategy: try attach to running terminal first, then MT5_PATH, then discovered paths.
    Raises ConnectionError or ValueError on failure.
    """
    paths_to_try = [None]  # None = attach to already-running terminal
    if MT5_PATH:
        paths_to_try.append(MT5_PATH)
    paths_to_try.extend(_discover_terminal_paths())

    last_error = None

    for path in paths_to_try:
        label = repr(path) if path else "(attach to running terminal)"
        logger.info(f"MT5 initialize attempt: {label}")

        init_kwargs = {"path": path} if path else {}
        if not mt5.initialize(**init_kwargs):
            last_error = mt5.last_error()
            logger.warning(f"MT5 initialize failed ({label}): {last_error}")
            continue

        # initialize() succeeded — check if already logged into correct account
        current = mt5.account_info()
        if current and current.login == MT5_LOGIN:
            logger.info(f"MT5 already logged in | account={current.login} server={current.server}")
            _select_symbols()
            return

        # Attempt login
        if not mt5.login(login=MT5_LOGIN, password=MT5_PASSWORD, server=MT5_SERVER):
            last_error = mt5.last_error()
            logger.warning(f"MT5 login() failed: {last_error}")
            mt5.shutdown()
            break  # Wrong credentials — no point trying other paths

        ai = mt5.account_info()
        logger.info(f"MT5 logged in | account={ai.login} server={ai.server} name={ai.name} balance={ai.balance:.2f}")
        _select_symbols()
        return

    # All paths failed
    err_code = last_error[0] if last_error else -1
    err_msg  = last_error[1] if last_error else "unknown"

    if err_code == -6:
        raise ConnectionError(
            "MT5 terminal is not running.\n"
            "FIX: Open MetaTrader 5, log in once, keep it running, then retry."
        )
    elif err_code in (10013, 10014):
        raise ConnectionError(
            f"MT5 login rejected (error {err_code}: {err_msg}).\n"
            f"Check MT5_LOGIN={MT5_LOGIN}, MT5_PASSWORD, MT5_SERVER={MT5_SERVER!r}"
        )
    else:
        raise ConnectionError(f"MT5 connection failed (error {err_code}: {err_msg})")


def _select_symbols() -> None:
    """Ensure both symbols are in Market Watch so ticks are available."""
    for sym in (SYMBOL_1, SYMBOL_2):
        if not mt5.symbol_select(sym, True):
            raise ValueError(
                f"Symbol '{sym}' not found in MT5 terminal. "
                f"Check broker symbol name (e.g. 'XAUUSDm' vs 'XAUUSD')."
            )
        logger.info(f"Symbol selected: {sym}")


def disconnect_mt5() -> None:
    mt5.shutdown()
    logger.info("MT5 disconnected.")

# ══════════════════════════════════════════════════════════════════════
#  SECTION 5 — ORDER EXECUTION
# ══════════════════════════════════════════════════════════════════════

def open_position(symbol: str, direction: str, lot: float, comment: str) -> Optional[int]:
    """
    Open a market position. Returns ticket number on success, None on failure.
    Retries once with a fresh price if the first attempt fails.

    direction: "BUY" or "SELL"
    """
    order_type = mt5.ORDER_TYPE_BUY if direction == "BUY" else mt5.ORDER_TYPE_SELL

    for attempt in range(2):
        tick = mt5.symbol_info_tick(symbol)
        if tick is None:
            logger.error(f"open_position: cannot get tick for {symbol}")
            time.sleep(0.5)
            continue

        price = tick.ask if direction == "BUY" else tick.bid

        request = {
            "action":       mt5.TRADE_ACTION_DEAL,
            "symbol":       symbol,
            "volume":       float(lot),
            "type":         order_type,
            "price":        price,
            "deviation":    DEVIATION,
            "magic":        MAGIC,
            "comment":      comment[:31],
            "type_time":    mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC,
        }

        result = mt5.order_send(request)

        if result is not None and result.retcode == 10009:
            logger.info(f"  ✔ Opened {direction:4s} {lot} {symbol:<12s} ticket={result.order} price={price}")
            return result.order

        retcode = result.retcode if result else "None"
        comment_str = result.comment if result else ""
        logger.warning(f"  open_position attempt {attempt+1} FAILED | {symbol} {direction} "
                       f"retcode={retcode} comment={comment_str!r}")
        time.sleep(0.3)

    logger.error(f"open_position FAILED after 2 attempts | {symbol} {direction}")
    return None


def close_position(ticket: int, symbol: str, lot: float,
                   pos_type: str, comment: str) -> bool:
    """
    Close an open position by ticket.
    Returns True on success, False on failure.

    pos_type: "BUY" or "SELL" — direction of the position being closed.
    Sends the opposite order type with 'position' field linking to the ticket.
    """
    close_type = mt5.ORDER_TYPE_SELL if pos_type == "BUY" else mt5.ORDER_TYPE_BUY

    for attempt in range(2):
        tick = mt5.symbol_info_tick(symbol)
        if tick is None:
            logger.error(f"close_position: cannot get tick for {symbol}")
            time.sleep(0.5)
            continue

        # Closing a BUY → sell at bid; closing a SELL → buy at ask
        price = tick.bid if pos_type == "BUY" else tick.ask

        request = {
            "action":       mt5.TRADE_ACTION_DEAL,
            "symbol":       symbol,
            "volume":       float(lot),
            "type":         close_type,
            "position":     ticket,         # links order to open ticket — critical
            "price":        price,
            "deviation":    DEVIATION,
            "magic":        MAGIC,
            "comment":      comment[:31],
            "type_time":    mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC,
        }

        result = mt5.order_send(request)

        if result is not None and result.retcode == 10009:
            logger.info(f"  ✔ Closed ticket={ticket} {symbol:<12s} ({pos_type}) price={price}")
            return True

        retcode = result.retcode if result else "None"
        comment_str = result.comment if result else ""
        logger.warning(f"  close_position attempt {attempt+1} FAILED | ticket={ticket} {symbol} "
                       f"retcode={retcode} comment={comment_str!r}")
        time.sleep(0.3)

    logger.error(f"close_position FAILED after 2 attempts | ticket={ticket} {symbol}")
    return False

# ══════════════════════════════════════════════════════════════════════
#  SECTION 6 — ABSOLUTE FLIP STATE MACHINE
# ══════════════════════════════════════════════════════════════════════

class AbsoluteFlipStateMachine:
    """
    Incremental port of detect_distance_flips() from absolute_flip.py.
    State persists between process_tick() calls — no batch processing.

    Index computation mirrors build_tick_index():
      - Both assets start at 1000.0 on first tick pair
      - idx updates only when that symbol ticks; other is forward-filled
      - spread = idx1 - idx2
    """

    def __init__(self, sym1: str, sym2: str, distance_n: float):
        self.sym1       = sym1
        self.sym2       = sym2
        self.distance_n = distance_n

        # Position labels (match tester output exactly)
        self.POS_LONG1  = f"LONG {sym1} / SHORT {sym2}"
        self.POS_SHORT1 = f"SHORT {sym1} / LONG {sym2}"

        # Running index
        self.idx1: float = 1000.0
        self.idx2: float = 1000.0

        # Previous mid used to compute returns (set at init tick)
        self.mid1_prev: Optional[float] = None
        self.mid2_prev: Optional[float] = None

        # Most recent known mid for each symbol (forward-fill)
        self.mid1_last: Optional[float] = None
        self.mid2_last: Optional[float] = None

        # State machine variables (mirror detect_distance_flips exactly)
        self.cur_pos:         Optional[str] = None   # "LONG1" or "SHORT1"
        self.pending:         bool = False
        self.pending_to_long: bool = False
        self.initialized:     bool = False

    def process_tick(self, which: int, mid: float) -> Optional[str]:
        """
        Feed one new tick to the state machine.

        which: 1 = SYMBOL_1 ticked, 2 = SYMBOL_2 ticked
        mid:   (bid + ask) / 2 of the new tick

        Returns:
            "LONG1"  — flip to LONG sym1 / SHORT sym2 confirmed
            "SHORT1" — flip to SHORT sym1 / LONG sym2 confirmed
            None     — no flip this tick
        """
        # Update forward-fill tracking
        if which == 1:
            self.mid1_last = mid
        else:
            self.mid2_last = mid

        # ── Initialization: need at least one tick from each symbol ──
        if not self.initialized:
            if self.mid1_last is None or self.mid2_last is None:
                return None  # Still waiting for both symbols

            # Both symbols have a first tick — initialize index and state
            self.mid1_prev = self.mid1_last
            self.mid2_prev = self.mid2_last
            # idx1 and idx2 already 1000.0 from __init__
            spread = 0.0  # idx1 - idx2 at base = 0
            self.cur_pos = "LONG1" if spread >= 0 else "SHORT1"
            self.initialized = True

            logger.info(
                f"State machine initialized | "
                f"idx1=1000.0000 idx2=1000.0000 spread=0.0000 "
                f"initial_pos={self.cur_pos}"
            )
            return None  # No flip on init — caller opens initial position

        # ── Update running index for the ticking symbol ──
        if which == 1 and self.mid1_prev is not None and self.mid1_prev != 0:
            r = (mid - self.mid1_prev) / self.mid1_prev
            self.idx1 = self.idx1 * (1.0 + r)
            self.mid1_prev = mid

        elif which == 2 and self.mid2_prev is not None and self.mid2_prev != 0:
            r = (mid - self.mid2_prev) / self.mid2_prev
            self.idx2 = self.idx2 * (1.0 + r)
            self.mid2_prev = mid

        spread = self.idx1 - self.idx2

        logger.debug(
            f"tick=sym{which} mid={mid:.5f} "
            f"idx1={self.idx1:.4f} idx2={self.idx2:.4f} "
            f"spread={spread:.4f} pos={self.cur_pos} pending={self.pending}"
        )

        # ── State machine (exact port of detect_distance_flips loop body) ──
        if not self.pending:
            implied = "LONG1" if spread >= 0 else "SHORT1"
            if implied != self.cur_pos:
                self.pending         = True
                self.pending_to_long = (implied == "LONG1")
                logger.debug(
                    f"Zero-cross detected → pending_to_long={self.pending_to_long} "
                    f"spread={spread:.4f}"
                )

        else:
            if self.pending_to_long:
                if spread >= self.distance_n:
                    self.cur_pos = "LONG1"
                    self.pending = False
                    logger.info(
                        f"★ CONFIRMED FLIP → {self.POS_LONG1} | "
                        f"spread={spread:.4f} loss={abs(spread):.4f}"
                    )
                    return "LONG1"

                elif spread < 0:
                    # Re-crossed back before reaching +N → cancel
                    self.pending = False
                    logger.debug(f"Flip cancelled (re-cross before +N) spread={spread:.4f}")

            else:
                if spread <= -self.distance_n:
                    self.cur_pos = "SHORT1"
                    self.pending = False
                    logger.info(
                        f"★ CONFIRMED FLIP → {self.POS_SHORT1} | "
                        f"spread={spread:.4f} loss={abs(spread):.4f}"
                    )
                    return "SHORT1"

                elif spread >= 0:
                    # Re-crossed back before reaching -N → cancel
                    self.pending = False
                    logger.debug(f"Flip cancelled (re-cross before -N) spread={spread:.4f}")

        return None

# ══════════════════════════════════════════════════════════════════════
#  SECTION 7 — MAIN LOOP
# ══════════════════════════════════════════════════════════════════════

# Direction map: which direction to trade each symbol per position state
_DIRECTION_MAP = {
    "LONG1":  {"sym1": "BUY",  "sym2": "SELL"},
    "SHORT1": {"sym1": "SELL", "sym2": "BUY"},
}


def is_trading_time() -> bool:
    """Check if current local time is within START_TIME and STOP_TIME."""
    if not START_TIME or not STOP_TIME:
        return True
        
    now_str = datetime.datetime.now().strftime("%H:%M")
    
    if START_TIME < STOP_TIME:
        return START_TIME <= now_str < STOP_TIME
    else: # Overnight, e.g. "23:00" to "05:00"
        return now_str >= START_TIME or now_str < STOP_TIME


def run_live() -> None:
    """
    Main entry point. Connects to MT5, starts tick polling loop,
    runs absolute flip state machine, executes trades on confirmed flips.
    """

    # ── Verify Configuration ───────────────────────────────────────
    if LOT_1 <= 0 and LOT_2 <= 0:
        logger.error("Both LOT_1 and LOT_2 are 0. Nothing to trade. Exiting.")
        sys.exit(1)

    # ── Position state ─────────────────────────────────────────────
    pos1_ticket: Optional[int] = None
    pos1_type:   Optional[str] = None   # "BUY" or "SELL"
    pos2_ticket: Optional[int] = None
    pos2_type:   Optional[str] = None

    # If any order fails critically, halt and require manual restart
    halted = [False]   # list so inner functions can mutate

    # ── Inner: execute a flip (close both, open both reversed) ─────
    def execute_flip(new_pos: str) -> None:
        nonlocal pos1_ticket, pos1_type, pos2_ticket, pos2_type

        dirs = _DIRECTION_MAP[new_pos]
        logger.info(f"{'═'*60}")
        logger.info(f"FLIP → {new_pos} | {SYMBOL_1} {dirs['sym1']} / {SYMBOL_2} {dirs['sym2']}")
        logger.info(f"{'═'*60}")

        # ── CLOSE PHASE ──────────────────────────────────────────
        if pos1_ticket is not None and LOT_1 > 0:
            ok = close_position(pos1_ticket, SYMBOL_1, LOT_1, pos1_type, "AbsFlip close")
            if ok:
                pos1_ticket = None
                pos1_type   = None
            else:
                logger.critical(
                    "HALT: Failed to close SYMBOL_1 position. "
                    "Manual intervention required. Restart script after fixing."
                )
                halted[0] = True
                return

        if pos2_ticket is not None and LOT_2 > 0:
            ok = close_position(pos2_ticket, SYMBOL_2, LOT_2, pos2_type, "AbsFlip close")
            if ok:
                pos2_ticket = None
                pos2_type   = None
            else:
                logger.critical(
                    "HALT: Failed to close SYMBOL_2 position. "
                    "Manual intervention required. Restart script after fixing."
                )
                halted[0] = True
                return

        # ── OPEN PHASE ───────────────────────────────────────────
        if LOT_1 > 0:
            t1 = open_position(SYMBOL_1, dirs["sym1"], LOT_1, f"AbsFlip {new_pos}"[:31])
            if t1 is None:
                logger.critical(
                    "HALT: Failed to open SYMBOL_1 position after close. "
                    "Both positions are now flat. Restart script to re-enter."
                )
                halted[0] = True
                return
            pos1_ticket = t1
            pos1_type   = dirs["sym1"]

        if LOT_2 > 0:
            t2 = open_position(SYMBOL_2, dirs["sym2"], LOT_2, f"AbsFlip {new_pos}"[:31])
            if t2 is None:
                logger.critical(
                    f"HALT: Failed to open SYMBOL_2. "
                    f"SYMBOL_1 ticket={pos1_ticket} is open but SYMBOL_2 is not. "
                    f"Close SYMBOL_1 ticket={pos1_ticket} manually and restart."
                )
                halted[0] = True
                return
            pos2_ticket = t2
            pos2_type   = dirs["sym2"]

        # Log active positions based on lot sizes
        msg_parts = ["Positions active |"]
        if LOT_1 > 0:
            msg_parts.append(f"{SYMBOL_1} ticket={pos1_ticket} {pos1_type} |")
        if LOT_2 > 0:
            msg_parts.append(f"{SYMBOL_2} ticket={pos2_ticket} {pos2_type}")
        logger.info(" ".join(msg_parts))

    # ── Inner: graceful shutdown ───────────────────────────────────
    def graceful_shutdown(*_) -> None:
        nonlocal pos1_ticket, pos1_type, pos2_ticket, pos2_type
        logger.info("Shutting down — closing open positions...")

        if pos1_ticket is not None and LOT_1 > 0:
            close_position(pos1_ticket, SYMBOL_1, LOT_1, pos1_type, "Shutdown")
            pos1_ticket = None

        if pos2_ticket is not None and LOT_2 > 0:
            close_position(pos2_ticket, SYMBOL_2, LOT_2, pos2_type, "Shutdown")
            pos2_ticket = None

        disconnect_mt5()
        logger.info("Shutdown complete.")
        sys.exit(0)

    # Register signals
    signal.signal(signal.SIGINT,  graceful_shutdown)
    signal.signal(signal.SIGTERM, graceful_shutdown)

    # ── Connect ────────────────────────────────────────────────────
    connect_mt5()

    logger.info("=" * 60)
    logger.info("Absolute Flip Live V2 — Started")
    logger.info(f"  SYMBOL_1    : {SYMBOL_1}  (lot={LOT_1})")
    logger.info(f"  SYMBOL_2    : {SYMBOL_2}  (lot={LOT_2})")
    logger.info(f"  TIME FILTER : {START_TIME or 'None'} to {STOP_TIME or 'None'}")
    logger.info(f"  DISTANCE_N  : {DISTANCE_N}")
    logger.info(f"  POLL        : {POLL_INTERVAL_MS}ms")
    logger.info(f"  MAGIC       : {MAGIC}")
    logger.info("Press Ctrl+C to stop and close all positions.")
    logger.info("=" * 60)

    # ── State machine + tick tracking ─────────────────────────────
    sm           = AbsoluteFlipStateMachine(SYMBOL_1, SYMBOL_2, DISTANCE_N)
    last_msc_1   = 0
    last_msc_2   = 0
    initial_open = False     # True after the first position pair is opened
    poll_sleep   = POLL_INTERVAL_MS / 1000.0

    # ── Main polling loop ──────────────────────────────────────────
    while True:

        if halted[0]:
            time.sleep(1.0)
            continue

        try:
            # ── Check Time Filter ──
            if not is_trading_time():
                # Close open positions if any
                if pos1_ticket is not None or pos2_ticket is not None:
                    logger.info("Outside trading hours, closing open positions.")
                    if pos1_ticket is not None and LOT_1 > 0:
                        close_position(pos1_ticket, SYMBOL_1, LOT_1, pos1_type, "Time Stop")
                        pos1_ticket = None
                        pos1_type = None
                    if pos2_ticket is not None and LOT_2 > 0:
                        close_position(pos2_ticket, SYMBOL_2, LOT_2, pos2_type, "Time Stop")
                        pos2_ticket = None
                        pos2_type = None
                    initial_open = False # reset so we re-open when time is right
                
                # Keep processing ticks so the state machine index stays up to date
                # But don't execute flips!
                tick1 = mt5.symbol_info_tick(SYMBOL_1)
                tick2 = mt5.symbol_info_tick(SYMBOL_2)

                if tick1 is not None and tick1.time_msc > last_msc_1:
                    last_msc_1 = tick1.time_msc
                    sm.process_tick(1, (tick1.bid + tick1.ask) / 2.0)
                
                if tick2 is not None and tick2.time_msc > last_msc_2:
                    last_msc_2 = tick2.time_msc
                    sm.process_tick(2, (tick2.bid + tick2.ask) / 2.0)

                time.sleep(poll_sleep)
                continue

            # ── Normal Trading Logic ──
            tick1 = mt5.symbol_info_tick(SYMBOL_1)
            tick2 = mt5.symbol_info_tick(SYMBOL_2)

            # Process SYMBOL_1 tick if new
            if tick1 is not None and tick1.time_msc > last_msc_1:
                last_msc_1 = tick1.time_msc
                mid1 = (tick1.bid + tick1.ask) / 2.0
                flip = sm.process_tick(1, mid1)

                if flip and not halted[0]:
                    execute_flip(flip)
                    initial_open = True

            # Process SYMBOL_2 tick if new
            if tick2 is not None and tick2.time_msc > last_msc_2:
                last_msc_2 = tick2.time_msc
                mid2 = (tick2.bid + tick2.ask) / 2.0
                flip = sm.process_tick(2, mid2)

                if flip and not halted[0]:
                    execute_flip(flip)
                    initial_open = True

            # Open initial position once SM is initialized
            if sm.initialized and not initial_open and not halted[0]:
                logger.info(f"Opening initial position: {sm.cur_pos}")
                execute_flip(sm.cur_pos)
                initial_open = True

        except Exception as e:
            logger.error(f"Loop error: {e}", exc_info=True)
            time.sleep(5.0)
            continue

        time.sleep(poll_sleep)

# ══════════════════════════════════════════════════════════════════════
#  SECTION 8 — ENTRY POINT
# ══════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    run_live()
