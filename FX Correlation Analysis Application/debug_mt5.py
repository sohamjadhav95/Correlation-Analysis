"""
MT5 Authorization Diagnostic — run this to pinpoint the auth failure.
"""

import os
from dotenv import load_dotenv

load_dotenv()

MT5_LOGIN   = int(os.getenv("MT5_LOGIN") or "0")
MT5_PASSWORD = os.getenv("MT5_PASSWORD", "")
MT5_SERVER   = os.getenv("MT5_SERVER", "")
MT5_PATH     = os.getenv("MT5_PATH", "")

print("=" * 60)
print("MT5 Diagnostic")
print("=" * 60)
print(f"  Login   : {MT5_LOGIN}")
print(f"  Server  : {MT5_SERVER}")
print(f"  Path    : {MT5_PATH!r}")
print(f"  Password: {'*' * len(MT5_PASSWORD)} (len={len(MT5_PASSWORD)})")
print()

# ── Check terminal file exists ──────────────────────────────
import pathlib
p = pathlib.Path(MT5_PATH)
print(f"Terminal path exists: {p.exists()}")
if not p.exists():
    # Try common install locations
    candidates = [
        r"C:\Program Files\MetaTrader 5\terminal64.exe",
        r"C:\Program Files (x86)\MetaTrader 5\terminal64.exe",
    ]
    # Also look for Exness-specific installs
    import glob
    candidates += glob.glob(r"C:\Users\*\AppData\Roaming\MetaQuotes\Terminal\*\terminal64.exe")
    candidates += glob.glob(r"C:\Program Files\*\terminal64.exe")
    candidates += glob.glob(r"C:\Program Files (x86)\*\terminal64.exe")
    
    print("\nSearching for terminal64.exe...")
    for c in candidates:
        if pathlib.Path(c).exists():
            print(f"  ✅ Found: {c}")
    print()

# ── Try MT5 import ──────────────────────────────────────────
try:
    import MetaTrader5 as mt5
    print(f"MetaTrader5 package version: {mt5.__version__}")
except ImportError as e:
    print(f"❌ MT5 import failed: {e}")
    exit(1)

# ── Step 1: initialize() without login ─────────────────────
print("\n[Step 1] mt5.initialize()...")
init_kwargs = {}
if MT5_PATH and p.exists():
    init_kwargs["path"] = MT5_PATH

ok = mt5.initialize(**init_kwargs)
if not ok:
    err = mt5.last_error()
    print(f"  ❌ initialize() FAILED: {err}")
    
    # Retry without path
    if init_kwargs:
        print("  Retrying without path...")
        ok = mt5.initialize()
        if not ok:
            print(f"  ❌ initialize() without path also FAILED: {mt5.last_error()}")
            exit(1)
        else:
            print("  ✅ initialize() succeeded WITHOUT path — update MT5_PATH in .env")
else:
    print(f"  ✅ initialize() OK")
    info = mt5.terminal_info()
    if info:
        print(f"     Terminal: {info.name}, build {info.build}, connected={info.connected}")

# ── Step 2: login() ────────────────────────────────────────
print(f"\n[Step 2] mt5.login({MT5_LOGIN}, server={MT5_SERVER!r})...")
ok = mt5.login(login=MT5_LOGIN, password=MT5_PASSWORD, server=MT5_SERVER)
if not ok:
    err = mt5.last_error()
    print(f"  ❌ login() FAILED: error code={err[0]}, message={err[1]!r}")
    
    # Common error codes
    if err[0] == 10013:
        print("  → Invalid account / password mismatch")
    elif err[0] == 10014:
        print("  → Invalid server name")
    elif err[0] in (10007, 10006):
        print("  → Network/connection error — is MT5 terminal running?")
    elif err[0] == -2:
        print("  → Terminal not ready / still loading")
else:
    acc = mt5.account_info()
    print(f"  ✅ login() OK!")
    print(f"     Account: {acc.login}, server={acc.server}, name={acc.name}")
    print(f"     Balance: {acc.balance} {acc.currency}")

mt5.shutdown()
print("\nDone.")
