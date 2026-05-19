import os
import threading

from dotenv import load_dotenv

# ── Runtime state (shared across modules) ────────────────────────────────────
stop_flag         = threading.Event()
pending_changes   = None
pending_lock      = threading.Lock()
TEST_MODE_ENABLED = False
TEST_MODE_PRICE   = None
_LOCK_HELD        = False
last_command_time = 0

# ── Grid configuration (can be updated by agent) ─────────────────────────────
GRID_LOWER           = 1.25
GRID_UPPER           = 1.45
GRID_LEVELS          = 20
ORDER_SIZE           = 5.0
MAX_CATCHUP_ZONES    = 3
AGENT_INTERVAL_HOURS = 3
AGENT_APPROVAL_MINS  = 10
AGENT_MIN_TRADES     = 3

# ── Safety limits ─────────────────────────────────────────────────────────────
MAX_OPEN_ZONES    = 10
MAX_TOTAL_TRADES  = 10000
MAX_LOSS_PCT      = 20.0
MIN_PROFIT_RATIO  = 0.75

# ── Volatility & trend ────────────────────────────────────────────────────────
VOLATILITY_LOOKBACK   = 14
VOLATILITY_ADJUSTMENT = True
VOL_THRESHOLD_LOW     = 0.01
VOL_THRESHOLD_HIGH    = 0.05
TREND_LOOKBACK        = 20
TREND_THRESHOLD       = 0.015
TREND_BIAS_ENABLED    = True

# ── Grid rebalancing ──────────────────────────────────────────────────────────
GRID_REBALANCE_THRESHOLD = 0.15
AUTO_REBALANCE_ENABLED   = True

# ── Fee settings (Phemex futures) ─────────────────────────────────────────────
MAKER_FEE_PCT        = 0.0001   # 0.01% for limit orders
TAKER_FEE_PCT        = 0.0006   # 0.06% for market orders
MIN_PROFIT_AFTER_FEES = 0.0005  # 0.05% minimum profit after fees

# ── Position health ───────────────────────────────────────────────────────────
UNDERWATER_ALERT_THRESHOLD = -5.0
HEALTH_CHECK_INTERVAL      = 3600
RECONCILE_INTERVAL_SECONDS = int(os.getenv("RECONCILE_INTERVAL_SECONDS", "300"))

# ── Credentials ───────────────────────────────────────────────────────────────
load_dotenv(os.path.expanduser("~/.env"))

TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
GROQ_API_KEY     = os.getenv("GROQ_API_KEY", "")

EXECUTE_LIVE = os.getenv("EXECUTE_LIVE", "false").lower() in ("1", "true", "yes")
EXCHANGE_ID = os.getenv("EXCHANGE_ID", "binance").strip().lower()
EXCHANGE_TESTNET = os.getenv("EXCHANGE_TESTNET", "false").lower() in (
    "1", "true", "yes"
)
EXCHANGE_MARKET_TYPE = os.getenv("EXCHANGE_MARKET_TYPE", "spot").strip().lower()
ALLOW_MAINNET_LIVE = os.getenv("ALLOW_MAINNET_LIVE", "false").lower() in (
    "1", "true", "yes"
)
LIVE_ACCOUNT_ISOLATED = os.getenv("LIVE_ACCOUNT_ISOLATED", "false").lower() in (
    "1", "true", "yes"
)
ALLOW_SHORTS = os.getenv("ALLOW_SHORTS", "false").lower() in (
    "1", "true", "yes"
)
MAX_SHORT_ZONES = int(os.getenv("MAX_SHORT_ZONES", "3"))
MAX_SHORT_NOTIONAL = float(os.getenv("MAX_SHORT_NOTIONAL", "50"))
MIN_MARGIN_RATIO = float(os.getenv("MIN_MARGIN_RATIO", "1.5"))
SHORT_MODE = os.getenv("SHORT_MODE", "futures").strip().lower()
ALLOW_MAINNET_SHORTS = os.getenv("ALLOW_MAINNET_SHORTS", "false").lower() in (
    "1", "true", "yes"
)


def _read_exchange_credential(kind):
    """Resolve API credentials with exchange-specific and testnet-aware priority."""
    ex = EXCHANGE_ID.upper()
    if kind == "key":
        generic_name = "API_KEY"
        direct_name = f"{ex}_API_KEY"
        testnet_name = f"{ex}_TESTNET_API_KEY"
    else:
        generic_name = "API_SECRET"
        direct_name = f"{ex}_API_SECRET"
        testnet_name = f"{ex}_TESTNET_API_SECRET"

    if EXCHANGE_TESTNET and os.getenv(testnet_name):
        return os.getenv(testnet_name, "")
    if os.getenv(direct_name):
        return os.getenv(direct_name, "")
    return os.getenv(generic_name, "")


API_KEY = _read_exchange_credential("key")
API_SECRET = _read_exchange_credential("secret")

SYMBOL = os.getenv(
    "SYMBOL",
    "XRP/USDT:USDT" if EXCHANGE_ID == "phemex" else "XRP/USDT",
)
PAPER_BALANCE = 100.0
CHECK_INTERVAL = 30
CMD_COOLDOWN_SECS = 5

if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
    raise SystemExit("ERROR: Missing TELEGRAM_TOKEN or TELEGRAM_CHAT_ID in ~/.env")
