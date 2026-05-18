import os
from dotenv import load_dotenv
# Global configuration and state
import threading

stop_flag       = threading.Event()
pending_changes = None
pending_lock    = threading.Lock()
TEST_MODE_ENABLED = False
TEST_MODE_PRICE = None
_LOCK_HELD = False
last_command_time = 0

# Config vars (can be dynamically updated by agent)
GRID_LOWER = 1.25
GRID_UPPER = 1.45
GRID_LEVELS = 20
ORDER_SIZE = 5.0
MAX_CATCHUP_ZONES = 3
AGENT_INTERVAL_HOURS = 3
# ============================================================
# LOAD CREDENTIALS
# ============================================================

load_dotenv(os.path.expanduser("~/.env"))

TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
API_KEY          = os.getenv("PHEMEX_API_KEY", "")
API_SECRET       = os.getenv("PHEMEX_API_SECRET", "")
GROQ_API_KEY     = os.getenv("GROQ_API_KEY", "")

# Live execution gating (must be explicitly enabled in env)
EXECUTE_LIVE = os.getenv("EXECUTE_LIVE", "false").lower() in ("1", "true", "yes")
EXCHANGE_ID = os.getenv("EXCHANGE_ID", "phemex")

if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
    raise SystemExit("ERROR: Missing TELEGRAM_TOKEN or TELEGRAM_CHAT_ID in ~/.env")

# ============================================================
# CONFIGURATION
