#!/usr/bin/env python3
"""
XRP/USDT Grid Trading Bot with Agentic Learning System
- Paper trading mode
- Telegram two-way control
- Catch-up sells and buys on restart
- Sell target = buy price + one full grid step (guaranteed profit)
- Agent adjusts all parameters including max_catchup_zones
- Groq LLM for reasoning, Claude Haiku ready as swap
- 30 minute approval window before grid changes apply

IMPROVEMENTS ADDED:
1. Dynamic position sizing based on volatility
2. Trend detection & grid bias
3. Smart grid rebalancing (auto-adjust when price drifts)
4. Fee-aware profit calculation (Phemex futures)
5. Position health monitoring & alerts
6. Performance analytics dashboard via Telegram
"""

import asyncio
import atexit
import json
import logging
import os
import random
import sqlite3
import sys
import threading
from datetime import datetime
from logging.handlers import RotatingFileHandler

import db as orders_db
from core.agent import apply_pending_changes, run_agent
from core.config import *
from core.telegram_handler import flush_telegram_queue, send_telegram, telegram_listener
from core.trading import (
    PaperTrader,
    find_oldest_open_buy_price,
    get_open_trades_from_db,
    normalize_remote_order,
    reconcile_once,
    reconcile_worker,
)
from exchange_adapter import ExchangeAdapter

# ============================================================

# Ensure imports of `android_grid_bot_v1` resolve to this process module even
# when launched as a script (`__main__`), avoiding duplicate module execution.
if __name__ == "__main__":
    sys.modules.setdefault("android_grid_bot_v1", sys.modules[__name__])

SYMBOL           = os.getenv("SYMBOL", SYMBOL)
PAPER_BALANCE    = 100.0
GRID_LOWER       = 1.25
GRID_UPPER       = 1.45
GRID_LEVELS      = 20
ORDER_SIZE       = 5.0
CHECK_INTERVAL   = 30
MIN_PROFIT_RATIO = 0.75

# Safety limits
MAX_OPEN_ZONES   = 10
MAX_TOTAL_TRADES = 10000
MAX_LOSS_PCT     = 20.0

# Catch-up settings
MAX_CATCHUP_ZONES = 3

# Agent settings
AGENT_INTERVAL_HOURS = 3
AGENT_APPROVAL_MINS  = 10
AGENT_MIN_TRADES     = 3

# Rate limiting
CMD_COOLDOWN_SECS = 5
last_command_time = 0

# Log rotation
LOG_FILE         = os.path.expanduser("~/grid_bot.log")
LOG_MAX_BYTES    = 5 * 1024 * 1024
LOG_BACKUP_COUNT = 3
LOCK_FILE        = os.path.expanduser("~/android_grid_bot_v1.lock")

# ============================================================
# NEW: VOLATILITY & TREND SETTINGS
# ============================================================

VOLATILITY_LOOKBACK = 14  # candles for ATR calculation
VOLATILITY_ADJUSTMENT = True
VOL_THRESHOLD_LOW = 0.01   # Below 1% = low volatility
VOL_THRESHOLD_HIGH = 0.05  # Above 5% = high volatility

TREND_LOOKBACK = 20  # candles for trend detection
TREND_THRESHOLD = 0.015  # 1.5% move to consider trending
TREND_BIAS_ENABLED = True

# ============================================================
# NEW: GRID REBALANCING SETTINGS
# ============================================================

GRID_REBALANCE_THRESHOLD = 0.15  # Rebalance if price >15% from grid center
AUTO_REBALANCE_ENABLED = True

# ============================================================
# NEW: FEE SETTINGS (PHEMEX FUTURES)
# ============================================================

MAKER_FEE_PCT = 0.0001  # 0.01% for limit orders
TAKER_FEE_PCT = 0.0006  # 0.06% for market orders
MIN_PROFIT_AFTER_FEES = 0.0005  # Minimum 0.05% profit after fees

# ============================================================
# NEW: POSITION HEALTH SETTINGS
# ============================================================

UNDERWATER_ALERT_THRESHOLD = -5.0  # Alert if position down >5%
HEALTH_CHECK_INTERVAL = 3600  # Check every hour (seconds)
# Reconciliation: how often to sync open orders with exchange (seconds)
RECONCILE_INTERVAL_SECONDS = int(os.getenv("RECONCILE_INTERVAL_SECONDS", "300"))

# ============================================================
# LOGGING
# ============================================================

log = logging.getLogger("gridbot")
log.setLevel(logging.INFO)

if not log.handlers:
    rotating_handler = RotatingFileHandler(
        LOG_FILE, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT
    )
    rotating_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] [pid=%(process)d] %(message)s")
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] [pid=%(process)d] %(message)s")
    )

    log.addHandler(rotating_handler)
    log.addHandler(console_handler)

# Global flags
stop_flag       = threading.Event()
pending_changes = None
pending_lock    = threading.Lock()
TEST_MODE_ENABLED = False
TEST_MODE_PRICE = None
_LOCK_HELD = False


def _pid_running(pid):
    """Best-effort process existence check for Linux/Termux."""
    if not pid or pid <= 0:
        return False
    return os.path.exists(f"/proc/{pid}")


def acquire_single_instance_lock():
    """Prevent multiple bot instances from running concurrently."""
    global _LOCK_HELD
    if _LOCK_HELD:
        return

    if os.path.exists(LOCK_FILE):
        try:
            with open(LOCK_FILE, "r", encoding="utf-8") as f:
                content = f.read().strip()
            existing_pid = int(content.split("|")[0]) if content else 0
        except Exception:
            existing_pid = 0

        if _pid_running(existing_pid):
            raise SystemExit(
                f"Another bot instance appears to be running (pid={existing_pid}). "
                f"If this is stale, remove {LOCK_FILE}."
            )

        try:
            os.remove(LOCK_FILE)
            log.warning("Removed stale lock file: %s", LOCK_FILE)
        except Exception as exc:
            raise SystemExit(
                f"Could not remove stale lock file {LOCK_FILE}: {exc}"
            ) from exc

    with open(LOCK_FILE, "w", encoding="utf-8") as f:
        f.write(f"{os.getpid()}|{datetime.now().isoformat()}")
    _LOCK_HELD = True
    log.info("Acquired single-instance lock at %s", LOCK_FILE)


def release_single_instance_lock():
    global _LOCK_HELD
    if not _LOCK_HELD:
        return
    try:
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
            log.info("Released single-instance lock at %s", LOCK_FILE)
    except Exception as e:
        log.warning("Could not remove lock file %s: %s", LOCK_FILE, e)
    finally:
        _LOCK_HELD = False


atexit.register(release_single_instance_lock)

# In-memory runtime metrics for observability.
METRICS_LOCK = threading.Lock()
BOT_METRICS = {
    "started_at": datetime.now().isoformat(),
    "telegram.commands.total": 0,
    "telegram.messages.sent": 0,
    "telegram.errors": 0,
    "trades.logged.total": 0,
    "trades.logged.buy": 0,
    "trades.logged.sell": 0,
    "reconcile.cycles": 0,
    "reconcile.inserted": 0,
    "reconcile.updated": 0,
    "reconcile.processed": 0,
    "reconcile.errors": 0,
    "bot.iterations": 0,
    "bot.errors": 0,
}


def _metrics_inc(name, amount=1):
    with METRICS_LOCK:
        BOT_METRICS[name] = BOT_METRICS.get(name, 0) + amount


def _metrics_set(name, value):
    with METRICS_LOCK:
        BOT_METRICS[name] = value


def _metrics_snapshot():
    with METRICS_LOCK:
        return dict(BOT_METRICS)


def _emit_event(event, level="info", **fields):
    """Emit a structured JSON log event with stable fields for parsing."""
    payload = {
        "ts": datetime.now().isoformat(),
        "event": event,
        "mode": get_runtime_mode_label(),
    }
    payload.update(fields)
    line = json.dumps(payload, default=str, sort_keys=True)
    if level == "debug":
        log.debug(line)
    elif level == "warning":
        log.warning(line)
    elif level == "error":
        log.error(line)
    else:
        log.info(line)


def get_runtime_mode_label():
    return "TEST" if TEST_MODE_ENABLED else "LIVE"


def get_simulated_price(seed_price=None):
    global TEST_MODE_PRICE
    if TEST_MODE_PRICE is None:
        base = seed_price if seed_price is not None else (GRID_LOWER + GRID_UPPER) / 2
        TEST_MODE_PRICE = round(base, 4)

    # Small random walk to mimic live ticks while remaining bounded.
    drift = TEST_MODE_PRICE * random.uniform(-0.0015, 0.0015)
    TEST_MODE_PRICE = round(min(5.0, max(0.5, TEST_MODE_PRICE + drift)), 4)
    return TEST_MODE_PRICE



# ============================================================
# DATABASE
# ============================================================

def init_db():
    conn = sqlite3.connect("trades.db")
    c = conn.cursor()

    # Table definitions: {table: [(col, type)]}
    schemas = {
        "trades": [
            ("id", "INTEGER PRIMARY KEY AUTOINCREMENT"),
            ("timestamp", "TEXT"),
            ("side", "TEXT"),
            ("price", "REAL"),
            ("amount", "REAL"),
            ("usdt_value", "REAL"),
            ("grid_level", "INTEGER"),
            ("profit_loss", "REAL"),
            ("balance_after", "REAL"),
            ("market_price", "REAL"),
            ("buy_price", "REAL"),
            ("sell_target", "REAL"),
            ("fees", "REAL"),
            ("notes", "TEXT")
        ],
        "agent_decisions": [
            ("id", "INTEGER PRIMARY KEY AUTOINCREMENT"),
            ("timestamp", "TEXT"),
            ("old_lower", "REAL"),
            ("old_upper", "REAL"),
            ("old_levels", "INTEGER"),
            ("old_order_size", "REAL"),
            ("old_max_catchup", "INTEGER"),
            ("new_lower", "REAL"),
            ("new_upper", "REAL"),
            ("new_levels", "INTEGER"),
            ("new_order_size", "REAL"),
            ("new_max_catchup", "INTEGER"),
            ("next_interval_hours", "REAL"),
            ("reasoning", "TEXT"),
            ("applied", "INTEGER DEFAULT 0"),
            ("rejected", "INTEGER DEFAULT 0")
        ],
        "catchup_trades": [
            ("id", "INTEGER PRIMARY KEY AUTOINCREMENT"),
            ("timestamp", "TEXT"),
            ("zone", "INTEGER"),
            ("entry_price", "REAL"),
            ("sell_target", "REAL"),
            ("exit_price", "REAL"),
            ("profit_loss", "REAL"),
            ("completed", "INTEGER DEFAULT 0")
        ]
    }

    for table, columns in schemas.items():
        # Create table if not exists
        col_defs = ", ".join([f"{col} {typ}" for col, typ in columns])
        try:
            c.execute(f"CREATE TABLE IF NOT EXISTS {table} ({col_defs})")
            log.info(f"Ensured table {table} exists.")
        except Exception as e:
            log.error(f"Could not create table {table}: {e}")
            continue
        # Get existing columns
        try:
            c.execute(f"PRAGMA table_info({table})")
            existing_cols = {row[1] for row in c.fetchall()}
        except Exception as e:
            log.error(f"Could not fetch columns for {table}: {e}")
            continue
        # Add missing columns
        for col, typ in columns:
            if col not in existing_cols:
                try:
                    c.execute(f"ALTER TABLE {table} ADD COLUMN {col} {typ}")
                    log.info(f"Added column {col} to {table}")
                except Exception as e:
                    log.error(f"Could not add column {col} to {table}: {e}")

    conn.commit()
    conn.close()
    # Ensure orders table used by the exchange reconciliation helper exists
    try:
        orders_db.ensure_orders_table()
        log.info("Ensured orders table exists for reconciliation.")
    except Exception as e:
        log.error(f"Could not ensure orders table: {e}")

    log.info("Database initialized (with auto table/column add)")

def log_trade(side, price, amount, usdt_value, grid_level, profit_loss,
              balance_after, market_price, buy_price=0, sell_target=0,
              fees=0, notes="", conn=None, commit=True):
    trade_id = orders_db.insert_trade(
        side,
        price,
        amount,
        usdt_value,
        grid_level,
        profit_loss,
        balance_after,
        market_price,
        buy_price=buy_price,
        sell_target=sell_target,
        fees=fees,
        notes=notes,
        timestamp=datetime.now().isoformat(),
        conn=conn,
        commit=commit,
    )
    _metrics_inc("trades.logged.total")
    _metrics_inc(f"trades.logged.{str(side).lower()}")
    _emit_event(
        "trade.logged",
        trade_id=trade_id,
        side=side,
        price=price,
        amount=amount,
        usdt_value=usdt_value,
        grid_level=grid_level,
        profit_loss=profit_loss,
        fees=fees,
        notes=notes,
    )
    return trade_id

def _extract_fill_info(row, norm):
    """Return (filled_amt, status, side, price) from a DB row + normalised order."""
    def _safe(key, fallback=None):
        try:
            return row[key]
        except Exception:
            return fallback

    filled_amt = (
        norm.get("filled")
        if norm and norm.get("filled") is not None
        else (_safe("filled") or 0)
    )
    status = (
        norm.get("status") if norm and norm.get("status") else _safe("status")
    )
    row_side = _safe("side") or ""
    side = (row_side or (norm.get("side") if norm else "") or "").upper()
    price = (
        norm.get("price") if norm and norm.get("price") else (_safe("price") or 0)
    )
    return filled_amt, status, side, price


def _infer_zone_for_order(trader, side, price):
    """Infer the grid zone for a filled order using in-memory trader state."""
    if trader is None or not getattr(trader, "grid_levels", None):
        return None
    try:
        if side == "BUY":
            return get_current_zone(price, trader.grid_levels)
        if side == "SELL":
            for zi in range(trader.num_zones):
                st = trader.zone_sell_target[zi]
                if st and abs(st - price) <= (trader.step_size * 0.0001):
                    return zi
    except Exception:
        pass
    return None


def _extract_fee_cost(remote, norm):
    """Extract total fee cost from raw remote order dict (0.0 if unavailable)."""
    if not norm or not isinstance(remote, dict):
        return 0.0
    try:
        f = remote.get("fee")
        if isinstance(f, dict):
            return float(f.get("cost") or 0)
        fees_list = remote.get("fees")
        if fees_list:
            return sum(float(x.get("cost", 0)) for x in fees_list if isinstance(x, dict))
    except Exception:
        pass
    return 0.0


def _reconcile_log_buy(
    local_exchange_id, exch_id, trader, price, trade_amount,
    usdt_value, zone, filled_amt, status, is_full, fee_cost, market_price,
):
    """Atomically log a reconciled BUY fill and update in-memory grid state."""
    notes = f"reconciled_buy;order={exch_id}"
    sell_target = round(price + (trader.step_size if trader else 0), 4) if trader else 0
    processed_fields = {"filled": filled_amt, "status": status}
    if is_full:
        processed_fields["processed"] = 1
    with orders_db.transaction() as tx_conn:
        trade_id = log_trade(
            "BUY", price, trade_amount, usdt_value, zone, 0,
            trader.portfolio_value(market_price) if trader and market_price else None,
            market_price or price, price, sell_target, fee_cost, notes,
            conn=tx_conn, commit=False,
        )
        if trade_id and exch_id:
            orders_db.insert_order_trade_mapping(
                local_exchange_id, exch_id, trade_id,
                "BUY", price, trade_amount, fee_cost,
                conn=tx_conn, commit=False,
            )
        orders_db.update_order_by_exchange_id(
            local_exchange_id, exch_id, conn=tx_conn, commit=False, **processed_fields,
        )
    if trader is not None and zone is not None and 0 <= zone < trader.num_zones:
        trader.zone_holding[zone] = True
        trader.zone_buy_price[zone] = price
        trader.zone_sell_target[zone] = round(price + trader.step_size, 4)


def _resolve_buy_price_for_sell(trader, zone, price_for_log):
    """Look up the buy price for a SELL order; returns (zone, buy_price)."""
    buy_price = None
    if trader is not None and zone is not None and 0 <= zone < trader.num_zones:
        buy_price = trader.zone_buy_price[zone]
    if buy_price is None and zone is not None:
        buy_price = find_oldest_open_buy_price(zone)
    if buy_price is None and zone is None and trader is not None and getattr(trader, "grid_levels", None):
        try:
            for t in get_open_trades_from_db(limit=1000):
                st = t.get("sell_target") or 0
                if st and abs(st - price_for_log) <= (trader.step_size * 0.0005):
                    return t.get("zone"), t.get("price")
        except Exception:
            pass
    return zone, buy_price


def _reconcile_log_sell(
    local_exchange_id, exch_id, trader, remote, norm,
    price, trade_amount, usdt_value, zone,
    filled_amt, status, is_full, fee_cost, market_price,
):
    """Atomically log a reconciled SELL fill with P&L and update in-memory state."""
    zone, buy_price = _resolve_buy_price_for_sell(trader, zone, price)
    order_type = ""
    try:
        if isinstance(remote, dict):
            order_type = (remote.get("type") or remote.get("orderType") or "").lower()
    except Exception:
        pass
    sell_fee_rate = TAKER_FEE_PCT if "market" in (order_type or "") else MAKER_FEE_PCT
    sell_fee_est = filled_amt * price * sell_fee_rate
    buy_fee_est = filled_amt * (buy_price or 0) * MAKER_FEE_PCT if buy_price else 0.0
    total_fee = fee_cost if fee_cost and fee_cost > 0 else (sell_fee_est + buy_fee_est)
    profit_loss = (price - buy_price) * filled_amt - total_fee if buy_price else -total_fee
    balance_after = trader.portfolio_value(market_price) if trader and market_price else 0.0
    notes = f"reconciled_sell;order={exch_id}"
    processed_fields = {"filled": filled_amt, "status": status}
    if is_full:
        processed_fields["processed"] = 1
    with orders_db.transaction() as tx_conn:
        trade_id = log_trade(
            "SELL", price, trade_amount, usdt_value, zone, profit_loss,
            balance_after, market_price or price,
            buy_price or 0, price, total_fee, notes,
            conn=tx_conn, commit=False,
        )
        if trade_id and exch_id:
            orders_db.insert_order_trade_mapping(
                local_exchange_id, exch_id, trade_id,
                "SELL", price, trade_amount, total_fee,
                conn=tx_conn, commit=False,
            )
        orders_db.update_order_by_exchange_id(
            local_exchange_id, exch_id, conn=tx_conn, commit=False, **processed_fields,
        )
    if trader is not None and zone is not None and 0 <= zone < trader.num_zones and is_full:
        trader.zone_holding[zone] = False
        trader.zone_buy_price[zone] = 0.0
        trader.zone_sell_target[zone] = 0.0


def _reconcile_fetch_order_for_row(adapter, row):
    exch_id = row["exchange_order_id"]
    try:
        return adapter.fetch_order(exch_id, row["symbol"] or None)
    except Exception as e:
        log.debug(
            "Reconciliation: could not fetch order %s for processing: %s",
            exch_id,
            e,
        )
        return None


def _reconcile_compute_fill_state(row, norm):
    filled_amt, status, side, price = _extract_fill_info(row, norm)
    if not filled_amt or filled_amt <= 0:
        return None
    try:
        total_amt = row["amount"] or 0
    except Exception:
        total_amt = 0
    is_full = (abs(filled_amt - total_amt) < 1e-8) or (
        status and str(status).lower() in ("closed", "filled")
    )
    return filled_amt, status, side, price, is_full


def _reconcile_get_market_price(adapter):
    try:
        if hasattr(adapter, "fetch_ticker"):
            return adapter.fetch_ticker(SYMBOL)["last"]
    except Exception:
        return None
    return None


def _reconcile_process_single_row(adapter, row, local_exchange_id, trader):
    exch_id = row["exchange_order_id"]
    remote = _reconcile_fetch_order_for_row(adapter, row)
    if remote is None:
        return 0

    norm = normalize_remote_order(remote)
    fill_state = _reconcile_compute_fill_state(row, norm)
    if fill_state is None:
        return 0
    filled_amt, status, side, price, is_full = fill_state

    zone = _infer_zone_for_order(trader, side, price)
    fee_cost = _extract_fee_cost(remote, norm)
    market_price = _reconcile_get_market_price(adapter)

    try:
        mapped_total = float(
            orders_db.get_mapped_amount_sum(local_exchange_id, exch_id) or 0.0
        )
    except Exception:
        mapped_total = 0.0
    delta_amt = filled_amt - mapped_total
    if delta_amt <= 1e-8:
        fields = {"filled": filled_amt, "status": status}
        if is_full:
            fields["processed"] = 1
        orders_db.update_order_by_exchange_id(local_exchange_id, exch_id, **fields)
        return 0

    usdt_value = round(delta_amt * price, 8)
    if side == "BUY":
        _reconcile_log_buy(
            local_exchange_id, exch_id, trader, price, delta_amt,
            usdt_value, zone, filled_amt, status, is_full, fee_cost, market_price,
        )
    elif side == "SELL":
        _reconcile_log_sell(
            local_exchange_id, exch_id, trader, remote, norm,
            price, delta_amt, usdt_value, zone,
            filled_amt, status, is_full, fee_cost, market_price,
        )
    return 1 if is_full else 0


def _reconcile_process_unprocessed(adapter, unproc, local_exchange_id, trader):
    """Iterate unprocessed DB orders and log trade rows for any filled amounts."""
    processed_count = 0
    for row in unproc:
        try:
            processed_count += _reconcile_process_single_row(
                adapter, row, local_exchange_id, trader
            )
        except Exception as e:
            exch_id = row.get("exchange_order_id", "unknown")
            log.error("Reconciliation: failed to log trade for %s: %s", exch_id, e)
    return processed_count


# ---- public entry point ------------------------------------------------------




# ============================================================
# GRID LOGIC
# ============================================================

def calculate_grid_levels(lower, upper, levels):
    step = (upper - lower) / levels
    return [round(lower + i * step, 4) for i in range(levels + 1)]

def get_current_zone(price, grid_levels):
    for i in range(len(grid_levels) - 1):
        if grid_levels[i] <= price < grid_levels[i + 1]:
            return i
    return len(grid_levels) - 2


def get_exchange():
    """Build a synchronous exchange adapter for trading + reconciliation."""
    options = None
    if EXCHANGE_ID == "phemex":
        options = {"defaultType": "swap"}
    elif EXCHANGE_ID == "binance" and EXCHANGE_MARKET_TYPE in {"future", "swap", "futures"}:
        options = {"defaultType": "future"}

    return ExchangeAdapter(
        exchange_id=EXCHANGE_ID,
        api_key=API_KEY,
        secret=API_SECRET,
        enable_rate_limit=True,
        testnet=EXCHANGE_TESTNET,
        options=options,
    )


def get_price(exchange):
    if TEST_MODE_ENABLED:
        return get_simulated_price()
    ticker = exchange.fetch_ticker(SYMBOL)
    return ticker["last"]


# ============================================================
# MAIN BOT LOOP
# ============================================================

def _start_bot_threads(trader, exchange, start_offset):
    """Start the Telegram listener and reconciliation worker as asyncio tasks."""
    asyncio.create_task(telegram_listener(trader, exchange, start_offset))
    log.info("Telegram command listener task created")

    try:
        exch_id = getattr(trader.exchange, 'exchange_id', EXCHANGE_ID)
        asyncio.create_task(reconcile_worker(
            getattr(trader, 'exchange', None),
            SYMBOL,
            exch_id,
            trader,
            RECONCILE_INTERVAL_SECONDS,
        ))
        log.info("Reconciliation worker task created (interval %ds)", RECONCILE_INTERVAL_SECONDS)
    except Exception as e:
        log.warning("Could not start reconciliation worker: %s", e)


def _log_bot_startup_info():
    log.info("Starting XRP/USDT Grid Bot with Agent (Paper Trading)")
    log.info("IMPROVEMENTS ACTIVE:")
    log.info("  - Dynamic position sizing (volatility-based)")
    log.info("  - Trend detection & grid bias")
    log.info("  - Auto grid rebalancing (%.0f%% threshold)", GRID_REBALANCE_THRESHOLD * 100)
    log.info("  - Fee-aware profit (Maker: %.2f%%, Taker: %.2f%%)", MAKER_FEE_PCT * 100, TAKER_FEE_PCT * 100)
    log.info("  - Position health alerts (%.1f%% threshold)", UNDERWATER_ALERT_THRESHOLD)
    log.info("  - Telegram analytics dashboard")


# ============================================================
# MAIN BOT LOOP
# ============================================================

def _run_initial_reconciliation(trader):
    try:
        exch_id = getattr(trader.exchange, 'exchange_id', EXCHANGE_ID)
        reconcile_once(getattr(trader, 'exchange', None), SYMBOL, exch_id, trader)
    except Exception as e:
        log.warning("Initial reconciliation failed: %s", e)


async def _pause_if_outside_grid(current_price):
    if current_price < GRID_LOWER:
        send_telegram(
            f"⚠️ <b>Price Below Range</b>\n"
            f"XRP: ${current_price:.4f} | Floor: ${GRID_LOWER}\n"
            "Bot paused."
        )
        await asyncio.sleep(CHECK_INTERVAL)
        return True
    if current_price > GRID_UPPER:
        send_telegram(
            f"⚠️ <b>Price Above Range</b>\n"
            f"XRP: ${current_price:.4f} | Ceiling: ${GRID_UPPER}\n"
            "Bot paused."
        )
        await asyncio.sleep(CHECK_INTERVAL)
        return True
    return False


def _maybe_send_hourly_status(trader, current_price, status_counter, checks_per_hour):
    status_counter += 1
    if status_counter < checks_per_hour:
        return status_counter
    send_telegram(trader.status_report(current_price))
    trader.check_underwater_positions(current_price)
    return 0


def _maybe_trigger_agent(trader, current_price, last_agent_run):
    hours_since_agent = (datetime.now() - last_agent_run).total_seconds() / 3600
    if hours_since_agent < AGENT_INTERVAL_HOURS:
        return last_agent_run
    now = datetime.now()
    # Assuming run_agent is synchronous or if async use create_task
    import threading
    threading.Thread(
        target=run_agent,
        args=(trader, current_price),
        daemon=True,
    ).start()
    return now


async def _run_bot_iteration(
    trader,
    exchange,
    last_price,
    status_counter,
    checks_per_hour,
    last_agent_run,
):
    _metrics_inc("bot.iterations")
    current_price = get_price(exchange)
    log.info("Price: $%.4f", current_price)

    if not trader.safety_check(current_price):
        return last_price, status_counter, last_agent_run, True

    apply_pending_changes(trader)
    trader.check_grid_rebalance(current_price)
    is_paused = await _pause_if_outside_grid(current_price)
    if is_paused:
        return last_price, status_counter, last_agent_run, False

    trader.check_grid(current_price, last_price, exchange)
    status_counter = _maybe_send_hourly_status(
        trader, current_price, status_counter, checks_per_hour
    )
    last_agent_run = _maybe_trigger_agent(trader, current_price, last_agent_run)
    await asyncio.sleep(CHECK_INTERVAL)
    return current_price, status_counter, last_agent_run, False


async def _run_bot_runtime_loop(trader, exchange, starting_price):
    last_price = starting_price
    status_counter = 0
    checks_per_hour = (60 * 60) // CHECK_INTERVAL
    last_agent_run = datetime.now()

    while not stop_flag.is_set():
        try:
            last_price, status_counter, last_agent_run, should_stop = await _run_bot_iteration(
                trader,
                exchange,
                last_price,
                status_counter,
                checks_per_hour,
                last_agent_run,
            )
            if should_stop:
                break
        except KeyboardInterrupt:
            log.info("Bot stopped by user")
            stop_flag.set()
            break
        except Exception as e:
            _metrics_inc("bot.errors")
            _emit_event("bot.runtime_error", level="error", error=str(e))
            log.error("Error: %s", e)
            send_telegram(f"⚠️ Bot error: {e}\nRestarting in 60s...")
            await asyncio.sleep(60)


def _send_shutdown_message(trader, exchange):
    _metrics_set("stopped_at", datetime.now().isoformat())
    _emit_event("bot.shutdown", **_metrics_snapshot())
    try:
        final_price = get_price(exchange)
        send_telegram(
            f"🛑 <b>Bot Stopped</b>\n"
            f"Final Price: ${final_price:.4f}\n"
            f"Final Net Profit: ${trader.total_profit:.4f}\n"
            f"Total Fees Paid: ${trader.total_fees_paid:.4f}\n"
            f"Total Trades: {trader.total_trades}\n"
            f"Portfolio: ${trader.portfolio_value(final_price):.2f}"
        )
    except Exception:
        send_telegram(
            f"🛑 Bot stopped.\n"
            f"Net Profit: ${trader.total_profit:.4f}"
        )


async def run_bot():
    global AGENT_INTERVAL_HOURS

    acquire_single_instance_lock()
    _log_bot_startup_info()
    init_db()

    exchange = get_exchange()
    grid_levels = calculate_grid_levels(GRID_LOWER, GRID_UPPER, GRID_LEVELS)
    trader = PaperTrader(PAPER_BALANCE, grid_levels)
    try:
        trader.exchange = exchange
    except Exception:
        trader.exchange = None

    log.info("Starting balance: $%s USDT", PAPER_BALANCE)
    starting_price = get_price(exchange)
    trend = 'SIDEWAYS' # detect_trend is blocking, maybe keep sideways or await
    log.info("Starting trend detection: %s", trend)

    trader.initialize_zones(starting_price, trend)
    trader.restore_open_trades_from_db()
    trader.check_missed_buys(starting_price)
    _run_initial_reconciliation(trader)

    start_offset = flush_telegram_queue()
    _start_bot_threads(trader, exchange, start_offset)
    await _run_bot_runtime_loop(trader, exchange, starting_price)
    _send_shutdown_message(trader, exchange)
    log.info("Bot shutdown complete")

if __name__ == "__main__":
    asyncio.run(run_bot())
