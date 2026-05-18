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

import atexit
import json
import logging
import os
import random
import sqlite3
import threading
import time
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler

import ccxt
import pandas as pd
import requests
from dotenv import load_dotenv

import db as orders_db
from exchange_adapter import ExchangeAdapter

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
# ============================================================

SYMBOL           = "XRP/USDT:USDT"
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
LOCK_FILE        = os.path.expanduser("~/andriod_grid_bot_v1.lock")

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

rotating_handler = RotatingFileHandler(
    LOG_FILE, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT)
rotating_handler.setFormatter(
    logging.Formatter("%(asctime)s [%(levelname)s] [pid=%(process)d] %(message)s"))

console_handler = logging.StreamHandler()
console_handler.setFormatter(
    logging.Formatter("%(asctime)s [%(levelname)s] [pid=%(process)d] %(message)s"))

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


def queue_simulated_agent_change(trader, current_price, approval_seconds=30):
    global pending_changes

    with pending_lock:
        if pending_changes:
            return False, "There are already pending agent changes. Use /apply or /reject first."

        new_lower = max(0.50, round(trader.grid_lower - 0.01, 4))
        new_upper = min(5.00, round(trader.grid_upper + 0.01, 4))
        new_levels = min(50, max(10, trader.grid_levels_count + 1))

        decision = {
            "new_lower": new_lower,
            "new_upper": new_upper,
            "new_levels": new_levels,
            "new_order_size": trader.order_size,
            "new_max_catchup_zones": trader.max_catchup_zones,
            "next_interval_hours": AGENT_INTERVAL_HOURS,
            "reasoning": "Test mode simulation queued a safe parameter tweak.",
            "changes_needed": True,
            "old_lower": trader.grid_lower,
            "old_upper": trader.grid_upper,
            "old_levels": trader.grid_levels_count,
            "old_order_size": trader.order_size,
            "old_max_catchup": trader.max_catchup_zones,
            "new_max_catchup": trader.max_catchup_zones,
        }

        apply_time = datetime.now() + timedelta(seconds=approval_seconds)
        pending_changes = {"decision": decision, "apply_at": apply_time}

    send_telegram(
        f"🧪 <b>Test Mode Simulation Queued</b>\n\n"
        f"Current price: ${current_price:.4f}\n"
        f"Grid: ${decision['new_lower']} - ${decision['new_upper']}\n"
        f"Levels: {decision['new_levels']} (was {decision['old_levels']})\n"
        f"Order Size: ${decision['new_order_size']}\n\n"
        f"⏳ Auto-apply in {approval_seconds}s\n"
        f"Use /apply to apply now or /reject to cancel."
    )
    log.info(f"[TESTMODE] Simulated agent decision queued for {apply_time}")
    return True, "Simulated agent change queued."

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

def log_catchup_trade(zone, entry_price, sell_target):
    conn = sqlite3.connect("trades.db")
    c = conn.cursor()
    c.execute("""
        INSERT INTO catchup_trades
        (timestamp, zone, entry_price, sell_target, completed)
        VALUES (?, ?, ?, ?, 0)
    """, (datetime.now().isoformat(), zone, entry_price, sell_target))
    conn.commit()
    conn.close()

def complete_catchup_trade(zone, exit_price, profit_loss):
    conn = sqlite3.connect("trades.db")
    c = conn.cursor()
    c.execute("""
        UPDATE catchup_trades SET exit_price=?, profit_loss=?, completed=1
        WHERE zone=? AND completed=0
    """, (exit_price, profit_loss, zone))
    conn.commit()
    conn.close()

def get_catchup_stats():
    conn = sqlite3.connect("trades.db")
    c = conn.cursor()
    c.execute("""
        SELECT COUNT(*), SUM(profit_loss), AVG(profit_loss)
        FROM catchup_trades WHERE completed=1
    """)
    row = c.fetchone()
    conn.close()
    return {
        "total": row[0] or 0,
        "total_profit": round(row[1] or 0, 4),
        "avg_profit": round(row[2] or 0, 4)
    }

def log_agent_decision(decision, applied=False, rejected=False):
    conn = sqlite3.connect("trades.db")
    c = conn.cursor()
    c.execute("""
        INSERT INTO agent_decisions (
            timestamp, old_lower, old_upper, old_levels, old_order_size,
            old_max_catchup, new_lower, new_upper, new_levels, new_order_size,
            new_max_catchup, next_interval_hours, reasoning, applied, rejected)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        datetime.now().isoformat(),
        decision["old_lower"], decision["old_upper"],
        decision["old_levels"], decision["old_order_size"],
        decision["old_max_catchup"],
        decision["new_lower"], decision["new_upper"],
        decision["new_levels"], decision["new_order_size"],
        decision["new_max_catchup"],
        decision["next_interval_hours"],
        decision["reasoning"],
        1 if applied else 0,
        1 if rejected else 0
    ))
    conn.commit()
    conn.close()

def get_last_trades(n=5):
    conn = sqlite3.connect("trades.db")
    c = conn.cursor()
    c.execute("""
        SELECT timestamp, side, price, amount, profit_loss, grid_level, sell_target
        FROM trades ORDER BY id DESC LIMIT ?
    """, (n,))
    rows = c.fetchall()
    conn.close()
    return rows

def get_open_trades_from_db(limit=25):
    """
    Reconstruct currently open trades by pairing BUY/SELL entries FIFO per zone.
    This allows commands like /grid to show open trades even after a bot restart.
    """
    conn = sqlite3.connect("trades.db")
    c = conn.cursor()
    c.execute("""
        SELECT id, timestamp, side, price, amount, grid_level, sell_target
        FROM trades
        WHERE side IN ('BUY', 'SELL')
        ORDER BY id ASC
    """)
    rows = c.fetchall()
    conn.close()

    zone_queues = {}
    for trade_id, ts, side, price, amount, zone, sell_target in rows:
        if zone is None:
            continue

        if side == "BUY":
            zone_queues.setdefault(zone, []).append({
                "id": trade_id,
                "timestamp": ts,
                "price": price,
                "amount": amount,
                "zone": zone,
                "sell_target": sell_target or 0.0,
            })
        elif side == "SELL":
            q = zone_queues.get(zone)
            if q:
                q.pop(0)

    open_trades = []
    for q in zone_queues.values():
        open_trades.extend(q)

    open_trades.sort(key=lambda t: t["id"], reverse=True)
    return open_trades[:limit]

def get_trade_stats():
    conn = sqlite3.connect("trades.db")
    c = conn.cursor()

    c.execute("SELECT COUNT(*) FROM trades WHERE side='BUY'")
    total_buys = c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM trades WHERE side='SELL'")
    total_sells = c.fetchone()[0]

    c.execute("""SELECT SUM(profit_loss), AVG(profit_loss),
              MAX(profit_loss), MIN(profit_loss)
              FROM trades WHERE side='SELL'""")
    profit_row   = c.fetchone()
    total_profit = profit_row[0] or 0
    avg_profit   = profit_row[1] or 0
    max_profit   = profit_row[2] or 0
    min_profit   = profit_row[3] or 0

    c.execute("""SELECT COUNT(*) FROM trades
              WHERE side='SELL' AND profit_loss > 0""")
    winning_trades = c.fetchone()[0]
    win_rate = (winning_trades / total_sells * 100) if total_sells > 0 else 0

    c.execute("""SELECT grid_level, COUNT(*) as cnt
              FROM trades GROUP BY grid_level ORDER BY cnt DESC LIMIT 5""")
    active_zones = c.fetchall()

    c.execute("""SELECT MAX(market_price), MIN(market_price)
              FROM trades WHERE timestamp > datetime('now', '-6 hours')""")
    price_row   = c.fetchone()
    recent_high = price_row[0] or 0
    recent_low  = price_row[1] or 0

    c.execute("""
        SELECT AVG(julianday(b.timestamp) - julianday(a.timestamp)) * 24 * 60
        FROM trades a JOIN trades b
        ON a.grid_level = b.grid_level
        AND a.side = 'BUY' AND b.side = 'SELL'
        AND b.timestamp > a.timestamp
    """)
    avg_hold_row  = c.fetchone()
    avg_hold_mins = avg_hold_row[0] or 0

    conn.close()
    catchup = get_catchup_stats()

    return {
        "total_buys": total_buys,
        "total_sells": total_sells,
        "total_profit": round(total_profit, 4),
        "avg_profit": round(avg_profit, 4),
        "max_profit": round(max_profit, 4),
        "min_profit": round(min_profit, 4),
        "win_rate_pct": round(win_rate, 1),
        "winning_trades": winning_trades,
        "most_active_zones": active_zones,
        "recent_high": recent_high,
        "recent_low": recent_low,
        "avg_hold_mins": round(avg_hold_mins, 1),
        "catchup_total": catchup["total"],
        "catchup_profit": catchup["total_profit"],
        "catchup_avg_profit": catchup["avg_profit"]
    }

# ============================================================
# NEW: ADVANCED ANALYTICS
# ============================================================

def get_advanced_stats():
    """Generate comprehensive performance analytics"""
    conn = sqlite3.connect("trades.db")
    c = conn.cursor()

    # Win rate by zone
    c.execute("""
        SELECT grid_level,
               SUM(CASE WHEN profit_loss > 0 THEN 1 ELSE 0 END) as wins,
               COUNT(*) as total,
               AVG(profit_loss) as avg_pnl
        FROM trades WHERE side='SELL'
        GROUP BY grid_level
        ORDER BY grid_level
    """)
    zone_performance = c.fetchall()

    # Best/worst hours to trade
    c.execute("""
        SELECT strftime('%H', timestamp) as hour,
               AVG(profit_loss) as avg_pnl,
               COUNT(*) as trades
        FROM trades WHERE side='SELL'
        GROUP BY hour
        ORDER BY avg_pnl DESC
        LIMIT 5
    """)
    best_hours = c.fetchall()

    # Worst hours to trade
    c.execute("""
        SELECT strftime('%H', timestamp) as hour,
               AVG(profit_loss) as avg_pnl,
               COUNT(*) as trades
        FROM trades WHERE side='SELL'
        GROUP BY hour
        ORDER BY avg_pnl ASC
        LIMIT 3
    """)
    worst_hours = c.fetchall()

    # Average hold time by zone
    c.execute("""
        SELECT buy.grid_level,
               AVG(julianday(sell.timestamp) - julianday(buy.timestamp)) * 24 * 60 as avg_mins
        FROM trades buy
        JOIN trades sell ON buy.grid_level = sell.grid_level
            AND buy.side = 'BUY' AND sell.side = 'SELL'
            AND sell.timestamp > buy.timestamp
        GROUP BY buy.grid_level
        LIMIT 10
    """)
    hold_times = c.fetchall()

    # Profit factor (gross profit / gross loss)
    c.execute("""
        SELECT
            SUM(CASE WHEN profit_loss > 0 THEN profit_loss ELSE 0 END) as gross_profit,
            SUM(CASE WHEN profit_loss < 0 THEN ABS(profit_loss) ELSE 0 END) as gross_loss
        FROM trades WHERE side='SELL'
    """)
    pf_row = c.fetchone()
    gross_profit = pf_row[0] or 0
    gross_loss = pf_row[1] or 0
    profit_factor = round(gross_profit / gross_loss, 2) if gross_loss > 0 else float('inf')

    # Daily breakdown (last 7 days)
    c.execute("""
        SELECT date(timestamp) as day,
               COUNT(*) as trades,
               SUM(profit_loss) as daily_pnl
        FROM trades WHERE side='SELL'
        AND timestamp > datetime('now', '-7 days')
        GROUP BY day
        ORDER BY day DESC
    """)
    daily_breakdown = c.fetchall()

    # Largest wins and losses
    c.execute("""
        SELECT price, profit_loss, grid_level, timestamp
        FROM trades WHERE side='SELL'
        ORDER BY profit_loss DESC LIMIT 3
    """)
    biggest_wins = c.fetchall()

    c.execute("""
        SELECT price, profit_loss, grid_level, timestamp
        FROM trades WHERE side='SELL'
        ORDER BY profit_loss ASC LIMIT 3
    """)
    biggest_losses = c.fetchall()

    conn.close()

    return {
        "zone_performance": zone_performance,
        "best_hours": best_hours,
        "worst_hours": worst_hours,
        "hold_times": hold_times,
        "profit_factor": profit_factor,
        "daily_breakdown": daily_breakdown,
        "biggest_wins": biggest_wins,
        "biggest_losses": biggest_losses,
        "gross_profit": round(gross_profit, 4),
        "gross_loss": round(gross_loss, 4)
    }

def export_performance_csv(filename="bot_performance.csv"):
    """Export all trade data to CSV file"""
    try:
        conn = sqlite3.connect("trades.db")
        df = pd.read_sql_query("SELECT * FROM trades ORDER BY timestamp DESC", conn)
        conn.close()
        df.to_csv(filename, index=False)
        log.info(f"Exported {len(df)} trades to {filename}")
        return filename
    except Exception as e:
        log.error(f"CSV export error: {e}")
        return None

def get_performance_summary():
    """Quick one-line performance summary"""
    stats = get_trade_stats()
    advanced = get_advanced_stats()

    pnl_emoji = "🟢" if stats['total_profit'] > 0 else "🔴"
    return (
        f"{pnl_emoji} Profit: ${stats['total_profit']:.4f} | "
        f"Win Rate: {stats['win_rate_pct']:.1f}% | "
        f"Trades: {stats['total_sells']} | "
        f"Factor: {advanced['profit_factor']}"
    )

# ============================================================
# TELEGRAM
# ============================================================

def send_telegram(message):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        resp = requests.post(url, data={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        }, timeout=10)
        if resp.ok:
            _metrics_inc("telegram.messages.sent")
        else:
            _metrics_inc("telegram.errors")
            _emit_event(
                "telegram.send_failed",
                level="warning",
                status_code=resp.status_code,
                body=resp.text[:300],
            )
    except Exception as e:
        _metrics_inc("telegram.errors")
        _emit_event("telegram.send_error", level="error", error=str(e))
        log.error(f"Telegram send error: {e}")

def flush_telegram_queue():
    try:
        url      = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
        response = requests.get(url, timeout=10)
        data     = response.json()
        if data.get("result"):
            latest_id = data["result"][-1]["update_id"]
            requests.get(url, params={"offset": latest_id + 1}, timeout=10)
            log.info(f"Flushed {len(data['result'])} pending Telegram messages")
            return latest_id + 1
        return None
    except Exception as e:
        log.error(f"Queue flush error: {e}")
        return None

def get_telegram_updates(offset=None):
    try:
        url    = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
        params = {"timeout": 10, "allowed_updates": ["message"]}
        if offset:
            params["offset"] = offset
        response = requests.get(url, params=params, timeout=15)
        return response.json()
    except Exception as e:
        log.error(f"Telegram polling error: {e}")
        return {"ok": False, "result": []}

# ============================================================
# NEW: VOLATILITY CALCULATION
# ============================================================

def calculate_volatility(exchange, symbol, lookback=14):
    """Calculate recent price volatility using ATR-like method"""
    try:
        # Phemex OHLCV endpoint requires base symbol format (XRP/USDT not XRP/USDT:USDT)
        fetch_symbol = symbol.replace(':USDT', '') if ':' in symbol else symbol
        ohlcv = exchange.fetch_ohlcv(fetch_symbol, timeframe='1h', limit=lookback)
        if len(ohlcv) < 2:
            return 0.02  # Default 2%

        true_ranges = []
        for i in range(1, len(ohlcv)):
            high = ohlcv[i][2]
            low = ohlcv[i][3]
            prev_close = ohlcv[i-1][4]
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            true_ranges.append(tr / prev_close)  # Normalized

        return sum(true_ranges) / len(true_ranges)
    except Exception as e:
        log.error(f"Volatility calculation error: {e}")
        return 0.02

def get_dynamic_order_size(base_size, volatility, vol_threshold_low=0.01, vol_threshold_high=0.05):
    """
    Adjust order size based on volatility:
    - Low volatility: Increase size (more confidence)
    - High volatility: Decrease size (reduce risk)
    """
    if volatility < vol_threshold_low:
        multiplier = 1.5  # 50% more
    elif volatility > vol_threshold_high:
        multiplier = 0.5  # 50% less
    else:
        # Linear interpolation
        multiplier = 1.0 - 0.5 * (volatility - vol_threshold_low) / (vol_threshold_high - vol_threshold_low)

    return round(base_size * multiplier, 2)

# ============================================================
# NEW: TREND DETECTION
# ============================================================

def detect_trend(exchange, symbol, lookback=20):
    """
    Returns: 'BULLISH', 'BEARISH', or 'SIDEWAYS'
    """
    try:
        # Phemex OHLCV endpoint requires base symbol format (XRP/USDT not XRP/USDT:USDT)
        fetch_symbol = symbol.replace(':USDT', '') if ':' in symbol else symbol
        ohlcv = exchange.fetch_ohlcv(fetch_symbol, timeframe='4h', limit=lookback)
        if len(ohlcv) < lookback:
            return 'SIDEWAYS'

        closes = [c[4] for c in ohlcv]
        start_price = closes[0]
        end_price = closes[-1]
        change_pct = (end_price - start_price) / start_price

        # Also check moving averages
        ma_short = sum(closes[-5:]) / 5
        ma_long = sum(closes[-15:]) / 15

        if change_pct > TREND_THRESHOLD and ma_short > ma_long:
            return 'BULLISH'
        elif change_pct < -TREND_THRESHOLD and ma_short < ma_long:
            return 'BEARISH'
        else:
            return 'SIDEWAYS'
    except Exception as e:
        log.error(f"Trend detection error: {e}")
        return 'SIDEWAYS'

# ============================================================
# AGENTIC SYSTEM
# ============================================================

def call_groq(prompt):
    try:
        from groq import Groq
        client   = Groq(api_key=GROQ_API_KEY)
        response = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are an expert algorithmic trading agent analyzing "
                        "a grid trading bot's performance. Your job is to analyze "
                        "trade statistics and recommend grid parameter adjustments "
                        "to improve profitability. Always respond with valid JSON only. "
                        "No markdown, no explanation outside the JSON."
                    )
                },
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=1000
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        log.error(f"Groq API error: {e}")
        return None

def run_agent(trader, current_price):
    global AGENT_INTERVAL_HOURS, MAX_CATCHUP_ZONES, pending_changes

    if TEST_MODE_ENABLED:
        log.info("[TESTMODE] run_agent intercepted; queuing simulated decision.")
        queue_simulated_agent_change(trader, current_price, approval_seconds=30)
        return

    log.info("Agent analysis starting...")
    stats = get_trade_stats()

    if stats["total_sells"] < AGENT_MIN_TRADES:
        send_telegram(
            f"🤖 <b>Agent Analysis</b>\n"
            f"Not enough completed trades yet.\n"
            f"Completed sells: {stats['total_sells']}/{AGENT_MIN_TRADES} needed\n"
            f"Next analysis in {AGENT_INTERVAL_HOURS}h"
        )
        log.info(f"Agent skipped — only {stats['total_sells']} sells so far")
        return

    prompt = f"""
You are analyzing a XRP/USDT grid trading bot. Current settings and performance:

CURRENT SETTINGS:
- Grid lower: ${trader.grid_lower}
- Grid upper: ${trader.grid_upper}
- Number of levels: {trader.grid_levels_count}
- Order size: ${trader.order_size} USDT
- Max catch-up zones on restart: {trader.max_catchup_zones}
- Current price: ${current_price}
- Sell logic: each buy sells at exactly one full grid step above entry price

PERFORMANCE STATISTICS:
- Total buys: {stats['total_buys']}
- Total completed sells: {stats['total_sells']}
- Win rate: {stats['win_rate_pct']}%
- Total profit: ${stats['total_profit']}
- Average profit per trade: ${stats['avg_profit']}
- Best trade: ${stats['max_profit']}
- Worst trade: ${stats['min_profit']}
- Average hold time: {stats['avg_hold_mins']} minutes
- Most active zones: {stats['most_active_zones']}
- Recent 6h price range: ${stats['recent_low']} - ${stats['recent_high']}
- Recent 6h volatility: ${round(stats['recent_high'] - stats['recent_low'], 4)}

CATCH-UP TRADE PERFORMANCE:
- Total catch-up trades completed: {stats['catchup_total']}
- Total catch-up profit: ${stats['catchup_profit']}
- Average catch-up profit: ${stats['catchup_avg_profit']}

CONSTRAINTS:
- Grid lower must be between $0.50 and current price
- Grid upper must be between current price and $5.00
- Levels must be between 10 and 50
- Order size must be between $0.50 and $10
- Max catch-up zones must be between 1 and 10
- Next analysis interval must be between 1 and 6 hours

Respond with ONLY this JSON, no other text:
{{
    "new_lower": <float>,
    "new_upper": <float>,
    "new_levels": <int>,
    "new_order_size": <float>,
    "new_max_catchup_zones": <int>,
    "next_interval_hours": <float>,
    "reasoning": "<clear explanation max 3 sentences>",
    "changes_needed": <true or false>
}}
"""

    log.info("Calling Groq LLM for analysis...")
    response = call_groq(prompt)

    if not response:
        send_telegram("⚠️ Agent analysis failed — LLM unavailable. Will retry next cycle.")
        return

    try:
        decision = json.loads(response)
        required = ["new_lower", "new_upper", "new_levels", "new_order_size",
                    "new_max_catchup_zones", "next_interval_hours",
                    "reasoning", "changes_needed"]
        if not all(k in decision for k in required):
            raise ValueError("Missing required fields in LLM response")

        decision["old_lower"]       = trader.grid_lower
        decision["old_upper"]       = trader.grid_upper
        decision["old_levels"]      = trader.grid_levels_count
        decision["old_order_size"]  = trader.order_size
        decision["old_max_catchup"] = trader.max_catchup_zones
        decision["new_max_catchup"] = decision["new_max_catchup_zones"]

        AGENT_INTERVAL_HOURS = decision["next_interval_hours"]

        if not decision["changes_needed"]:
            send_telegram(
                f"🤖 <b>Agent Analysis Complete</b>\n\n"
                f"<b>Decision: No changes needed</b>\n\n"
                f"<b>Reasoning:</b>\n{decision['reasoning']}\n\n"
                f"📊 {stats['total_sells']} trades | "
                f"{stats['win_rate_pct']}% win rate | "
                f"${stats['total_profit']} profit\n"
                f"🔄 Catch-up: {stats['catchup_total']} trades | "
                f"${stats['catchup_profit']} profit\n\n"
                f"Next analysis in {AGENT_INTERVAL_HOURS}h"
            )
            log_agent_decision(decision, applied=False, rejected=False)
            return

        apply_time = datetime.now() + timedelta(minutes=AGENT_APPROVAL_MINS)
        log.info(f"[AGENT] Setting pending_changes to apply at {apply_time} with decision: {decision}")
        with pending_lock:
            pending_changes = {"decision": decision, "apply_at": apply_time}

        send_telegram(
            f"🤖 <b>Agent Recommends Changes</b>\n\n"
            f"<b>Reasoning:</b>\n{decision['reasoning']}\n\n"
            f"<b>Proposed Changes:</b>\n"
            f"Grid: ${decision['new_lower']} - ${decision['new_upper']}\n"
            f"  (was ${decision['old_lower']} - ${decision['old_upper']})\n"
            f"Levels: {decision['new_levels']} (was {decision['old_levels']})\n"  # No $ sign here
            f"Order Size: ${decision['new_order_size']} (was ${decision['old_order_size']})\n"
            f"Max Catch-up: {decision['new_max_catchup_zones']} (was {decision['old_max_catchup']})\n"
            f"Next analysis: {decision['next_interval_hours']}h\n\n"
            f"📊 {stats['total_sells']} trades | {stats['win_rate_pct']}% win rate | ${stats['total_profit']} profit\n\n"
            f"⏳ <b>Applying in {AGENT_APPROVAL_MINS} minutes</b>\n"
            f"Send /apply to apply now, or /reject to cancel."
        )
        log.info(f"Agent proposed changes — applying at {apply_time}")

    except (json.JSONDecodeError, ValueError, KeyError) as e:
        log.error(f"Agent parse error: {e} | Response: {response}")
        send_telegram(f"⚠️ Agent error — could not parse LLM response.\n{e}")

def apply_pending_changes(trader):
    global pending_changes, GRID_LOWER, GRID_UPPER, GRID_LEVELS
    global ORDER_SIZE, MAX_CATCHUP_ZONES


    try:
        with pending_lock:
            log.debug(f"[APPLY] Checking pending_changes: {pending_changes}")
            if not pending_changes:
                log.debug("[APPLY] No pending changes to apply.")
                return
            now = datetime.now()
            apply_at = pending_changes["apply_at"]
            log.info(f"[APPLY] Now: {now}, apply_at: {apply_at}")
            if now < apply_at:
                log.info("[APPLY] Not time yet. Waiting for approval window.")
                return
            decision = pending_changes["decision"]
            log.info(f"[APPLY] Applying changes: {decision}")
            pending_changes = None

        old_lower   = GRID_LOWER
        old_upper   = GRID_UPPER
        old_levels  = GRID_LEVELS
        old_size    = ORDER_SIZE
        old_catchup = MAX_CATCHUP_ZONES

        try:
            GRID_LOWER        = decision["new_lower"]
            GRID_UPPER        = decision["new_upper"]
            GRID_LEVELS       = decision["new_levels"]
            ORDER_SIZE        = decision["new_order_size"]
            MAX_CATCHUP_ZONES = decision["new_max_catchup_zones"]
            log.info("[APPLY] Updated global config variables.")
        except Exception as e:
            log.error(f"[APPLY] Error updating global config: {e}")
            send_telegram(f"⚠️ Error updating config: {e}")
            return

        try:
            new_grid_levels = calculate_grid_levels(GRID_LOWER, GRID_UPPER, GRID_LEVELS)
            trader.update_grid(new_grid_levels, GRID_LOWER, GRID_UPPER,
                               GRID_LEVELS, ORDER_SIZE, MAX_CATCHUP_ZONES)
            log.info("[APPLY] Called trader.update_grid with new settings.")
        except Exception as e:
            log.error(f"[APPLY] Error updating trader grid: {e}")
            send_telegram(f"⚠️ Error updating trader grid: {e}")
            return

        try:
            log_agent_decision(decision, applied=True)
            _emit_event(
                "agent.decision_applied",
                old_lower=old_lower,
                old_upper=old_upper,
                old_levels=old_levels,
                old_order_size=old_size,
                old_max_catchup=old_catchup,
                new_lower=GRID_LOWER,
                new_upper=GRID_UPPER,
                new_levels=GRID_LEVELS,
                new_order_size=ORDER_SIZE,
                new_max_catchup=MAX_CATCHUP_ZONES,
            )
        except Exception as e:
            log.error(f"[APPLY] Error logging agent decision: {e}")

        try:
            send_telegram(
                f"✅ <b>Agent Changes Applied</b>\n\n"
                f"Grid: ${old_lower}-${old_upper} → ${GRID_LOWER}-${GRID_UPPER}\n"
                f"Levels: {old_levels} → {GRID_LEVELS}\n"
                f"Order Size: ${old_size} → ${ORDER_SIZE}\n"
                f"Max Catch-up: {old_catchup} → {MAX_CATCHUP_ZONES}\n\n"
                f"Bot continuing with new settings."
            )
        except Exception as e:
            log.error(f"[APPLY] Error sending Telegram notification: {e}")

        log.info("Agent changes applied")
    except Exception as e:
        log.error(f"[APPLY] Unexpected error in apply_pending_changes: {e}")
        send_telegram(f"⚠️ Unexpected error in apply_pending_changes: {e}")

# ============================================================
# TELEGRAM COMMAND LISTENER
# ============================================================

def telegram_listener(trader, exchange, start_offset):
    global last_command_time
    log.info("Telegram command listener started")
    offset = start_offset

    while not stop_flag.is_set():
        try:
            updates = get_telegram_updates(offset)
            if not updates.get("ok"):
                time.sleep(5)
                continue

            should_stop = False
            for update in updates.get("result", []):
                offset  = update["update_id"] + 1
                message = update.get("message", {})
                chat_id = str(message.get("chat", {}).get("id", ""))

                if chat_id != TELEGRAM_CHAT_ID:
                    log.warning("Blocked unauthorized command from: %s", chat_id)
                    continue

                now = time.time()
                if now - last_command_time < CMD_COOLDOWN_SECS:
                    continue
                last_command_time = now

                text = message.get("text", "").strip().lower()
                log.info("Command received: %s", text)
                should_stop = _dispatch_command(text, trader, exchange)
                if should_stop:
                    break

            if should_stop:
                break
            time.sleep(1)

        except Exception as e:
            log.error("Listener error: %s", e)
            time.sleep(5)

    log.info("Telegram listener stopped")


# ---- telegram command handlers -----------------------------------------------

def _dispatch_command(text, trader, exchange):
    """Route a Telegram command text to the appropriate handler.
    Returns True if the listener loop should stop."""
    _metrics_inc("telegram.commands.total")
    _emit_event("telegram.command", command=text)
    if text == "/stop":
        _cmd_stop()
        return True
    if text.startswith("/testmode"):
        _cmd_testmode(text, trader, exchange)

    handlers = {
        "/help": _cmd_help,
        "/summary": _cmd_summary,
        "/analytics": _cmd_analytics,
        "/metrics": _cmd_metrics,
        "/export": _cmd_export,
        "/trades": _cmd_trades,
        "/status": lambda: _cmd_status(trader, exchange),
        "/grid": lambda: _cmd_grid(trader),
        "/agent": lambda: _cmd_agent(trader, exchange),
        "/apply": lambda: _cmd_apply(trader),
        "/reject": _cmd_reject,
    }
    handler = handlers.get(text)
    if handler:
        handler()
    return False


def _cmd_help():
    send_telegram(
        "🤖 <b>Available Commands</b>\n\n"
        "<b>Monitoring:</b>\n"
        "/status — price, balances, P&L\n"
        "/summary — quick performance summary\n"
        "/analytics — detailed analytics dashboard\n"
        "/metrics — runtime health counters\n"
        "/trades — last 5 trades\n"
        "/grid — grid levels and zone states\n\n"
        "<b>Control:</b>\n"
        "/agent — trigger manual agent analysis\n"
        "/testmode on — enable test mode\n"
        "/testmode run — run one test simulation now\n"
        "/testmode off — disable test mode\n"
        "/apply — apply pending agent changes now\n"
        "/reject — reject pending agent changes\n"
        "/stop — safely stop the bot\n\n"
        "<b>Data:</b>\n"
        "/export — download CSV of all trades\n"
        "/help — this message"
    )


def _cmd_metrics():
    m = _metrics_snapshot()
    started_at = m.get("started_at", "n/a")
    msg = "📡 <b>Runtime Metrics</b>\n\n"
    msg += f"Started: {started_at}\n"
    msg += f"Commands: {m.get('telegram.commands.total', 0)}\n"
    msg += f"Telegram sent: {m.get('telegram.messages.sent', 0)}\n"
    msg += f"Telegram errors: {m.get('telegram.errors', 0)}\n"
    msg += f"Trades logged: {m.get('trades.logged.total', 0)}\n"
    msg += (
        f"  BUY: {m.get('trades.logged.buy', 0)} | "
        f"SELL: {m.get('trades.logged.sell', 0)}\n"
    )
    msg += f"Reconcile cycles: {m.get('reconcile.cycles', 0)}\n"
    msg += f"Reconcile inserted: {m.get('reconcile.inserted', 0)}\n"
    msg += f"Reconcile updated: {m.get('reconcile.updated', 0)}\n"
    msg += f"Reconcile processed: {m.get('reconcile.processed', 0)}\n"
    msg += f"Reconcile errors: {m.get('reconcile.errors', 0)}\n"
    msg += f"Main-loop iterations: {m.get('bot.iterations', 0)}\n"
    msg += f"Main-loop errors: {m.get('bot.errors', 0)}"
    send_telegram(msg)


def _cmd_status(trader, exchange):
    try:
        current_price = get_price(exchange)
        send_telegram(trader.status_report(current_price))
    except Exception as e:
        send_telegram(f"⚠️ Could not fetch price: {e}")


def _cmd_summary():
    try:
        summary = get_performance_summary()
        stats = get_trade_stats()
        msg = "📈 <b>Quick Summary</b>\n\n"
        msg += summary + "\n\n"
        msg += "<b>Today's Stats:</b>\n"
        msg += f"  Trades: {stats['total_sells']}\n"
        msg += f"  Avg Profit: ${stats['avg_profit']:.4f}\n"
        msg += f"  Best: ${stats['max_profit']:.4f}\n"
        msg += f"  Worst: ${stats['min_profit']:.4f}\n"
        send_telegram(msg)
    except Exception as e:
        send_telegram(f"⚠️ Summary error: {e}")


def _cmd_analytics():
    try:
        stats = get_trade_stats()
        advanced = get_advanced_stats()
        catchup = get_catchup_stats()

        msg_parts = [
            _analytics_overall_block(stats, advanced),
            _analytics_best_hours_block(advanced),
            _analytics_worst_hours_block(advanced),
            _analytics_zone_block(advanced),
            _analytics_hold_time_block(advanced),
            _analytics_daily_block(advanced),
            _analytics_catchup_block(catchup),
        ]
        send_telegram("".join(msg_parts))
    except Exception as e:
        log.error("Analytics error: %s", e)
        send_telegram(f"⚠️ Analytics error: {e}")


def _analytics_overall_block(stats, advanced):
    pnl_emoji = "🟢" if stats['total_profit'] > 0 else "🔴"
    msg = "📊 <b>Advanced Analytics</b>\n"
    msg += "━━━━━━━━━━━━━━━━━━━━\n\n"
    msg += "<b>Overall Performance</b>\n"
    msg += f"{pnl_emoji} Total Profit: ${stats['total_profit']:.4f}\n"
    msg += f"📈 Profit Factor: {advanced['profit_factor']}\n"
    msg += (
        f"🎯 Win Rate: {stats['win_rate_pct']:.1f}%"
        f" ({stats['winning_trades']}/{stats['total_sells']})\n"
    )
    msg += f"💰 Gross Profit: ${advanced['gross_profit']:.4f}\n"
    msg += f"📉 Gross Loss: ${advanced['gross_loss']:.4f}\n\n"
    return msg


def _analytics_best_hours_block(advanced):
    msg = "<b>⏰ Best Trading Hours</b>\n"
    if advanced['best_hours']:
        for hour, avg_pnl, trades in advanced['best_hours'][:3]:
            msg += f"  {hour}:00 - ${avg_pnl:.4f} avg ({trades} trades)\n"
    else:
        msg += "  Not enough data yet\n"
    msg += "\n"
    return msg


def _analytics_worst_hours_block(advanced):
    if not advanced['worst_hours']:
        return ""
    msg = "<b>⚠️ Worst Trading Hours</b>\n"
    for hour, avg_pnl, trades in advanced['worst_hours'][:2]:
        msg += f"  {hour}:00 - ${avg_pnl:.4f} avg ({trades} trades)\n"
    msg += "\n"
    return msg


def _analytics_zone_block(advanced):
    msg = "<b>🎯 Top Performing Zones</b>\n"
    if not advanced['zone_performance']:
        return msg + "  Not enough data yet\n\n"
    sorted_zones = sorted(
        advanced['zone_performance'],
        key=lambda x: x[3] if x[3] else 0,
        reverse=True,
    )[:5]
    for zone, wins, total, avg in sorted_zones:
        win_rate = wins / total * 100 if total > 0 else 0
        msg += f"  Zone {zone}: {win_rate:.0f}% win | ${avg:.4f} avg ({total} trades)\n"
    msg += "\n"
    return msg


def _analytics_hold_time_block(advanced):
    if not advanced['hold_times']:
        return ""
    avg_hold = sum(h[1] for h in advanced['hold_times']) / len(advanced['hold_times'])
    return (
        "<b>⏱️ Average Hold Time</b>\n"
        f"  {avg_hold:.1f} minutes across zones\n\n"
    )


def _analytics_daily_block(advanced):
    msg = "<b>📅 Last 7 Days</b>\n"
    if advanced['daily_breakdown']:
        for day, trades, pnl in advanced['daily_breakdown'][:5]:
            day_emoji = "🟢" if pnl > 0 else "🔴"
            msg += f"  {day_emoji} {day}: ${pnl:.4f} ({trades} trades)\n"
    else:
        msg += "  No recent data\n"
    msg += "\n"
    return msg


def _analytics_catchup_block(catchup):
    msg = "<b>🔄 Catch-up Trades</b>\n"
    msg += f"  Total: {catchup['total']}\n"
    msg += f"  Profit: ${catchup['total_profit']:.4f}\n"
    msg += f"  Avg: ${catchup['avg_profit']:.4f}\n"
    return msg


def _cmd_export():
    try:
        filepath = export_performance_csv()
        if filepath:
            with open(filepath, 'rb') as f:
                requests.post(
                    f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument",
                    data={"chat_id": TELEGRAM_CHAT_ID},
                    files={"document": f},
                )
            send_telegram(
                "📁 <b>Performance data exported</b>\n"
                "Open in Excel or Google Sheets for deeper analysis."
            )
        else:
            send_telegram("⚠️ Export failed - check logs")
    except Exception as e:
        log.error("Export error: %s", e)
        send_telegram(f"⚠️ Export error: {e}")


def _cmd_trades():
    rows = get_last_trades(5)
    if not rows:
        send_telegram("📭 No trades recorded yet.")
        return
    msg = "📋 <b>Last 5 Trades</b>\n\n"
    for row in rows:
        ts, side, price, amount, pnl, zone, target = row
        time_str = ts[11:19]
        emoji    = "🟢" if side == "BUY" else "🔴"
        if side == "SELL":
            detail = f"P&L: ${pnl:.4f}"
        else:
            detail = f"sell target: ${target:.4f}" if target else "open"
        msg += (
            f"{emoji} {side} @ ${price:.4f}\n"
            f"   {amount:.2f} XRP | Zone {zone} | {detail}\n"
            f"   {time_str}\n\n"
        )
    send_telegram(msg)


def _cmd_grid(trader):
    msg = (
        f"📐 <b>Grid Levels</b>\n"
        f"Range: ${GRID_LOWER} - ${GRID_UPPER}\n"
        f"Step: ${trader.step_size:.4f}\n"
        f"Max Catch-up Zones: {trader.max_catchup_zones}\n\n"
    )
    open_zone_count = 0
    for i in range(trader.num_zones):
        lower = trader.grid_levels[i]
        upper = trader.grid_levels[i + 1]
        if trader.zone_holding[i]:
            open_zone_count += 1
            tag   = "🔄" if trader.zone_is_catchup[i] else "🔒"
            buy_p = trader.zone_buy_price[i]
            tgt   = trader.zone_sell_target[i]
            state = f"{tag} HOLDING bought ${buy_p:.4f} → sell ${tgt:.4f}"
        else:
            state = "⬜ empty"
        msg += f"Zone {i}: ${lower:.4f}-${upper:.4f} {state}\n"

    db_open = get_open_trades_from_db(limit=10)
    if db_open:
        msg += "\n📂 <b>Open Trades (DB)</b>\n"
        for t in db_open:
            ts = t["timestamp"][11:19] if t["timestamp"] else "--:--:--"
            tgt_text = (
                f"${t['sell_target']:.4f}"
                if t["sell_target"] and t["sell_target"] > 0
                else "n/a"
            )
            msg += (
                f"🔓 Zone {t['zone']}: buy ${t['price']:.4f} "
                f"→ target {tgt_text} ({ts})\n"
            )

    if open_zone_count == 0 and not db_open:
        msg += "\nℹ️ No open trades currently."
    send_telegram(msg)


def _cmd_agent(trader, exchange):
    try:
        current_price = get_price(exchange)
        send_telegram("🤖 Manual agent analysis triggered...")
        threading.Thread(
            target=run_agent,
            args=(trader, current_price),
            daemon=True,
        ).start()
    except Exception as e:
        send_telegram(f"⚠️ Could not trigger agent: {e}")


def _cmd_testmode(text, trader, exchange):
    global TEST_MODE_ENABLED
    if text == "/testmode on":
        TEST_MODE_ENABLED = True
        current_price = get_price(exchange)
        queued, msg = queue_simulated_agent_change(trader, current_price, approval_seconds=30)
        send_telegram(
            "🧪 <b>Test mode enabled</b>\n"
            "Agent and status now report TEST mode.\n"
            + ("Simulation queued." if queued else msg)
        )
        log.info("Test mode enabled by user")
    elif text == "/testmode run":
        if not TEST_MODE_ENABLED:
            send_telegram("ℹ️ Test mode is OFF. Use /testmode on first.")
        else:
            current_price = get_price(exchange)
            queued, msg = queue_simulated_agent_change(trader, current_price, approval_seconds=15)
            send_telegram("🧪 One-shot test simulation queued." if queued else f"ℹ️ {msg}")
    elif text == "/testmode off":
        TEST_MODE_ENABLED = False
        send_telegram("✅ <b>Test mode disabled</b>\nBot returned to LIVE mode.")
        log.info("Test mode disabled by user")
    else:
        send_telegram("ℹ️ Usage:\n/testmode on\n/testmode run\n/testmode off")


def _cmd_apply(trader):
    global pending_changes
    should_apply_now = False
    with pending_lock:
        if pending_changes:
            pending_changes["apply_at"] = datetime.now()
            should_apply_now = True
            log.info("Agent changes set to apply immediately by user command")
        else:
            send_telegram("ℹ️ No pending changes to apply.")
    if should_apply_now:
        send_telegram("⚡ Applying pending agent changes now...")
        apply_pending_changes(trader)


def _cmd_reject():
    global pending_changes
    with pending_lock:
        if pending_changes:
            pending_changes = None
            send_telegram("❌ <b>Pending changes rejected.</b>\nGrid settings unchanged.")
            log.info("Agent changes rejected by user")
        else:
            send_telegram("ℹ️ No pending changes to reject.")


def _cmd_stop():
    send_telegram(
        "🛑 <b>Stop command received</b>\n"
        "Finishing current cycle then shutting down..."
    )
    stop_flag.set()

# ============================================================
# EXCHANGE
# ============================================================

def get_exchange():
    # Use the centralized ExchangeAdapter for resilient CCXT calls
    try:
        adapter = ExchangeAdapter(
            exchange_id=EXCHANGE_ID,
            api_key=API_KEY,
            secret=API_SECRET,
            enable_rate_limit=True,
            options={"defaultType": "swap"}
        )
        return adapter
    except ccxt.AuthenticationError as e:
        err = str(e)
        if any(kw in err.lower() for kw in ("ip", "whitelist", "not allowed", "30002")):
            raise SystemExit(
                "ERROR: Phemex rejected this connection because your current IP address "
                "is not on the API key whitelist.\n"
                "Fix: Log in to Phemex → API Management → edit your API key → "
                "either disable IP restriction or add your phone's IP address.\n"
                f"Original error: {e}"
            ) from e
        raise SystemExit(f"ERROR: Phemex authentication failed: {e}") from e
    except Exception as e:
        log.error(f"Could not create ExchangeAdapter: {e}; falling back to ccxt direct client")
        return ccxt.phemex({
            "apiKey": API_KEY,
            "secret": API_SECRET,
            "enableRateLimit": True,
            "options": {"defaultType": "swap"}
        })

def get_price(exchange):
    if TEST_MODE_ENABLED:
        return get_simulated_price()
    ticker = exchange.fetch_ticker(SYMBOL)
    return ticker["last"]


def normalize_remote_order(o):
    return {
        "id": o.get("id") or o.get("orderId") or o.get("clientOrderId"),
        "symbol": o.get("symbol"),
        "side": o.get("side"),
        "price": float(o.get("price") or 0),
        "amount": float(o.get("amount") or o.get("remaining") or 0),
        "filled": float(o.get("filled") or 0),
        "status": o.get("status")
    }


def find_oldest_open_buy_price(zone, limit=1000):
    """Return the price of the oldest open BUY trade for a zone (or None)."""
    try:
        open_trades = get_open_trades_from_db(limit=limit)
        buys = [t for t in open_trades if t.get('zone') == zone]
        if not buys:
            return None
        oldest = min(buys, key=lambda t: t['id'])
        return oldest.get('price')
    except Exception:
        return None


# ---- reconcile helpers -------------------------------------------------------

def _reconcile_build_adapter(adapter, exchange_id):
    """Return an existing adapter or create a new one; returns None on failure."""
    if adapter is not None:
        return adapter
    try:
        api_key = (
            os.getenv(f"{exchange_id.upper()}_API_KEY") or os.getenv("API_KEY")
        )
        api_secret = (
            os.getenv(f"{exchange_id.upper()}_API_SECRET") or os.getenv("API_SECRET")
        )
        return ExchangeAdapter(exchange_id, api_key=api_key, secret=api_secret)
    except Exception as e:
        log.error("Reconciliation: could not create adapter for %s: %s", exchange_id, e)
        return None


def _reconcile_sync_remote_orders(remote_orders, local_exchange_id):
    """Upsert remote open orders into local DB; returns inserted count."""
    inserted = 0
    for ro in remote_orders:
        o = normalize_remote_order(ro)
        if not o["id"]:
            continue
        existing = orders_db.find_order_by_exchange_id(local_exchange_id, o["id"])
        if existing:
            orders_db.update_order_by_exchange_id(
                local_exchange_id, o["id"],
                filled=o["filled"], status=o["status"],
            )
        else:
            orders_db.insert_order(
                local_exchange_id, o["id"], o["symbol"], o["side"],
                o["price"], o["amount"], status=o["status"] or "open",
            )
            inserted += 1
    return inserted


def _reconcile_update_db_open(adapter, db_open, local_exchange_id):
    """Refresh each DB-open order with latest exchange state; returns updated count."""
    updated = 0
    for row in db_open:
        exch_id = row["exchange_order_id"]
        try:
            remote = adapter.fetch_order(exch_id, row["symbol"] or None)
            if remote:
                norm = normalize_remote_order(remote)
                orders_db.update_order_by_exchange_id(
                    local_exchange_id, exch_id,
                    filled=norm["filled"], status=norm["status"],
                )
                updated += 1
        except Exception as e:
            log.debug("Reconciliation: could not fetch order %s: %s", exch_id, e)
    return updated


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

def reconcile_once(adapter=None, symbol=None, exchange_id=None, trader=None):
    """Fetch open orders from exchange and ensure local orders table matches."""
    if exchange_id is None:
        exchange_id = EXCHANGE_ID

    orders_db.ensure_orders_table()

    adapter = _reconcile_build_adapter(adapter, exchange_id)
    if adapter is None:
        return

    local_exchange_id = getattr(adapter, "exchange_id", exchange_id)

    _metrics_inc("reconcile.cycles")
    try:
        remote_orders = adapter.fetch_open_orders(symbol)
    except Exception as e:
        _metrics_inc("reconcile.errors")
        _emit_event(
            "reconcile.fetch_open_orders_error",
            level="warning",
            exchange_id=local_exchange_id,
            error=str(e),
        )
        log.error("Reconciliation: failed to fetch open orders: %s", e)
        remote_orders = []

    inserted = _reconcile_sync_remote_orders(remote_orders, local_exchange_id)
    _metrics_inc("reconcile.inserted", inserted)
    log.info("Reconciliation: inserted %d missing orders from %s", inserted, local_exchange_id)

    db_open = orders_db.get_open_orders_from_db(local_exchange_id)
    updated = _reconcile_update_db_open(adapter, db_open, local_exchange_id)
    _metrics_inc("reconcile.updated", updated)
    log.info("Reconciliation: refreshed %d local open orders against %s", updated, local_exchange_id)

    try:
        unproc = orders_db.get_unprocessed_orders(local_exchange_id)
    except Exception:
        unproc = []
    processed_count = _reconcile_process_unprocessed(adapter, unproc, local_exchange_id, trader)
    _metrics_inc("reconcile.processed", processed_count)
    _emit_event(
        "reconcile.cycle",
        exchange_id=local_exchange_id,
        symbol=symbol,
        fetched_open=len(remote_orders),
        inserted=inserted,
        updated=updated,
        processed=processed_count,
    )
    if processed_count:
        log.info("Reconciliation: logged %d filled orders for %s", processed_count, local_exchange_id)


def reconcile_worker(adapter=None, symbol=None, exchange_id=None, trader=None, interval=RECONCILE_INTERVAL_SECONDS):
    log.info(f"Reconciliation worker starting (interval={interval}s)")
    while not stop_flag.wait(interval):
        try:
            reconcile_once(adapter, symbol, exchange_id, trader)
        except Exception as e:
            log.error(f"Reconciliation worker error: {e}")

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

class PaperTrader:
    def __init__(self, balance, grid_levels):
        self.usdt_balance      = balance
        self.xrp_balance       = 0.0
        self.total_trades      = 0
        self.total_profit      = 0.0
        self.total_fees_paid   = 0.0  # NEW: Fee tracking
        self.start_balance     = balance
        self.grid_levels       = grid_levels
        self.num_zones         = len(grid_levels) - 1
        self.step_size         = grid_levels[1] - grid_levels[0]
        self.min_move          = self.step_size * MIN_PROFIT_RATIO
        self.zone_holding      = [False] * self.num_zones
        self.zone_buy_price    = [0.0]  * self.num_zones
        self.zone_sell_target  = [0.0]  * self.num_zones  # buy + one step
        self.zone_is_catchup   = [False] * self.num_zones
        self.grid_lower        = GRID_LOWER
        self.grid_upper        = GRID_UPPER
        self.grid_levels_count = GRID_LEVELS
        self.order_size        = ORDER_SIZE
        self.max_catchup_zones = MAX_CATCHUP_ZONES

        log.info(f"Grid step: ${self.step_size:.4f} | "
                 f"Min sell move: ${self.min_move:.4f}")

    def restore_open_trades_from_db(self):
        """Restore open trades and order mappings into in-memory grid state on restart."""
        self._restore_from_trades_table()
        exchange_id = getattr(getattr(self, 'exchange', None), 'exchange_id', EXCHANGE_ID)
        try:
            mappings = orders_db.get_mappings_by_exchange(exchange_id)
            self._restore_from_order_mappings(exchange_id, mappings)
        except Exception as e:
            log.debug("Mapping restore skipped or failed: %s", e)

    def _restore_from_trades_table(self):
        """First-pass restore: populate zone state from the trades table."""
        open_trades = get_open_trades_from_db(limit=100)
        restored = 0
        for t in open_trades:
            zone = t["zone"]
            if zone is None or zone < 0 or zone >= self.num_zones:
                continue
            price = t["price"]
            sell_target = t["sell_target"] or round(price + self.step_size, 4)
            self.zone_holding[zone] = True
            self.zone_buy_price[zone] = price
            self.zone_sell_target[zone] = sell_target
            self.zone_is_catchup[zone] = False
            restored += 1
        log.info("Restored %d open trades from DB into grid state.", restored)

    def _parse_mapping_trade_context(self, mapping):
        trade_row = orders_db.get_trade_by_id(mapping['trade_id'])
        if not trade_row:
            return None
        side = (mapping['side'] or '').upper() if mapping.get('side') else ''
        price = (
            float(trade_row['price'])
            if trade_row and trade_row['price'] is not None
            else float(mapping.get('price') or 0)
        )
        zone = trade_row['grid_level'] if 'grid_level' in trade_row.keys() else None
        if zone is None:
            try:
                zone = get_current_zone(price, self.grid_levels)
            except Exception:
                zone = None
        if zone is None or zone < 0 or zone >= self.num_zones:
            return None
        return trade_row, side, price, zone

    def _apply_mapping_to_zone_state(self, trade_row, side, price, zone):
        if side == 'BUY' and not self.zone_holding[zone]:
            self.zone_holding[zone] = True
            self.zone_buy_price[zone] = price
            st = trade_row['sell_target'] if 'sell_target' in trade_row.keys() else None
            self.zone_sell_target[zone] = st or round(price + self.step_size, 4)
            notes = trade_row['notes'] if 'notes' in trade_row.keys() else ''
            self.zone_is_catchup[zone] = bool(notes and 'catch' in notes.lower())
            return 1
        if side == 'SELL' and self.zone_holding[zone]:
            self.zone_holding[zone] = False
            self.zone_buy_price[zone] = 0.0
            self.zone_sell_target[zone] = 0.0
            self.zone_is_catchup[zone] = False
            return 1
        return 0

    def _restore_from_order_mappings(self, exchange_id, mappings):
        """Second-pass restore: fill gaps and clear sold zones using order→trade map."""
        mapping_restored = 0
        for m in mappings:
            try:
                parsed = self._parse_mapping_trade_context(m)
                if parsed is None:
                    continue
                trade_row, side, price, zone = parsed
                mapping_restored += self._apply_mapping_to_zone_state(
                    trade_row, side, price, zone
                )
                try:
                    orders_db.update_order_by_exchange_id(
                        exchange_id, m['exchange_order_id'], processed=1
                    )
                except Exception:
                    pass
            except Exception:
                continue
        if mapping_restored:
            log.info(
                "Restored %d zones from order->trade mappings for %s.",
                mapping_restored, exchange_id,
            )

    def update_grid(self, new_grid_levels, lower, upper,
                    levels, order_size, max_catchup):
        self.grid_levels       = new_grid_levels
        self.num_zones         = len(new_grid_levels) - 1
        self.step_size         = new_grid_levels[1] - new_grid_levels[0]
        self.min_move          = self.step_size * MIN_PROFIT_RATIO
        self.zone_holding      = [False] * self.num_zones
        self.zone_buy_price    = [0.0]  * self.num_zones
        self.zone_sell_target  = [0.0]  * self.num_zones
        self.zone_is_catchup   = [False] * self.num_zones
        self.grid_lower        = lower
        self.grid_upper        = upper
        self.grid_levels_count = levels
        self.order_size        = order_size
        self.max_catchup_zones = max_catchup
        log.info(f"Grid updated: ${lower}-${upper} "
                 f"{levels} levels | max catchup: {max_catchup}")

    def initialize_zones(self, current_price, trend='SIDEWAYS'):
        current_zone           = get_current_zone(current_price, self.grid_levels)
        self.zone_holding      = [False] * self.num_zones
        self.zone_buy_price    = [0.0]  * self.num_zones
        self.zone_sell_target  = [0.0]  * self.num_zones
        self.zone_is_catchup   = [False] * self.num_zones

        # NEW: Trend-aware initialization
        if TREND_BIAS_ENABLED:
            self._open_initial_position_trend_aware(current_price, current_zone, trend)
        else:
            self._open_initial_position(current_price, current_zone)

        send_telegram(
            f"🤖 <b>Grid Bot + Agent Started</b>\n"
            f"Pair: {SYMBOL}\n"
            f"Mode: {'LIVE' if EXECUTE_LIVE else 'Paper Trading'}\n"
            f"Runtime Mode: {get_runtime_mode_label()}\n"
            f"Balance: ${PAPER_BALANCE} USDT\n"
            f"Grid: ${GRID_LOWER} - ${GRID_UPPER}\n"
            f"Levels: {GRID_LEVELS} | Step: ${self.step_size:.4f}\n"
            f"Order Size: ${ORDER_SIZE} USDT\n"
            f"Min Move to Sell: ${self.min_move:.4f}\n"
            f"Max Catch-up Zones: {self.max_catchup_zones}\n"
            f"Starting Price: ${current_price:.4f}\n"
            f"Starting Zone: {current_zone}/{self.num_zones - 1}\n"
            f"Trend: {trend}\n"
            f"Sell target: ${round(current_price + self.step_size, 4)}\n"
            f"Agent analysis every {AGENT_INTERVAL_HOURS}h\n\n"
            f"<i>Commands: /status /summary /analytics /trades /grid /agent /testmode on|run|off /apply /reject /export /stop /help</i>"
        )

    def _open_initial_position(self, price, zone):
        if self.usdt_balance < self.order_size:
            log.warning("Insufficient balance for initial position")
            return
        xrp_amount                      = self.order_size / price
        sell_target                     = round(price + self.step_size, 4)
        self.usdt_balance              -= self.order_size
        self.xrp_balance               += xrp_amount
        self.zone_holding[zone]         = True
        self.zone_buy_price[zone]       = price
        self.zone_sell_target[zone]     = sell_target
        self.zone_is_catchup[zone]      = False
        log_trade("BUY", price, xrp_amount, self.order_size, zone, 0,
                  self.portfolio_value(price), price, price, sell_target,
                  0, "initial position")
        log.info(f"Initial position: {xrp_amount:.2f} XRP "
                 f"@ ${price:.4f} sell target ${sell_target:.4f}")

    def _open_initial_position_trend_aware(self, price, current_zone, trend):
        """Open initial positions with trend bias"""
        if trend == 'BULLISH':
            # Start with more positions (expect upward movement)
            zones_to_fill = min(3, self.max_catchup_zones, current_zone + 1)
            log.info(f"Bullish trend detected - opening {zones_to_fill} initial positions")
            for i in range(current_zone, max(-1, current_zone - zones_to_fill), -1):
                if i >= 0 and self.usdt_balance >= self.order_size:
                    self._open_position_at_zone(i, price)
        elif trend == 'BEARISH':
            # Start conservative (expect downward movement)
            log.info("Bearish trend detected - opening single conservative position")
            self._open_initial_position(price, current_zone)
        else:
            # Normal grid start
            log.info("Sideways trend - normal grid initialization")
            self._open_initial_position(price, current_zone)

    def _open_position_at_zone(self, zone, price):
        """Helper to open position at specific zone"""
        if self.usdt_balance < self.order_size:
            return
        xrp_amount = self.order_size / price
        sell_target = round(price + self.step_size, 4)
        self.usdt_balance -= self.order_size
        self.xrp_balance += xrp_amount
        self.zone_holding[zone] = True
        self.zone_buy_price[zone] = price
        self.zone_sell_target[zone] = sell_target
        self.zone_is_catchup[zone] = False
        log_trade("BUY", price, xrp_amount, self.order_size, zone, 0,
                  self.portfolio_value(price), price, price, sell_target,
                  0, "trend-biased initial")

    # NEW: Grid rebalancing
    def check_grid_rebalance(self, current_price):
        """Auto-rebalance grid when price drifts too far"""
        if not AUTO_REBALANCE_ENABLED:
            return False

        grid_center = (self.grid_lower + self.grid_upper) / 2
        drift_pct = abs(current_price - grid_center) / grid_center

        if drift_pct > GRID_REBALANCE_THRESHOLD:
            log.info(f"Grid rebalance triggered: {drift_pct:.1%} drift")

            # Calculate new grid centered on current price
            new_range = self.grid_upper - self.grid_lower
            new_lower = round(current_price - new_range / 2, 4)
            new_upper = round(current_price + new_range / 2, 4)

            # Ensure bounds
            new_lower = max(0.50, new_lower)
            new_upper = min(5.00, new_upper)

            new_grid_levels = calculate_grid_levels(new_lower, new_upper, self.grid_levels_count)
            self.update_grid(new_grid_levels, new_lower, new_upper,
                            self.grid_levels_count, self.order_size, self.max_catchup_zones)

            send_telegram(
                f"🔄 <b>Auto Grid Rebalance</b>\n"
                f"Drift: {drift_pct:.1%}\n"
                f"Old: ${self.grid_lower} - ${self.grid_upper}\n"
                f"New: ${new_lower} - ${new_upper}\n"
                f"Price: ${current_price:.4f}"
            )
            return True
        return False

    # NEW: Position health monitoring
    def get_position_health(self, current_price):
        """Return detailed health report of all positions"""
        health = []
        for i in range(self.num_zones):
            if not self.zone_holding[i]:
                continue

            buy_price = self.zone_buy_price[i]
            current_pnl_pct = (current_price - buy_price) / buy_price * 100
            distance_to_target = (self.zone_sell_target[i] - current_price) / current_price * 100

            status = "🟢" if current_pnl_pct > 0 else "🔴"
            health.append({
                "zone": i,
                "buy_price": buy_price,
                "current_pnl_pct": current_pnl_pct,
                "distance_to_target_pct": distance_to_target,
                "is_catchup": self.zone_is_catchup[i],
                "status": status
            })

        return sorted(health, key=lambda x: x["current_pnl_pct"])

    def check_underwater_positions(self, current_price, threshold=UNDERWATER_ALERT_THRESHOLD):
        """Alert if any position is down more than threshold %"""
        health = self.get_position_health(current_price)
        underwater = [h for h in health if h["current_pnl_pct"] < threshold]

        if underwater:
            msg = f"⚠️ <b>Underwater Positions ({len(underwater)})</b>\n\n"
            for pos in underwater:
                msg += (
                    f"{pos['status']} Zone {pos['zone']}: "
                    f"{pos['current_pnl_pct']:.1f}%\n"
                    f"  Entry: ${pos['buy_price']:.4f}\n"
                    f"  Needs: {abs(pos['distance_to_target_pct']):.1f}% to target\n"
                )
            send_telegram(msg)
            log.warning(f"{len(underwater)} positions underwater > {threshold}%")

    def safety_check(self, current_price):
        portfolio = self.portfolio_value(current_price)
        loss_pct  = ((self.start_balance - portfolio) / self.start_balance) * 100
        if loss_pct >= MAX_LOSS_PCT:
            send_telegram(
                f"🚨 <b>SAFETY STOP — Max Loss</b>\n"
                f"Portfolio dropped {loss_pct:.1f}%\n"
                f"Limit: {MAX_LOSS_PCT}%\n"
                f"Bot stopping to protect capital."
            )
            stop_flag.set()
            return False
        if self.total_trades >= MAX_TOTAL_TRADES:
            send_telegram(
                f"🚨 <b>SAFETY STOP — Max Trades</b>\n"
                f"Total trades: {self.total_trades}"
            )
            stop_flag.set()
            return False
        return True

    def check_missed_sells(self, current_price):
        """
        Catch-up sells: fire any holding zones where current price
        has already reached or exceeded the zone sell target.
        Handles missed sells from restarts or connection drops.
        """
        for i in range(self.num_zones):
            if not self.zone_holding[i]:
                continue
            sell_target = self.zone_sell_target[i]
            if current_price >= sell_target:
                log.info(f"Catch-up sell: zone {i} "
                         f"target ${sell_target:.4f} "
                         f"current ${current_price:.4f}")
                self.sell(current_price, self.order_size, i)

    def check_missed_buys(self, current_price):
        """
        Catch-up buys: find empty zones below current price missed
        while bot was offline. Buy at current price, sell target is
        buy price + one full step — guaranteed profit on recovery.
        Fills closest zones first, up to max_catchup_zones.
        """
        current_zone  = get_current_zone(current_price, self.grid_levels)
        catchup_count = 0
        caught_zones  = []

        # Check zones below current zone, closest first
        for i in range(current_zone - 1, -1, -1):
            if catchup_count >= self.max_catchup_zones:
                break
            if self.zone_holding[i]:
                continue
            if self.usdt_balance < self.order_size:
                log.warning("Insufficient USDT for catch-up buy — stopping")
                break

            xrp_amount                      = self.order_size / current_price
            sell_target                     = round(current_price + self.step_size, 4)
            self.usdt_balance              -= self.order_size
            self.xrp_balance               += xrp_amount
            self.total_trades              += 1
            self.zone_holding[i]            = True
            self.zone_buy_price[i]          = current_price
            self.zone_sell_target[i]        = sell_target
            self.zone_is_catchup[i]         = True
            catchup_count                  += 1

            # NEW: Calculate fees for catch-up (taker fee - market order)
            fees = self.order_size * TAKER_FEE_PCT
            self.total_fees_paid += fees

            log_trade("BUY", current_price, xrp_amount, self.order_size,
                      i, 0, self.portfolio_value(current_price),
                      current_price, current_price, sell_target,
                      fees, "catch-up buy")
            log_catchup_trade(i, current_price, sell_target)
            caught_zones.append(
                f"Zone {i}: entry ${current_price:.4f} "
                f"→ sell ${sell_target:.4f}"
            )
            log.info(f"Catch-up buy zone {i} @ ${current_price:.4f} "
                     f"sell target ${sell_target:.4f}")

        if caught_zones:
            send_telegram(
                f"🔄 <b>Catch-up Buys ({len(caught_zones)})</b>\n"
                f"Missed zones recovered @ ${current_price:.4f}\n\n"
                + "\n".join(caught_zones) +
                f"\n\nUSDT left: ${self.usdt_balance:.2f}\n"
                f"Each zone sells one full step above entry."
            )

    def buy(self, price, usdt_amount, zone, is_taker=False):
        if self.zone_holding[zone]:
            log.warning(f"Zone {zone} already holding — skipping")
            return False
        if sum(self.zone_holding) >= MAX_OPEN_ZONES:
            log.warning("Max open zones reached — skipping")
            return False
        if self.usdt_balance < usdt_amount:
            log.warning("Insufficient USDT — skipping")
            return False

        xrp_amount = usdt_amount / price
        sell_target = round(price + self.step_size, 4)

        # NEW: Calculate fees
        fee_rate = TAKER_FEE_PCT if is_taker else MAKER_FEE_PCT
        fees = usdt_amount * fee_rate
        self.total_fees_paid += fees

        notes = f"{'taker' if is_taker else 'maker'}"
        exchange_order_id = None
        order_status = None

        # Attempt live order placement when enabled and adapter attached
        if EXECUTE_LIVE and getattr(self, 'exchange', None):
            try:
                if is_taker:
                    order = self.exchange.create_market_order(SYMBOL, 'buy', xrp_amount)
                else:
                    order = self.exchange.create_limit_order(SYMBOL, 'buy', price, xrp_amount)

                if isinstance(order, dict):
                    exchange_order_id = order.get('id') or order.get('orderId') or order.get('clientOrderId')
                    order_status = order.get('status') or order.get('state') or 'open'
                else:
                    exchange_order_id = str(order)

                try:
                    orders_db.insert_order(getattr(self.exchange, 'exchange_id', EXCHANGE_ID),
                                            exchange_order_id or 'unknown', SYMBOL, 'BUY', price, xrp_amount,
                                            status=order_status or 'open', processed=1)
                except Exception as e:
                    log.warning(f"Could not insert exchange order into orders DB: {e}")

                notes += f";live_order={exchange_order_id}"
            except Exception as e:
                log.error(f"Live buy failed: {e}")
                notes += ";live_failed"

        # Reserve funds / update in-memory state (same as paper mode)
        self.usdt_balance          -= usdt_amount
        self.xrp_balance           += xrp_amount
        self.total_trades          += 1
        self.zone_holding[zone]     = True
        self.zone_buy_price[zone]   = price
        self.zone_sell_target[zone] = sell_target
        self.zone_is_catchup[zone]  = False

        if exchange_order_id:
            with orders_db.transaction() as tx_conn:
                trade_id = log_trade("BUY", price, xrp_amount, usdt_amount, zone, 0,
                          self.portfolio_value(price), price, price, sell_target,
                          fees, notes, conn=tx_conn, commit=False)
                orders_db.insert_order_trade_mapping(getattr(self.exchange, 'exchange_id', EXCHANGE_ID),
                                                     exchange_order_id, trade_id, 'BUY', price, xrp_amount, fees, conn=tx_conn, commit=False)
        else:
            trade_id = log_trade("BUY", price, xrp_amount, usdt_amount, zone, 0,
                      self.portfolio_value(price), price, price, sell_target,
                      fees, notes)

        header = "LIVE BUY" if EXECUTE_LIVE else "PAPER BUY"
        send_telegram(
            f"🟢 <b>{header}</b>\n"
            f"Price: ${price:.4f}\n"
            f"Amount: {xrp_amount:.2f} XRP (${usdt_amount:.2f})\n"
            f"Zone: {zone}/{self.num_zones - 1}\n"
            f"Sell target: ${sell_target:.4f}\n"
            f"Fee: ${fees:.4f}\n"
            f"Expected profit: ${round((sell_target - price) * xrp_amount - fees, 4)}\n"
            f"USDT left: ${self.usdt_balance:.2f}\n"
            f"Portfolio: ${self.portfolio_value(price):.2f}"
        )
        log.info(f"BUY zone {zone}: {xrp_amount:.2f} XRP @ ${price:.4f} "
                 f"sell target ${sell_target:.4f} | fee: ${fees:.4f} | notes: {notes}")
        return True

    def sell(self, price, usdt_amount, zone):
        if not self.zone_holding[zone]:
            log.warning(f"Zone {zone} not holding — skipping phantom sell")
            return False

        sell_target = self.zone_sell_target[zone]
        buy_price   = self.zone_buy_price[zone]

        # Only sell if price has reached the sell target
        if price < sell_target:
            log.info(f"Zone {zone} price ${price:.4f} "
                     f"below sell target ${sell_target:.4f} — holding")
            return False

        xrp_amount = usdt_amount / price
        if self.xrp_balance < xrp_amount:
            log.warning("Insufficient XRP — skipping")
            return False

        is_catchup = self.zone_is_catchup[zone]

        # NEW: Calculate fees and net profit
        buy_fee = self.order_size * MAKER_FEE_PCT
        sell_fee = usdt_amount * MAKER_FEE_PCT
        total_fee = buy_fee + sell_fee
        self.total_fees_paid += sell_fee  # Buy fee already counted

        notes = f"{'catch-up' if is_catchup else 'normal'}"
        exchange_order_id = None
        order_status = None

        # Attempt live order placement if enabled
        if EXECUTE_LIVE and getattr(self, 'exchange', None):
            try:
                # Use a limit sell at the target price to try and get maker fee
                order = self.exchange.create_limit_order(SYMBOL, 'sell', price, xrp_amount)
                if isinstance(order, dict):
                    exchange_order_id = order.get('id') or order.get('orderId') or order.get('clientOrderId')
                    order_status = order.get('status') or order.get('state') or 'open'
                else:
                    exchange_order_id = str(order)

                try:
                    orders_db.insert_order(getattr(self.exchange, 'exchange_id', EXCHANGE_ID),
                                            exchange_order_id or 'unknown', SYMBOL, 'SELL', price, xrp_amount,
                                            status=order_status or 'open', processed=1)
                except Exception as e:
                    log.warning(f"Could not insert exchange sell order into orders DB: {e}")

                notes += f";live_order={exchange_order_id}"
            except Exception as e:
                log.error(f"Live sell failed: {e}")
                notes += ";live_failed"

        # Update balances / finalize in-memory state
        self.xrp_balance           -= xrp_amount
        self.usdt_balance          += usdt_amount
        self.total_trades          += 1

        # Net profit after all fees
        gross_profit = (price - buy_price) * xrp_amount
        net_profit = gross_profit - total_fee
        self.total_profit          += net_profit

        self.zone_holding[zone]     = False
        self.zone_buy_price[zone]   = 0.0
        self.zone_sell_target[zone] = 0.0
        self.zone_is_catchup[zone]  = False

        if exchange_order_id:
            with orders_db.transaction() as tx_conn:
                trade_id = log_trade("SELL", price, xrp_amount, usdt_amount, zone, net_profit,
                          self.portfolio_value(price), price, buy_price, sell_target,
                          total_fee, notes, conn=tx_conn, commit=False)
                orders_db.insert_order_trade_mapping(getattr(self.exchange, 'exchange_id', EXCHANGE_ID),
                                                     exchange_order_id, trade_id, 'SELL', price, xrp_amount, total_fee, conn=tx_conn, commit=False)
        else:
            trade_id = log_trade("SELL", price, xrp_amount, usdt_amount, zone, net_profit,
                  self.portfolio_value(price), price, buy_price, sell_target,
                  total_fee, notes)

        if is_catchup:
            complete_catchup_trade(zone, price, net_profit)

        tag = "🔄 CATCH-UP " if is_catchup else ""
        header = f"LIVE SELL {tag}" if EXECUTE_LIVE else f"PAPER SELL {tag}"
        send_telegram(
            f"🔴 <b>{header}</b>\n"
            f"Price: ${price:.4f}\n"
            f"Amount: {xrp_amount:.2f} XRP (${usdt_amount:.2f})\n"
            f"Zone: {zone}/{self.num_zones - 1}\n"
            f"Bought at: ${buy_price:.4f}\n"
            f"Sell target was: ${sell_target:.4f}\n"
            f"Move: ${round(price - buy_price, 4)}\n"
            f"Fees: ${total_fee:.4f}\n"
            f"Net Profit: ${net_profit:.4f}\n"
            f"Total Profit: ${self.total_profit:.4f}\n"
            f"Portfolio: ${self.portfolio_value(price):.2f}"
        )
        log.info(f"SELL zone {zone}: {xrp_amount:.2f} XRP @ ${price:.4f} | "
                 f"Net Profit: ${net_profit:.4f} | Fees: ${total_fee:.4f} | Catch-up: {is_catchup} | notes: {notes}")
        return True

    def check_grid(self, current_price, last_price, exchange):
        # Catch-up sells first — handle any missed sells
        self.check_missed_sells(current_price)

        # NEW: Calculate dynamic order size based on volatility
        volatility = calculate_volatility(exchange, SYMBOL, VOLATILITY_LOOKBACK)
        dynamic_size = get_dynamic_order_size(self.order_size, volatility,
                                               VOL_THRESHOLD_LOW, VOL_THRESHOLD_HIGH) if VOLATILITY_ADJUSTMENT else self.order_size

        if dynamic_size != self.order_size:
            log.debug(f"Dynamic size adjustment: ${self.order_size} → ${dynamic_size} (vol: {volatility:.2%})")

        for i in range(self.num_zones):
            self.grid_levels[i]
            upper = self.grid_levels[i + 1]

            # Price dropped into zone from above = BUY
            if last_price >= upper and current_price < upper:
                if not self.zone_holding[i]:
                    # Use dynamic size, maker order (limit)
                    self.buy(current_price, dynamic_size, i, is_taker=False)

            # Price reached sell target for this zone = SELL
            if self.zone_holding[i]:
                if current_price >= self.zone_sell_target[i]:
                    self.sell(current_price, dynamic_size, i)

    def portfolio_value(self, current_price):
        return self.usdt_balance + (self.xrp_balance * current_price)

    def status_report(self, current_price):
        pnl     = self.portfolio_value(current_price) - self.start_balance
        pnl_pct = (pnl / self.start_balance) * 100
        holding_zones   = [i for i, s in enumerate(self.zone_holding) if s]
        holding_details = ""
        for z in holding_zones:
            current_price - self.zone_buy_price[z]
            target  = self.zone_sell_target[z]
            needed  = target - current_price
            tag     = " 🔄" if self.zone_is_catchup[z] else ""
            holding_details += (
                f"\n  Zone {z}{tag}: bought ${self.zone_buy_price[z]:.4f} "
                f"| target ${target:.4f} "
                f"| needs ${needed:.4f} more"
            )
        with pending_lock:
            pending_str = ""
            if pending_changes:
                mins_left = max(0, int(
                    (pending_changes["apply_at"] -
                     datetime.now()).total_seconds() / 60))
                pending_str = f"\n⏳ Pending agent changes in {mins_left}min"

        catchup = get_catchup_stats()
        return (
            f"📊 <b>BOT STATUS</b>\n"
            f"Mode: {get_runtime_mode_label()}\n"
            f"Price: ${current_price:.4f}\n"
            f"USDT: ${self.usdt_balance:.2f}\n"
            f"XRP: {self.xrp_balance:.2f}\n"
            f"Portfolio: ${self.portfolio_value(current_price):.2f}\n"
            f"P&L: ${pnl:.2f} ({pnl_pct:.1f}%)\n"
            f"Total Trades: {self.total_trades}\n"
            f"Net Profit: ${self.total_profit:.4f}\n"
            f"Total Fees Paid: ${self.total_fees_paid:.4f}\n"
            f"Catch-up Profit: ${catchup['total_profit']}\n"
            f"Open Zones: {len(holding_zones)}"
            f"{holding_details}"
            f"{pending_str}"
        )

# ============================================================
# MAIN BOT LOOP
# ============================================================

def _start_bot_threads(trader, exchange, start_offset):
    """Start the Telegram listener and reconciliation worker as daemon threads."""
    listener_thread = threading.Thread(
        target=telegram_listener,
        args=(trader, exchange, start_offset),
        daemon=True,
    )
    listener_thread.start()
    log.info("Telegram command listener running")

    try:
        exch_id = getattr(trader.exchange, 'exchange_id', EXCHANGE_ID)
        reconcile_thread = threading.Thread(
            target=reconcile_worker,
            args=(
                getattr(trader, 'exchange', None),
                SYMBOL,
                exch_id,
                trader,
                RECONCILE_INTERVAL_SECONDS,
            ),
            daemon=True,
        )
        reconcile_thread.start()
        log.info("Reconciliation worker running every %ds", RECONCILE_INTERVAL_SECONDS)
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


def _pause_if_outside_grid(current_price):
    if current_price < GRID_LOWER:
        send_telegram(
            f"⚠️ <b>Price Below Range</b>\n"
            f"XRP: ${current_price:.4f} | Floor: ${GRID_LOWER}\n"
            "Bot paused."
        )
        time.sleep(CHECK_INTERVAL)
        return True
    if current_price > GRID_UPPER:
        send_telegram(
            f"⚠️ <b>Price Above Range</b>\n"
            f"XRP: ${current_price:.4f} | Ceiling: ${GRID_UPPER}\n"
            "Bot paused."
        )
        time.sleep(CHECK_INTERVAL)
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
    threading.Thread(
        target=run_agent,
        args=(trader, current_price),
        daemon=True,
    ).start()
    return now


def _run_bot_iteration(
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
    if _pause_if_outside_grid(current_price):
        return last_price, status_counter, last_agent_run, False

    trader.check_grid(current_price, last_price, exchange)
    status_counter = _maybe_send_hourly_status(
        trader, current_price, status_counter, checks_per_hour
    )
    last_agent_run = _maybe_trigger_agent(trader, current_price, last_agent_run)
    time.sleep(CHECK_INTERVAL)
    return current_price, status_counter, last_agent_run, False


def _run_bot_runtime_loop(trader, exchange, starting_price):
    last_price = starting_price
    status_counter = 0
    checks_per_hour = (60 * 60) // CHECK_INTERVAL
    last_agent_run = datetime.now()

    while not stop_flag.is_set():
        try:
            last_price, status_counter, last_agent_run, should_stop = _run_bot_iteration(
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
            time.sleep(60)


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


def run_bot():
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
    trend = detect_trend(exchange, SYMBOL, TREND_LOOKBACK) if TREND_BIAS_ENABLED else 'SIDEWAYS'
    log.info("Starting trend detection: %s", trend)

    trader.initialize_zones(starting_price, trend)
    trader.restore_open_trades_from_db()
    trader.check_missed_buys(starting_price)
    _run_initial_reconciliation(trader)

    start_offset = flush_telegram_queue()
    _start_bot_threads(trader, exchange, start_offset)
    _run_bot_runtime_loop(trader, exchange, starting_price)
    _send_shutdown_message(trader, exchange)
    log.info("Bot shutdown complete")

if __name__ == "__main__":
    run_bot()
