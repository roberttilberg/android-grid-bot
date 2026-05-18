from datetime import datetime
"""db.py
Thread-safe SQLite helper for WAL mode and simple order table helpers.

Designed for Termux/local deployments; keeps the same `trades.db` filename
used by the main bot so it remains compatible with existing code.
"""
import logging
import os
import sqlite3
import threading
from contextlib import contextmanager

log = logging.getLogger("gridbot.db")

# Keep DB file next to the bot for compatibility with existing scripts
DB_PATH = os.path.join(os.path.dirname(__file__), "trades.db")

_thread_local = threading.local()


def get_conn():
    """Return a thread-local sqlite3 connection configured for WAL mode."""
    conn = getattr(_thread_local, "conn", None)
    if conn:
        return conn

    conn = sqlite3.connect(
        DB_PATH,
        detect_types=sqlite3.PARSE_DECLTYPES,
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
    except Exception as e:
        log.warning(f"Could not set pragmas on SQLite DB: {e}")

    _thread_local.conn = conn
    return conn


def close_conn():
    conn = getattr(_thread_local, "conn", None)
    if conn:
        try:
            conn.close()
        except Exception:
            pass
        _thread_local.conn = None


@contextmanager
def transaction():
    """Run a set of SQLite operations atomically on the thread-local connection."""
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def ensure_orders_table():
    """Create a lightweight orders table for reconciliation/tracking."""
    conn = get_conn()
    c = conn.cursor()
    orders_create = (
        "CREATE TABLE IF NOT EXISTS orders ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "exchange TEXT, "
        "exchange_order_id TEXT, "
        "symbol TEXT, "
        "side TEXT, "
        "price REAL, "
        "amount REAL, "
        "filled REAL DEFAULT 0, "
        "status TEXT, "
        "created_at TEXT DEFAULT (datetime('now')), "
        "updated_at TEXT DEFAULT (datetime('now')), "
        "processed INTEGER DEFAULT 0"
        ")"
    )
    c.execute(orders_create)
    conn.commit()

    # Ensure 'processed' column exists for older DBs
    try:
        c.execute("PRAGMA table_info(orders)")
        cols = {r[1] for r in c.fetchall()}
        if 'processed' not in cols:
            c.execute("ALTER TABLE orders ADD COLUMN processed INTEGER DEFAULT 0")
            conn.commit()
    except Exception:
        # Non-fatal
        pass

    # Create order_trades mapping table to link exchange orders -> trades (idempotency)
    try:
        order_trades_create = (
            "CREATE TABLE IF NOT EXISTS order_trades ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "exchange TEXT, "
            "exchange_order_id TEXT, "
            "trade_id INTEGER, "
            "side TEXT, "
            "price REAL, "
            "amount REAL, "
            "fee REAL DEFAULT 0, "
            "created_at TEXT DEFAULT (datetime('now')), "
            "UNIQUE(exchange, exchange_order_id, trade_id)"
            ")"
        )
        c.execute(order_trades_create)
        conn.commit()
    except Exception:
        # Non-fatal
        pass

    # Deduplicate historical rows before adding a uniqueness guard.
    try:
        delete_dup_sql = (
            "DELETE FROM orders "
            "WHERE id NOT IN ( "
            "SELECT MAX(id) FROM orders GROUP BY exchange, exchange_order_id )"
        )
        c.execute(delete_dup_sql)
        c.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_exchange_order_id "
            "ON orders(exchange, exchange_order_id)"
        )
        conn.commit()
    except Exception as e:
        log.warning(f"Could not enforce unique order index: {e}")


def insert_order(
    exchange,
    exchange_order_id,
    symbol,
    side,
    price,
    amount,
    status="open",
    processed=0,
    conn=None,
    commit=True,
):
    conn = conn or get_conn()
    c = conn.cursor()
    sql = (
        "INSERT INTO orders (exchange, exchange_order_id, symbol, side, "
        "price, amount, status, processed) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(exchange, exchange_order_id) DO UPDATE SET "
        "symbol=excluded.symbol, "
        "side=excluded.side, "
        "price=excluded.price, "
        "amount=excluded.amount, "
        "status=excluded.status, "
        "processed=excluded.processed, "
        "updated_at=datetime('now')"
    )
    c.execute(
        sql,
        (
            exchange,
            str(exchange_order_id),
            symbol,
            side,
            price,
            amount,
            status,
            processed,
        ),
    )
    if commit:
        conn.commit()
    return c.lastrowid


def insert_trade(
    side,
    price,
    amount,
    usdt_value,
    grid_level,
    profit_loss,
    balance_after,
    market_price,
    buy_price=0,
    sell_target=0,
    fees=0,
    notes="",
    timestamp=None,
    conn=None,
    commit=True,
):
    conn = conn or get_conn()
    c = conn.cursor()
    trade_sql = (
        "INSERT INTO trades (timestamp, side, price, amount, usdt_value, "
        "grid_level, profit_loss, balance_after, "
        "market_price, buy_price, sell_target, fees, notes) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )
    c.execute(
        trade_sql,
        (
            timestamp,
            side,
            price,
            amount,
            usdt_value,
            grid_level,
            profit_loss,
            balance_after,
            market_price,
            buy_price,
            sell_target,
            fees,
            notes,
        ),
    )
    trade_id = c.lastrowid
    if commit:
        conn.commit()
    return trade_id


def get_unprocessed_orders(exchange):
    conn = get_conn()
    c = conn.cursor()
    try:
        c.execute("SELECT * FROM orders WHERE exchange=? AND processed=0", (exchange,))
        return c.fetchall()
    except Exception:
        # Older DB without 'processed' column: return none
        return []


def get_mappings_for_order(exchange, exchange_order_id):
    conn = get_conn()
    c = conn.cursor()
    try:
        c.execute(
            "SELECT * FROM order_trades WHERE exchange=? AND exchange_order_id=?",
            (exchange, str(exchange_order_id)),
        )
        return c.fetchall()
    except Exception:
        return []


def get_mappings_by_exchange(exchange):
    """Return all order->trade mappings for an exchange."""
    conn = get_conn()
    c = conn.cursor()
    try:
        c.execute(
            "SELECT * FROM order_trades WHERE exchange=?",
            (exchange,),
        )
        return c.fetchall()
    except Exception:
        return []


def get_mapped_amount_sum(exchange, exchange_order_id):
    """Return the sum of mapped amounts for a given exchange order (0.0 if none)."""
    conn = get_conn()
    c = conn.cursor()
    try:
        c.execute(
            "SELECT SUM(amount) as total FROM order_trades "
            "WHERE exchange=? AND exchange_order_id=?",
            (exchange, str(exchange_order_id)),
        )
        row = c.fetchone()
        if not row:
            return 0.0
        if isinstance(row, tuple):
            total = row[0]
        else:
            total = row["total"] if "total" in row.keys() else None
        return float(total) if total is not None else 0.0
    except Exception:
        return 0.0


def insert_order_trade_mapping(
    exchange,
    exchange_order_id,
    trade_id,
    side,
    price,
    amount,
    fee=0.0,
    conn=None,
    commit=True,
):
    conn = conn or get_conn()
    c = conn.cursor()
    try:
        # Avoid duplicate mapping for same exchange order + trade
        c.execute(
            "SELECT id FROM order_trades "
            "WHERE exchange=? AND exchange_order_id=? AND trade_id=?",
            (exchange, str(exchange_order_id), int(trade_id)),
        )
        row = c.fetchone()
        if row:
            return row[0]
        c.execute(
            "INSERT INTO order_trades (exchange, exchange_order_id, trade_id, "
            "side, price, amount, fee) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (exchange, str(exchange_order_id), int(trade_id), side, price, amount, fee),
        )
        if commit:
            conn.commit()
        return c.lastrowid
    except Exception as e:
        log.warning(f"Could not insert order_trade mapping: {e}")
        return None


def get_trade_by_id(trade_id):
    """Fetch a trade row from the trades table by id."""
    conn = get_conn()
    c = conn.cursor()
    try:
        c.execute("SELECT * FROM trades WHERE id=?", (int(trade_id),))
        return c.fetchone()
    except Exception:
        return None


def update_order_by_exchange_id(
    exchange,
    exchange_order_id,
    conn=None,
    commit=True,
    **fields,
):
    if not fields:
        return
    conn = conn or get_conn()
    c = conn.cursor()
    cols = ", ".join([f"{k}=?" for k in fields.keys()])
    vals = list(fields.values())
    vals.extend([exchange, str(exchange_order_id)])
    query = (
        f"UPDATE orders SET {cols}, updated_at = datetime('now') "
        "WHERE exchange=? AND exchange_order_id=?"
    )
    c.execute(query, vals)
    if commit:
        conn.commit()


def find_order_by_exchange_id(exchange, exchange_order_id):
    conn = get_conn()
    c = conn.cursor()
    c.execute(
        "SELECT * FROM orders WHERE exchange=? AND exchange_order_id=? LIMIT 1",
        (exchange, str(exchange_order_id)),
    )
    return c.fetchone()


def get_open_orders_from_db(exchange):
    conn = get_conn()
    c = conn.cursor()
    c.execute(
        "SELECT * FROM orders WHERE exchange=? AND status IN ('open','new','partial')",
        (exchange,),
    )
    return c.fetchall()


def log_catchup_trade(zone, entry_price, sell_target):
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        INSERT INTO catchup_trades
        (timestamp, zone, entry_price, sell_target, completed)
        VALUES (?, ?, ?, ?, 0)
    """, (datetime.now().isoformat(), zone, entry_price, sell_target))
    conn.commit()

def complete_catchup_trade(zone, exit_price, profit_loss):
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        UPDATE catchup_trades SET exit_price=?, profit_loss=?, completed=1
        WHERE zone=? AND completed=0
    """, (exit_price, profit_loss, zone))
    conn.commit()

def log_agent_decision(decision, applied=False, rejected=False):
    conn = get_conn()
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

def get_last_trades(n=5):
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT timestamp, side, price, amount, profit_loss, grid_level, sell_target
        FROM trades ORDER BY id DESC LIMIT ?
    """, (n,))
    return c.fetchall()

def get_open_trades_from_db(limit=25):
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT id, timestamp, side, price, amount, grid_level, sell_target
        FROM trades
        WHERE side IN ('BUY', 'SELL')
        ORDER BY id ASC
    """)
    rows = c.fetchall()

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
