import importlib
import sqlite3
import sys
import types
from pathlib import Path

import pytest


@pytest.fixture
def app(tmp_path, monkeypatch):
    # Make repo importable
    android_dir = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(android_dir))

    # Lightweight stubs for heavy external deps used at import-time
    for modname in ("ccxt", "pandas", "requests", "dotenv"):
        if modname not in sys.modules:
            m = types.ModuleType(modname)
            if modname == "dotenv":
                m.load_dotenv = lambda *a, **k: None
            sys.modules[modname] = m

    # Minimal env required by the bot on import
    monkeypatch.setenv("TELEGRAM_TOKEN", "demo")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "1")
    monkeypatch.setenv("EXECUTE_LIVE", "false")

    # Run inside tmpdir so grid.init_db() writes to tmp_path/trades.db
    monkeypatch.chdir(tmp_path)

    db = importlib.import_module("db")
    # point DB to tmp path and ensure any existing thread-local connection is reset
    db.DB_PATH = str(tmp_path / "trades.db")
    try:
        db.close_conn()
    except Exception:
        pass
    db.ensure_orders_table()

    grid = importlib.import_module("andriod_grid_bot_v1")
    grid.init_db()

    yield db, grid


def test_reconcile_full_fill(app):
    db, grid = app

    # insert a sample open order into orders table
    db.insert_order("phemex", "sample-order-1", "XRP/USDT", "BUY", 1.30, 3.0, status="open", processed=0)

    class FakeAdapter:
        def __init__(self):
            self.exchange_id = "phemex"

        def fetch_open_orders(self, symbol):
            return []

        def fetch_order(self, order_id, symbol=None):
            if order_id == "sample-order-1":
                return {
                    "id": "sample-order-1",
                    "symbol": "XRP/USDT",
                    "side": "BUY",
                    "price": 1.30,
                    "amount": 3.0,
                    "filled": 3.0,
                    "status": "closed",
                    "fee": {"cost": 0.001},
                }
            return None

        def fetch_ticker(self, symbol):
            return {"last": 1.35}

    trader = grid.PaperTrader(grid.PAPER_BALANCE, grid.calculate_grid_levels(grid.GRID_LOWER, grid.GRID_UPPER, grid.GRID_LEVELS))
    grid.reconcile_once(adapter=FakeAdapter(), symbol="XRP/USDT", exchange_id="phemex", trader=trader)

    conn = sqlite3.connect(db.DB_PATH)
    c = conn.cursor()

    c.execute("SELECT exchange_order_id, filled, status, processed FROM orders WHERE exchange_order_id=?", ("sample-order-1",))
    row = c.fetchone()
    assert row is not None
    assert float(row[1]) == pytest.approx(3.0)
    assert row[2] in ("closed", "filled") or str(row[2]).lower() == "closed"
    assert int(row[3]) == 1

    c.execute("SELECT COUNT(*) FROM trades")
    trades_count = c.fetchone()[0]
    assert trades_count >= 1

    c.execute("SELECT SUM(amount) FROM order_trades WHERE exchange_order_id=?", ("sample-order-1",))
    mapped_sum = c.fetchone()[0]
    assert float(mapped_sum) == pytest.approx(3.0)


def test_reconcile_partial_fill(app):
    db, grid = app

    db.insert_order("phemex", "sample-order-1", "XRP/USDT", "BUY", 1.30, 3.0, status="open", processed=0)

    class PartialAdapter:
        def __init__(self, fills):
            self.exchange_id = "phemex"
            self.fills = fills
            self.calls = 0

        def fetch_open_orders(self, symbol):
            return []

        def fetch_order(self, order_id, symbol=None):
            # This adapter returns the first fill value for the first two calls
            # (the reconcile implementation calls fetch_order twice per run),
            # then advances to the next fill on subsequent calls to simulate
            # a later reconcile run.
            if order_id == "sample-order-1":
                if self.calls < 2:
                    filled = self.fills[0]
                else:
                    filled = self.fills[1] if len(self.fills) > 1 else self.fills[0]
                self.calls += 1
                return {
                    "id": "sample-order-1",
                    "symbol": "XRP/USDT",
                    "side": "BUY",
                    "price": 1.30,
                    "amount": 3.0,
                    "filled": filled,
                    "status": "closed" if filled >= 3.0 else "partial",
                    "fee": {"cost": 0.001},
                }
            return None

        def fetch_ticker(self, symbol):
            return {"last": 1.35}

    adapter = PartialAdapter([1.5, 3.0])
    trader = grid.PaperTrader(grid.PAPER_BALANCE, grid.calculate_grid_levels(grid.GRID_LOWER, grid.GRID_UPPER, grid.GRID_LEVELS))

    # First reconcile: should log a trade for 1.5 and not mark processed
    grid.reconcile_once(adapter=adapter, symbol="XRP/USDT", exchange_id="phemex", trader=trader)

    conn = sqlite3.connect(db.DB_PATH)
    c = conn.cursor()
    c.execute("SELECT SUM(amount) FROM order_trades WHERE exchange_order_id=?", ("sample-order-1",))
    mapped_sum = c.fetchone()[0] or 0.0
    assert float(mapped_sum) == pytest.approx(1.5)

    c.execute("SELECT processed FROM orders WHERE exchange_order_id=?", ("sample-order-1",))
    processed = c.fetchone()[0]
    assert int(processed) == 0

    # Second reconcile: should log the remaining delta (1.5) and mark processed
    grid.reconcile_once(adapter=adapter, symbol="XRP/USDT", exchange_id="phemex", trader=trader)
    c.execute("SELECT SUM(amount) FROM order_trades WHERE exchange_order_id=?", ("sample-order-1",))
    mapped_sum2 = c.fetchone()[0] or 0.0
    assert float(mapped_sum2) == pytest.approx(3.0)

    c.execute("SELECT processed FROM orders WHERE exchange_order_id=?", ("sample-order-1",))
    processed2 = c.fetchone()[0]
    assert int(processed2) == 1

    conn.close()
