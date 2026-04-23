#!/usr/bin/env python3
"""Simulated reconcile demo.

Creates a temporary DB, inserts a sample open order, and runs `reconcile_once`
from the bot using a fake adapter that returns a filled remote order. Prints
orders/trades/order_trades tables to demonstrate end-to-end behavior.
"""
# ruff: noqa: E402
import importlib
import os
import sqlite3
import sys
import tempfile
import types
from pathlib import Path

print('Starting simulated reconcile demo')

# Create temp workspace
tmpdir = tempfile.mkdtemp(prefix='reconcile_demo_')
print('Temp dir:', tmpdir)

# Ensure minimal env vars so importing the bot doesn't exit
os.environ.setdefault('TELEGRAM_TOKEN', 'demo')
os.environ.setdefault('TELEGRAM_CHAT_ID', '1')
os.environ.setdefault('EXECUTE_LIVE', 'false')

# Calculate paths
THIS = Path(__file__).resolve()
ANDROID_DIR = str(THIS.parents[1])  # <android-grid-bot>
print('Android dir:', ANDROID_DIR)
# Make sure Python can import local modules from the repo
sys.path.insert(0, ANDROID_DIR)

# Inject light-weight stubs for heavy external packages so import works
for modname in ('ccxt', 'pandas', 'requests', 'dotenv'):
    if modname not in sys.modules:
        m = types.ModuleType(modname)
        if modname == 'dotenv':
            m.load_dotenv = lambda *a, **k: None
        sys.modules[modname] = m

import db

# Point DB to temp file inside tmpdir
db.DB_PATH = os.path.join(tmpdir, 'trades.db')

# Ensure orders table exists
db.ensure_orders_table()

# Change cwd so modules that use relative 'trades.db' write into tmpdir
os.chdir(tmpdir)

# Import the bot module
grid = importlib.import_module('andriod_grid_bot_v1')

# Initialize bot DB structures (creates trades table etc.)
grid.init_db()

print('Inserting sample open order into local orders table...')
orders_db = db
orders_db.insert_order(
    'phemex',
    'sample-order-1',
    'XRP/USDT',
    'BUY',
    1.30,
    3.0,
    status='open',
    processed=0,
)

class FakeAdapter:
    def __init__(self):
        self.exchange_id = 'phemex'

    def fetch_open_orders(self, symbol):
        return []

    def fetch_order(self, order_id, symbol=None):
        if order_id == 'sample-order-1':
            return {
                'id': 'sample-order-1',
                'symbol': 'XRP/USDT',
                'side': 'BUY',
                'price': 1.30,
                'amount': 3.0,
                'filled': 3.0,
                'status': 'closed',
                'fee': {'cost': 0.001}
            }
        return None

    def fetch_ticker(self, symbol):
        return {'last': 1.35}

print('Running reconcile_once with fake adapter...')
adapter = FakeAdapter()
grid_levels = grid.calculate_grid_levels(
    grid.GRID_LOWER,
    grid.GRID_UPPER,
    grid.GRID_LEVELS,
)
trader = grid.PaperTrader(grid.PAPER_BALANCE, grid_levels)

grid.reconcile_once(
    adapter=adapter,
    symbol='XRP/USDT',
    exchange_id='phemex',
    trader=trader,
)

print('\n=== DB CONTENTS ===')
conn = sqlite3.connect(os.path.join(tmpdir, 'trades.db'))
c = conn.cursor()

print('\nORDERS:')
for row in c.execute(
    (
        'SELECT id, exchange, exchange_order_id, side, price, amount, '
        'filled, status, processed FROM orders'
    )
):
    print(row)

print('\nTRADES:')
for row in c.execute(
    (
        'SELECT id, timestamp, side, price, amount, profit_loss, '
        'fees, notes FROM trades'
    )
):
    print(row)

print('\nORDER_TRADES:')
for row in c.execute(
    (
        'SELECT id, exchange, exchange_order_id, trade_id, side, price, '
        'amount, fee FROM order_trades'
    )
):
    print(row)

conn.close()
print('\nDemo complete — temp DB at', os.path.join(tmpdir, 'trades.db'))
