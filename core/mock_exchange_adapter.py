"""Mock exchange adapter for deterministic API lifecycle testing.

Implements the same method surface used by the bot's exchange layer so
trading/reconciliation paths can be validated without testnet access.
"""

from __future__ import annotations

from collections import deque
from datetime import datetime


class MockExchangeAdapter:
    def __init__(self, exchange_id="mock", initial_price=1.0):
        self.exchange_id = exchange_id
        self.current_price = float(initial_price)
        self._orders = {}
        self._seq = 0
        self._create_outcomes = deque()

    def queue_create_outcome(self, outcome):
        """Queue a create-order outcome.

        Supported outcome shape:
        {
            "error": "message",   # optional, raises RuntimeError
            "status": "open|partial|closed|canceled|rejected",
            "filled": 0.0,         # optional, defaults to 0 unless closed
        }
        """
        self._create_outcomes.append(dict(outcome or {}))

    def supports_shorting(self, short_mode="futures"):
        mode = (short_mode or "").strip().lower()
        return mode in {"futures", "margin"}

    def validate_shorting_requirements(self, short_mode="futures"):
        if not self.supports_shorting(short_mode=short_mode):
            raise ValueError(
                f"Mock adapter does not support short mode: {short_mode}"
            )

    def set_price(self, price):
        self.current_price = float(price)

    def _next_order_id(self):
        self._seq += 1
        return f"mock-{self._seq}"

    def _create_order(self, symbol, side, amount, price=None, order_type="limit"):
        outcome = self._create_outcomes.popleft() if self._create_outcomes else {}
        if outcome.get("error"):
            raise RuntimeError(str(outcome["error"]))

        status = str(outcome.get("status", "open")).lower()
        amount = float(amount)
        if price is None:
            price = self.current_price
        price = float(price)

        if "filled" in outcome:
            filled = float(outcome["filled"])
        elif status == "closed":
            filled = amount
        else:
            filled = 0.0

        order = {
            "id": self._next_order_id(),
            "symbol": symbol,
            "type": order_type,
            "side": side,
            "price": price,
            "amount": amount,
            "filled": max(0.0, min(filled, amount)),
            "status": status,
            "timestamp": datetime.now().isoformat(),
        }
        self._orders[order["id"]] = order
        return dict(order)

    def create_limit_order(self, symbol, side, price, amount):
        return self._create_order(symbol, side, amount, price=price, order_type="limit")

    def create_market_order(self, symbol, side, amount):
        return self._create_order(
            symbol,
            side,
            amount,
            price=self.current_price,
            order_type="market",
        )

    def fetch_ticker(self, symbol):
        return {"symbol": symbol, "last": self.current_price}

    def fetch_ohlcv(self, symbol, timeframe="1h", limit=500):
        candle = [
            0,
            self.current_price,
            self.current_price,
            self.current_price,
            self.current_price,
            1,
        ]
        return [list(candle) for _ in range(int(limit))]

    def fetch_order(self, order_id, symbol=None):
        order = self._orders.get(order_id)
        return dict(order) if order else None

    def fetch_open_orders(self, symbol=None):
        open_statuses = {"open", "partial", "partially_filled"}
        rows = [
            o
            for o in self._orders.values()
            if str(o.get("status", "")).lower() in open_statuses
        ]
        if symbol:
            rows = [o for o in rows if o.get("symbol") == symbol]
        return [dict(o) for o in rows]

    def cancel_order(self, order_id, symbol=None):
        order = self._orders.get(order_id)
        if not order:
            raise RuntimeError(f"Unknown mock order: {order_id}")
        order["status"] = "canceled"
        return dict(order)

    def simulate_fill(self, order_id, filled):
        order = self._orders.get(order_id)
        if not order:
            raise RuntimeError(f"Unknown mock order: {order_id}")
        filled = float(filled)
        order["filled"] = max(0.0, min(filled, float(order["amount"])))
        if order["filled"] >= float(order["amount"]):
            order["status"] = "closed"
        elif order["filled"] > 0:
            order["status"] = "partial"
        else:
            order["status"] = "open"
        return dict(order)
