"""exchange_adapter.py
Simple synchronous CCXT adapter with retry/backoff helpers.
Keep calls centralized so grid logic can call a single adapter and
benefit from common retry/error handling appropriate for Termux devices.
"""
import logging
import time
from functools import wraps

import ccxt

log = logging.getLogger("gridbot.exchange")


def retry(max_attempts=5, base_delay=0.5):
    def deco(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            attempt = 0
            while True:
                try:
                    return f(*args, **kwargs)
                except Exception as e:
                    attempt += 1
                    if attempt >= max_attempts:
                        log.error("Exhausted retries for %s: %s", f.__name__, e)
                        raise
                    delay = base_delay * (2 ** (attempt - 1)) + (0.05 * attempt)
                    log.warning(
                        "Retry %d/%d for %s after error %s; sleeping %.2fs",
                        attempt,
                        max_attempts,
                        f.__name__,
                        e,
                        delay,
                    )
                    time.sleep(delay)
        return wrapper
    return deco


class ExchangeAdapter:
    def __init__(
        self,
        exchange_id="binance",
        api_key=None,
        secret=None,
        enable_rate_limit=True,
        options=None,
    ):
        cfg = {"enableRateLimit": enable_rate_limit}
        if api_key:
            cfg.update({"apiKey": api_key, "secret": secret})
        if options:
            cfg["options"] = options

        try:
            self.exchange = getattr(ccxt, exchange_id)(cfg)
            # load markets lazily but attempt once to validate keys
            try:
                self.exchange.load_markets()
            except Exception:
                # non-fatal: allow using adapter for public endpoints
                log.info(
                    "Could not load markets during adapter init; continuing "
                    "(public requests allowed)"
                )
        except AttributeError as exc:
            raise ValueError(f"Unknown exchange id: {exchange_id}") from exc
        self.exchange_id = exchange_id

    @retry()
    def fetch_ticker(self, symbol):
        return self.exchange.fetch_ticker(symbol)

    @retry()
    def fetch_ohlcv(self, symbol, timeframe="1h", limit=500):
        return self.exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)

    @retry()
    def create_limit_order(self, symbol, side, price, amount):
        return self.exchange.create_order(symbol, "limit", side, amount, price)

    @retry()
    def create_market_order(self, symbol, side, amount):
        return self.exchange.create_order(symbol, "market", side, amount)

    @retry()
    def cancel_order(self, order_id, symbol=None):
        if symbol:
            return self.exchange.cancel_order(order_id, symbol)
        return self.exchange.cancel_order(order_id)

    @retry()
    def fetch_order(self, order_id, symbol=None):
        if symbol:
            return self.exchange.fetch_order(order_id, symbol)
        return self.exchange.fetch_order(order_id)

    @retry()
    def fetch_open_orders(self, symbol=None):
        if symbol:
            return self.exchange.fetch_open_orders(symbol)
        return self.exchange.fetch_open_orders()
