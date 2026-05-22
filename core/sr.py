"""Support/Resistance detection helpers.

This module provides simple pivot-based SR detection and a helper to map
detected SR levels into a grid compatible with the bot's existing
`calculate_grid_levels` helper.

The implementation is intentionally lightweight and dependency-free so it
can run in Termux environments without extra packages.
"""
import importlib
import logging
from typing import Dict, List, Optional

# Avoid importing core.config at module import time (it may raise SystemExit
# when running tests). Import lazily inside functions and tolerate failure.
config = None

log = logging.getLogger("gridbot.sr")


def _cluster_prices(prices: List[float], tolerance_pct: float) -> List[Dict]:
    """Cluster nearby prices into representative levels.

    Returns list of {'price': float, 'strength': int}.
    """
    if not prices:
        return []
    prices = sorted(prices)
    clusters: List[Dict] = []
    for p in prices:
        if not clusters:
            clusters.append({"sum": p, "count": 1})
            continue
        last = clusters[-1]
        center = last["sum"] / last["count"]
        if abs(p - center) / center <= tolerance_pct:
            last["sum"] += p
            last["count"] += 1
        else:
            clusters.append({"sum": p, "count": 1})
    out: List[Dict] = []
    for c in clusters:
        out.append({"price": round(c["sum"] / c["count"], 4), "strength": c["count"]})
    return out


def _find_pivots(lows: List[float], highs: List[float], pivot_window: int):
    """Scan low/high arrays and return (supports, resistances) lists."""
    supports: List[float] = []
    resistances: List[float] = []
    n = len(lows)
    for i in range(pivot_window, n - pivot_window):
        window_lows = lows[i - pivot_window : i + pivot_window + 1]
        window_highs = highs[i - pivot_window : i + pivot_window + 1]
        cur_low = lows[i]
        cur_high = highs[i]
        if cur_low <= min(window_lows) and window_lows.count(cur_low) == 1:
            supports.append(float(cur_low))
        if cur_high >= max(window_highs) and window_highs.count(cur_high) == 1:
            resistances.append(float(cur_high))
    return supports, resistances


def _build_adapter_if_none(adapter, exchange_id=None):
    if adapter is not None:
        return adapter
    try:
        # Lazy import of core.config; tolerate failures (tests may not set env)
        try:
            cfg = importlib.import_module('core.config')
        except BaseException:
            cfg = None
        if exchange_id is None:
            exchange_id = getattr(cfg, "EXCHANGE_ID", "phemex")
        if exchange_id == "mock":
            from core.mock_exchange_adapter import MockExchangeAdapter
            return MockExchangeAdapter(exchange_id="mock")
        from exchange_adapter import ExchangeAdapter
        API_KEY = getattr(cfg, "API_KEY", None)
        API_SECRET = getattr(cfg, "API_SECRET", None)
        options = None
        if exchange_id == "phemex":
            options = {"defaultType": "swap"}
        return ExchangeAdapter(exchange_id, API_KEY, API_SECRET, options=options)
    except Exception as e:
        log.debug("Could not build adapter for SR detection: %s", e)
        return None


def detect_support_resistance(
    adapter=None,
    symbol: str = None,
    timeframe: str = "1h",
    lookback: int = 300,
    pivot_window: int = 3,
    cluster_tolerance_pct: float = 0.002,
    max_levels: int = 24,
) -> List[Dict]:
    """Detect pivot-based support and resistance levels.

    Returns a list of dicts with keys: price, strength and type.
    The list is ordered by strength (descending).
    """
    try:
        adapter = _build_adapter_if_none(adapter)
        if adapter is None:
            return []

        if symbol is None:
            try:
                cfg = importlib.import_module('core.config')
            except BaseException:
                cfg = None
            symbol = getattr(cfg, "SYMBOL", None) if cfg is not None else None
            if symbol is None:
                return []

        fetch_symbol = symbol.replace(":USDT", "") if ":" in symbol else symbol
        ohlcv = adapter.fetch_ohlcv(fetch_symbol, timeframe=timeframe, limit=lookback)
        if not ohlcv or len(ohlcv) < (pivot_window * 2 + 1):
            return []

        lows = [c[3] for c in ohlcv]
        highs = [c[2] for c in ohlcv]

        # Find pivot lows/highs using helper to keep complexity down
        supports, resistances = _find_pivots(lows, highs, pivot_window)

        # Cluster nearby pivots into representative levels
        sup_clusters = _cluster_prices(supports, cluster_tolerance_pct)
        res_clusters = _cluster_prices(resistances, cluster_tolerance_pct)

        # Merge into single list with type and sort by strength desc
        out = []
        for s in sup_clusters:
            out.append(
                {
                    'price': s['price'],
                    'strength': s['strength'],
                    'type': 'support',
                }
            )
        for r in res_clusters:
            out.append(
                {
                    'price': r['price'],
                    'strength': r['strength'],
                    'type': 'resistance',
                }
            )

        out = sorted(out, key=lambda x: (-x['strength'], x['price']))
        return out[:max_levels]
    except Exception as e:
        log.debug("SR detection error: %s", e)
        return []


def compute_grid_from_sr(
    sr_levels: List[Dict],
    target_levels: int = 20,
    lower_pad_pct: float = 0.002,
    upper_pad_pct: float = 0.002,
    min_spacing_pct: float = 0.0025,
    tick_size: float = 0.0001,
) -> Optional[List[float]]:
    """Map SR levels into a market grid.

    Returns a list of price levels (len = target_levels + 1).

    Strategy (simple first-pass):
    - Use min/max of detected SR prices and pad slightly
    - Fall back to equal spacing via `calculate_grid_levels` from core.trading
    """
    try:
        if not sr_levels:
            return None
        prices = sorted({float(p['price']) for p in sr_levels})
        low = prices[0] * (1 - lower_pad_pct)
        high = prices[-1] * (1 + upper_pad_pct)
        if low >= high:
            # degenerate case: expand a tiny bit around the single level
            low = prices[0] * (1 - 0.01)
            high = prices[0] * (1 + 0.01)

        # Ensure minimum spacing between adjacent levels
        try:
            from core.trading import calculate_grid_levels
        except BaseException:
            # If trading helper not available, build simple linear grid
            step = (high - low) / target_levels
            return [round(low + i * step, 4) for i in range(target_levels + 1)]

        grid = calculate_grid_levels(round(low, 4), round(high, 4), target_levels)
        # Snap to tick size
        def snap(x):
            return round(round(x / tick_size) * tick_size, 4)

        grid = [snap(x) for x in grid]
        # Deduplicate after snapping
        uniq = []
        for x in grid:
            if not uniq or abs(uniq[-1] - x) > 1e-8:
                uniq.append(x)
        if len(uniq) < 2:
            return None
        return uniq
    except Exception as e:
        log.debug("compute_grid_from_sr error: %s", e)
        return None
