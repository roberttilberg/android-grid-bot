import math

from core.sr import compute_grid_from_sr, detect_support_resistance


class FakeAdapter:
    def __init__(self, prices):
        # prices: list of (open, high, low, close)
        self._prices = prices

    def fetch_ohlcv(self, symbol, timeframe='1h', limit=300):
        # Return list of candles: [ts, open, high, low, close, vol]
        out = []
        ts = 1600000000000
        for o, h, low, c in self._prices:
            out.append([ts, o, h, low, c, 0])
            ts += 3600000
        return out


def make_test_series():
    # Build a simple synthetic series with visible pivots
    prices = []
    base = 1.30
    # gentle up trend with two clear peaks and two clear troughs
    for i in range(40):
        if i in (8, 24):
            h = base + 0.12
            low = base + 0.06
            o = base + 0.08
            c = base + 0.10
        elif i in (16, 32):
            h = base + 0.04
            low = base - 0.06
            o = base - 0.02
            c = base - 0.04
        else:
            h = base + 0.02 * math.sin(i / 3.0)
            low = base - 0.02 * math.cos(i / 5.0)
            o = base
            c = base + 0.01 * math.sin(i / 2.0)
        prices.append((round(o, 6), round(h, 6), round(low, 6), round(c, 6)))
    return prices


def test_sr_detection_and_grid_mapping():
    series = make_test_series()
    adapter = FakeAdapter(series)
    sr = detect_support_resistance(
        adapter=adapter, symbol='XRP/USDT', timeframe='1h', lookback=40
    )
    assert isinstance(sr, list)
    assert len(sr) > 0
    for item in sr:
        assert 'price' in item and 'strength' in item and 'type' in item

    grid = compute_grid_from_sr(sr, target_levels=10)
    assert isinstance(grid, list)
    assert len(grid) == 11
    assert grid[0] < grid[-1]
