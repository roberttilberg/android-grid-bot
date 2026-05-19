import importlib
import sys
import types


def _prepare_env(monkeypatch):
    # Minimal runtime env expected by core.config during import.
    monkeypatch.setenv("TELEGRAM_TOKEN", "demo")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "1")
    monkeypatch.setenv("EXECUTE_LIVE", "false")


def test_shorts_disabled_by_default(monkeypatch):
    _prepare_env(monkeypatch)
    monkeypatch.delenv("ALLOW_SHORTS", raising=False)

    if "dotenv" not in sys.modules:
        dotenv_mod = types.ModuleType("dotenv")
        dotenv_mod.load_dotenv = lambda *args, **kwargs: None
        sys.modules["dotenv"] = dotenv_mod

    import core.config as config

    importlib.reload(config)
    assert config.ALLOW_SHORTS is False


def test_signed_exposure_tracks_long_state(monkeypatch):
    _prepare_env(monkeypatch)

    if "dotenv" not in sys.modules:
        dotenv_mod = types.ModuleType("dotenv")
        dotenv_mod.load_dotenv = lambda *args, **kwargs: None
        sys.modules["dotenv"] = dotenv_mod

    import core.trading as trading

    trader = trading.PaperTrader(100.0, trading.calculate_grid_levels(1.25, 1.45, 20))
    assert all(exposure == 0 for exposure in trader.zone_exposure)

    trader._set_zone_long(0, 1.30, 1.31, is_catchup=False)
    assert trader.zone_exposure[0] == 1
    assert trader.zone_holding[0] is True

    trader._clear_zone(0)
    assert trader.zone_exposure[0] == 0
    assert trader.zone_holding[0] is False


def test_open_short_is_gated_when_disabled(monkeypatch):
    _prepare_env(monkeypatch)
    monkeypatch.setenv("ALLOW_SHORTS", "false")

    if "dotenv" not in sys.modules:
        dotenv_mod = types.ModuleType("dotenv")
        dotenv_mod.load_dotenv = lambda *args, **kwargs: None
        sys.modules["dotenv"] = dotenv_mod

    import core.config as config
    import core.trading as trading

    importlib.reload(config)
    importlib.reload(trading)

    trader = trading.PaperTrader(100.0, trading.calculate_grid_levels(1.25, 1.45, 20))
    assert trader.open_short(1.30, 5.0, 0) is False
    assert trader.zone_exposure[0] == 0


def test_open_and_close_short_lifecycle(monkeypatch):
    _prepare_env(monkeypatch)
    monkeypatch.setenv("ALLOW_SHORTS", "true")
    monkeypatch.setenv("MAX_SHORT_ZONES", "3")
    monkeypatch.setenv("MAX_SHORT_NOTIONAL", "50")
    monkeypatch.setenv("MIN_MARGIN_RATIO", "0.01")

    if "dotenv" not in sys.modules:
        dotenv_mod = types.ModuleType("dotenv")
        dotenv_mod.load_dotenv = lambda *args, **kwargs: None
        sys.modules["dotenv"] = dotenv_mod

    import core.config as config
    import core.trading as trading

    importlib.reload(config)
    importlib.reload(trading)

    trader = trading.PaperTrader(100.0, trading.calculate_grid_levels(1.25, 1.45, 20))
    opened = trader.open_short(1.40, 5.0, 1)
    assert opened is True
    assert trader.zone_exposure[1] == -1
    assert trader.zone_sell_target[1] < 1.40

    closed = trader.close_short(1.38, 5.0, 1)
    assert closed is True
    assert trader.zone_exposure[1] == 0


def test_risk_snapshot_and_emergency_short_close(monkeypatch):
    _prepare_env(monkeypatch)
    monkeypatch.setenv("ALLOW_SHORTS", "true")
    monkeypatch.setenv("MAX_SHORT_ZONES", "3")
    monkeypatch.setenv("MAX_SHORT_NOTIONAL", "50")
    monkeypatch.setenv("MIN_MARGIN_RATIO", "0.01")

    if "dotenv" not in sys.modules:
        dotenv_mod = types.ModuleType("dotenv")
        dotenv_mod.load_dotenv = lambda *args, **kwargs: None
        sys.modules["dotenv"] = dotenv_mod

    import core.config as config
    import core.trading as trading

    importlib.reload(config)
    importlib.reload(trading)

    trader = trading.PaperTrader(100.0, trading.calculate_grid_levels(1.25, 1.45, 20))
    assert trader.open_short(1.40, 5.0, 2) is True

    snap = trader.get_positions_snapshot(1.39)
    assert 2 in snap["short_zones"]

    risk = trader.get_risk_snapshot(1.39)
    assert risk["allow_shorts"] is True
    assert risk["short_count"] == 1

    closed = trader.emergency_close_shorts(1.39, reason="test")
    assert closed == 1
    assert trader.zone_exposure[2] == 0
