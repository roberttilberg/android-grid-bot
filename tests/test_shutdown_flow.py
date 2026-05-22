import asyncio
import importlib
import sys
import types

import pytest


@pytest.fixture
def app(monkeypatch):
    # Minimal env required by core.config during imports.
    monkeypatch.setenv("TELEGRAM_TOKEN", "demo")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "1")
    monkeypatch.setenv("EXECUTE_LIVE", "false")

    # Lightweight stubs for optional import-time dependencies.
    for modname in ("ccxt", "pandas", "requests", "dotenv"):
        if modname not in sys.modules:
            m = types.ModuleType(modname)
            if modname == "dotenv":
                m.load_dotenv = lambda *a, **k: None
            sys.modules[modname] = m

    import android_grid_bot_v1 as grid
    import core.config as config
    importlib.reload(config)
    importlib.reload(grid)
    config.stop_flag.clear()
    yield config, grid
    config.stop_flag.clear()


def test_grid_module_uses_shared_stop_flag(app):
    config, grid = app
    assert grid.stop_flag is config.stop_flag


def test_runtime_loop_exits_when_shared_stop_flag_set(app):
    config, grid = app
    config.stop_flag.set()

    class DummyTrader:
        pass

    class DummyExchange:
        pass

    asyncio.run(grid._run_bot_runtime_loop(DummyTrader(), DummyExchange(), 1.0))


def test_shutdown_background_tasks_cancels_all(app, monkeypatch):
    config, grid = app

    async def fake_listener(*args, **kwargs):
        await grid.asyncio.sleep(60)

    async def fake_reconcile(*args, **kwargs):
        await grid.asyncio.sleep(60)

    monkeypatch.setattr(grid, "telegram_listener", fake_listener)
    monkeypatch.setattr(grid, "reconcile_worker", fake_reconcile)

    class DummyTrader:
        exchange = types.SimpleNamespace(exchange_id="mock")

    class DummyExchange:
        pass

    async def runner():
        tasks = grid._start_bot_threads(
            DummyTrader(), DummyExchange(), start_offset=None
        )
        assert len(tasks) == 2
        assert all(not t.done() for t in tasks)
        await grid._shutdown_background_tasks(tasks)
        assert all(t.done() for t in tasks)

    asyncio.run(runner())
