import importlib
import threading
from unittest.mock import MagicMock

import pytest


@pytest.fixture(autouse=True)
def ensure_telegram_env(monkeypatch):
    # Ensure core.config import won't exit during tests
    monkeypatch.setenv("TELEGRAM_TOKEN", "testtoken")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "12345")
    yield


def load_modules():
    # Import and reload config first so it picks up the env fixture
    import core.config as config
    importlib.reload(config)
    th = importlib.import_module("core.telegram_handler")
    importlib.reload(th)
    return th, config


def test_stop_sets_config_stop_flag(monkeypatch):
    th, config = load_modules()
    mock_send = MagicMock()
    monkeypatch.setattr(th, "send_telegram", mock_send)
    # ensure cleared before test
    try:
        config.stop_flag.clear()
    except Exception:
        pass

    res = th._dispatch_command("/stop", None, None)
    assert res is True
    assert config.stop_flag.is_set()
    assert mock_send.called

    # cleanup
    config.stop_flag.clear()


def test_stop_is_idempotent(monkeypatch):
    th, config = load_modules()
    mock_send = MagicMock()
    monkeypatch.setattr(th, "send_telegram", mock_send)

    config.stop_flag.clear()
    first = th._dispatch_command("/stop", None, None)
    second = th._dispatch_command("/stop", None, None)

    assert first is True
    assert second is True
    assert config.stop_flag.is_set()
    assert mock_send.call_count == 2
    assert "Stop command received" in mock_send.call_args_list[0][0][0]
    assert "already in progress" in mock_send.call_args_list[1][0][0]

    config.stop_flag.clear()


def test_agent_triggers_run_agent(monkeypatch):
    th, config = load_modules()
    run_called = threading.Event()

    def fake_run_agent(trader, price):
        run_called.set()

    monkeypatch.setattr(th, "run_agent", fake_run_agent)
    mock_send = MagicMock()
    monkeypatch.setattr(th, "send_telegram", mock_send)

    class DummyTrader:
        pass

    th._dispatch_command("/agent", DummyTrader(), None)
    # Wait briefly for the daemon thread to execute
    run_called.wait(1.0)
    assert run_called.is_set(), "run_agent thread was not started/executed"
    assert mock_send.called


def test_help_sends_message(monkeypatch):
    th, config = load_modules()
    mock_send = MagicMock()
    monkeypatch.setattr(th, "send_telegram", mock_send)
    res = th._dispatch_command("/help", None, None)
    assert res is False
    assert mock_send.called


def test_status_calls_trader_status_report(monkeypatch):
    th, config = load_modules()
    mock_send = MagicMock()
    monkeypatch.setattr(th, "send_telegram", mock_send)
    monkeypatch.setattr(th, "get_price", lambda exchange: 42.0)

    class DummyTrader:
        def status_report(self, current_price):
            return f"STATUS at {current_price}"

    trader = DummyTrader()
    res = th._dispatch_command("/status", trader, None)
    assert res is False
    mock_send.assert_called_once()
    sent_arg = mock_send.call_args[0][0]
    assert "STATUS at 42.0" in sent_arg


def test_unknown_command_ignored(monkeypatch):
    th, config = load_modules()
    mock_send = MagicMock()
    monkeypatch.setattr(th, "send_telegram", mock_send)
    res = th._dispatch_command("/doesnotexist", None, None)
    assert res is False
    assert not mock_send.called
