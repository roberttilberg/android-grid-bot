from core.mock_exchange_adapter import MockExchangeAdapter


def test_mock_create_and_fetch_open_orders():
    adapter = MockExchangeAdapter(initial_price=1.23)
    order = adapter.create_limit_order("XRP/USDT", "buy", 1.2, 5.0)

    assert order["id"].startswith("mock-")
    assert order["status"] == "open"
    assert float(order["filled"]) == 0.0

    open_orders = adapter.fetch_open_orders("XRP/USDT")
    assert len(open_orders) == 1
    assert open_orders[0]["id"] == order["id"]


def test_mock_simulate_partial_then_closed_fill():
    adapter = MockExchangeAdapter(initial_price=1.25)
    order = adapter.create_market_order("XRP/USDT", "buy", 4.0)

    partial = adapter.simulate_fill(order["id"], 1.5)
    assert partial["status"] == "partial"
    assert float(partial["filled"]) == 1.5

    closed = adapter.simulate_fill(order["id"], 4.0)
    assert closed["status"] == "closed"
    assert float(closed["filled"]) == 4.0

    # Closed orders should disappear from open-orders list.
    assert adapter.fetch_open_orders("XRP/USDT") == []


def test_mock_cancel_order():
    adapter = MockExchangeAdapter()
    order = adapter.create_limit_order("XRP/USDT", "sell", 1.4, 3.0)
    canceled = adapter.cancel_order(order["id"], "XRP/USDT")

    assert canceled["status"] == "canceled"
    assert adapter.fetch_open_orders("XRP/USDT") == []


def test_mock_queue_create_error():
    adapter = MockExchangeAdapter()
    adapter.queue_create_outcome({"error": "simulated_exchange_error"})

    try:
        adapter.create_market_order("XRP/USDT", "buy", 1.0)
    except RuntimeError as exc:
        assert "simulated_exchange_error" in str(exc)
    else:
        raise AssertionError("Expected RuntimeError from queued mock outcome")


def test_mock_short_capabilities():
    adapter = MockExchangeAdapter()

    assert adapter.supports_shorting("futures") is True
    assert adapter.supports_shorting("margin") is True
    assert adapter.supports_shorting("invalid") is False

    adapter.validate_shorting_requirements("futures")
