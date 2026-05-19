import asyncio
import logging
from datetime import datetime

import core.config as config
import db as orders_db
from core.analytics import get_catchup_stats
from core.config import (
    AGENT_INTERVAL_HOURS,
    ALLOW_SHORTS,
    AUTO_REBALANCE_ENABLED,
    EXCHANGE_ID,
    GRID_LEVELS,
    GRID_LOWER,
    GRID_REBALANCE_THRESHOLD,
    GRID_UPPER,
    MAKER_FEE_PCT,
    MAX_CATCHUP_ZONES,
    MAX_LOSS_PCT,
    MAX_OPEN_ZONES,
    MAX_SHORT_NOTIONAL,
    MAX_SHORT_ZONES,
    MAX_TOTAL_TRADES,
    MIN_MARGIN_RATIO,
    MIN_PROFIT_RATIO,
    ORDER_SIZE,
    PAPER_BALANCE,
    SHORT_MODE,
    SYMBOL,
    TAKER_FEE_PCT,
    TEST_MODE_ENABLED,
    TREND_BIAS_ENABLED,
    TREND_THRESHOLD,
    UNDERWATER_ALERT_THRESHOLD,
    VOL_THRESHOLD_HIGH,
    VOL_THRESHOLD_LOW,
    VOLATILITY_ADJUSTMENT,
    VOLATILITY_LOOKBACK,
    pending_changes,
    pending_lock,
    stop_flag,
)
from core.telegram_handler import send_telegram
from db import complete_catchup_trade, get_open_trades_from_db, log_catchup_trade

log = logging.getLogger('gridbot.trading')

def _metrics_inc(*args): pass  # Placeholder for now
def _emit_event(*args, **kwargs): pass  # Placeholder for now

def log_trade(side, price, amount, usdt_value, grid_level, profit_loss,
              balance_after, market_price, buy_price=0, sell_target=0,
              fees=0, notes="", conn=None, commit=True):
    """Insert a trade record into the database."""
    return orders_db.insert_trade(
        side, price, amount, usdt_value, grid_level, profit_loss,
        balance_after, market_price,
        buy_price=buy_price, sell_target=sell_target,
        fees=fees, notes=notes,
        timestamp=datetime.now().isoformat(),
        conn=conn, commit=commit,
    )

def calculate_grid_levels(lower, upper, levels):
    """Build an evenly-spaced list of grid price levels."""
    step = (upper - lower) / levels
    return [round(lower + i * step, 4) for i in range(levels + 1)]

def get_current_zone(price, grid_levels):
    """Return the zone index that contains the given price."""
    for i in range(len(grid_levels) - 1):
        if grid_levels[i] <= price < grid_levels[i + 1]:
            return i
    return len(grid_levels) - 2

def get_runtime_mode_label():
    """Return a human-readable label for the current runtime mode."""
    if TEST_MODE_ENABLED:
        return "TEST MODE"
    return "LIVE" if config.EXECUTE_LIVE else "Paper Trading"

def find_oldest_open_buy_price(zone):
    """Look up the oldest unmatched BUY price for a zone from the DB."""
    try:
        conn = orders_db.get_conn()
        c = conn.cursor()
        c.execute(
            "SELECT price FROM trades WHERE side='BUY' AND grid_level=? "
            "ORDER BY id ASC LIMIT 1", (zone,)
        )
        row = c.fetchone()
        return float(row[0]) if row else None
    except Exception:
        return None

def normalize_remote_order(remote):
    """Normalise a raw exchange order dict to a consistent schema."""
    if not isinstance(remote, dict):
        return {}
    return {
        "id": remote.get("id") or remote.get("orderId"),
        "side": (remote.get("side") or "").upper(),
        "price": remote.get("price") or remote.get("avgPrice") or 0,
        "filled": remote.get("filled") or remote.get("executedQty") or 0,
        "amount": remote.get("amount") or remote.get("origQty") or 0,
        "status": remote.get("status") or remote.get("state") or "",
    }

def calculate_volatility(exchange, symbol, lookback=14):
    """Calculate recent price volatility using ATR-like method"""
    try:
        # Phemex OHLCV endpoint requires base symbol format (XRP/USDT not XRP/USDT:USDT)
        fetch_symbol = symbol.replace(':USDT', '') if ':' in symbol else symbol
        ohlcv = exchange.fetch_ohlcv(fetch_symbol, timeframe='1h', limit=lookback)
        if len(ohlcv) < 2:
            return 0.02  # Default 2%

        true_ranges = []
        for i in range(1, len(ohlcv)):
            high = ohlcv[i][2]
            low = ohlcv[i][3]
            prev_close = ohlcv[i-1][4]
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            true_ranges.append(tr / prev_close)  # Normalized

        return sum(true_ranges) / len(true_ranges)
    except Exception as e:
        log.error(f"Volatility calculation error: {e}")
        return 0.02

def get_dynamic_order_size(base_size, volatility, vol_threshold_low=0.01, vol_threshold_high=0.05):
    """
    Adjust order size based on volatility:
    - Low volatility: Increase size (more confidence)
    - High volatility: Decrease size (reduce risk)
    """
    if volatility < vol_threshold_low:
        multiplier = 1.5  # 50% more
    elif volatility > vol_threshold_high:
        multiplier = 0.5  # 50% less
    else:
        # Linear interpolation
        multiplier = 1.0 - 0.5 * (volatility - vol_threshold_low) / (vol_threshold_high - vol_threshold_low)

    return round(base_size * multiplier, 2)

def detect_trend(exchange, symbol, lookback=20):
    """
    Returns: 'BULLISH', 'BEARISH', or 'SIDEWAYS'
    """
    try:
        # Phemex OHLCV endpoint requires base symbol format (XRP/USDT not XRP/USDT:USDT)
        fetch_symbol = symbol.replace(':USDT', '') if ':' in symbol else symbol
        ohlcv = exchange.fetch_ohlcv(fetch_symbol, timeframe='4h', limit=lookback)
        if len(ohlcv) < lookback:
            return 'SIDEWAYS'

        closes = [c[4] for c in ohlcv]
        start_price = closes[0]
        end_price = closes[-1]
        change_pct = (end_price - start_price) / start_price

        # Also check moving averages
        ma_short = sum(closes[-5:]) / 5
        ma_long = sum(closes[-15:]) / 15

        if change_pct > TREND_THRESHOLD and ma_short > ma_long:
            return 'BULLISH'
        elif change_pct < -TREND_THRESHOLD and ma_short < ma_long:
            return 'BEARISH'
        else:
            return 'SIDEWAYS'
    except Exception as e:
        log.error(f"Trend detection error: {e}")
        return 'SIDEWAYS'

def _reconcile_build_adapter(adapter, exchange_id):
    """Return adapter; build a minimal one from ExchangeAdapter if not supplied."""
    if adapter is not None:
        return adapter
    if exchange_id == "mock":
        from core.mock_exchange_adapter import MockExchangeAdapter

        return MockExchangeAdapter(exchange_id="mock")
    try:
        from core.config import (
            API_KEY,
            API_SECRET,
            EXCHANGE_MARKET_TYPE,
            EXCHANGE_TESTNET,
        )
        from exchange_adapter import ExchangeAdapter

        options = None
        if exchange_id == "phemex":
            options = {"defaultType": "swap"}
        elif exchange_id == "binance" and EXCHANGE_MARKET_TYPE in {"future", "swap", "futures"}:
            options = {"defaultType": "future"}

        return ExchangeAdapter(
            exchange_id,
            API_KEY,
            API_SECRET,
            testnet=EXCHANGE_TESTNET,
            options=options,
        )
    except Exception as e:
        log.warning("Could not build reconcile adapter: %s", e)
        return None


def _reconcile_sync_remote_orders(remote_orders, local_exchange_id):
    """Insert any remote open orders not yet in the local DB."""
    inserted = 0
    for order in remote_orders:
        try:
            oid = order.get("id") or order.get("orderId")
            sym = order.get("symbol", SYMBOL)
            side = (order.get("side") or "").upper()
            price = float(order.get("price") or 0)
            amount = float(order.get("amount") or order.get("origQty") or 0)
            status = order.get("status") or "open"
            if not oid:
                continue
            existing = orders_db.find_order_by_exchange_id(local_exchange_id, oid)
            if existing:
                continue
            orders_db.insert_order(
                local_exchange_id, oid, sym, side, price, amount,
                status=status, processed=0,
            )
            inserted += 1
        except Exception as e:
            log.debug("Could not sync remote order: %s", e)
    return inserted


def _reconcile_update_db_open(adapter, db_open, local_exchange_id):
    """Refresh status/filled for locally-open orders by fetching from exchange."""
    updated = 0
    for row in db_open:
        try:
            oid = row["exchange_order_id"]
            remote = adapter.fetch_order(oid, row["symbol"] or None)
            if not remote:
                continue
            norm = normalize_remote_order(remote)
            orders_db.update_order_by_exchange_id(
                local_exchange_id, oid,
                filled=norm.get("filled") or 0,
                status=norm.get("status") or "open",
            )
            updated += 1
        except Exception as e:
            log.debug("Could not update order %s: %s",
                      row.get("exchange_order_id"), e)
    return updated


def _reconcile_process_unprocessed(adapter, unproc, local_exchange_id, trader):
    """Iterate unprocessed DB orders and log trade rows for any filled amounts."""
    from android_grid_bot_v1 import (
        _reconcile_process_single_row,
    )
    processed_count = 0
    for row in unproc:
        try:
            processed_count += _reconcile_process_single_row(
                adapter, row, local_exchange_id, trader
            )
        except Exception as e:
            try:
                exch_id = row["exchange_order_id"]
            except Exception:
                exch_id = "unknown"
            log.error(
                "Reconciliation: failed to log trade for %s: %s", exch_id, e
            )
    return processed_count


def reconcile_once(adapter=None, symbol=None, exchange_id=None, trader=None):
    """Fetch open orders from exchange and ensure local orders table matches."""
    if exchange_id is None:
        exchange_id = EXCHANGE_ID

    orders_db.ensure_orders_table()

    adapter = _reconcile_build_adapter(adapter, exchange_id)
    if adapter is None:
        return

    local_exchange_id = getattr(adapter, "exchange_id", exchange_id)

    _metrics_inc("reconcile.cycles")
    try:
        remote_orders = adapter.fetch_open_orders(symbol)
    except Exception as e:
        _metrics_inc("reconcile.errors")
        _emit_event(
            "reconcile.fetch_open_orders_error",
            level="warning",
            exchange_id=local_exchange_id,
            error=str(e),
        )
        log.error("Reconciliation: failed to fetch open orders: %s", e)
        remote_orders = []

    inserted = _reconcile_sync_remote_orders(remote_orders, local_exchange_id)
    _metrics_inc("reconcile.inserted", inserted)
    log.info("Reconciliation: inserted %d missing orders from %s", inserted, local_exchange_id)

    db_open = orders_db.get_open_orders_from_db(local_exchange_id)
    updated = _reconcile_update_db_open(adapter, db_open, local_exchange_id)
    _metrics_inc("reconcile.updated", updated)
    log.info("Reconciliation: refreshed %d local open orders against %s", updated, local_exchange_id)

    try:
        unproc = orders_db.get_unprocessed_orders(local_exchange_id)
    except Exception:
        unproc = []
    processed_count = _reconcile_process_unprocessed(adapter, unproc, local_exchange_id, trader)
    _metrics_inc("reconcile.processed", processed_count)
    _emit_event(
        "reconcile.cycle",
        exchange_id=local_exchange_id,
        symbol=symbol,
        fetched_open=len(remote_orders),
        inserted=inserted,
        updated=updated,
        processed=processed_count,
    )
    if processed_count:
        log.info("Reconciliation: logged %d filled orders for %s", processed_count, local_exchange_id)

async def reconcile_worker(adapter=None, symbol=None, exchange_id=None, trader=None, interval=300):
    log.info(f"Reconciliation worker starting (interval={interval}s)")
    while not config.stop_flag.is_set():
        try:
            reconcile_once(adapter, symbol, exchange_id, trader)
        except Exception as e:
            log.error(f"Reconciliation worker error: {e}")
        await asyncio.sleep(interval)

class PaperTrader:
    def __init__(self, balance, grid_levels):
        self.usdt_balance      = balance
        self.xrp_balance       = 0.0
        self.total_trades      = 0
        self.total_profit      = 0.0
        self.total_fees_paid   = 0.0  # NEW: Fee tracking
        self.start_balance     = balance
        self.grid_levels       = grid_levels
        self.num_zones         = len(grid_levels) - 1
        self.step_size         = grid_levels[1] - grid_levels[0]
        self.min_move          = self.step_size * MIN_PROFIT_RATIO
        self.zone_exposure     = [0] * self.num_zones  # -1 short, 0 flat, +1 long
        self.zone_position_amount = [0.0] * self.num_zones
        self.zone_entry_notional = [0.0] * self.num_zones
        self.zone_holding      = [False] * self.num_zones
        self.zone_buy_price    = [0.0]  * self.num_zones
        self.zone_sell_target  = [0.0]  * self.num_zones  # buy + one step
        self.zone_is_catchup   = [False] * self.num_zones
        self.grid_lower        = GRID_LOWER
        self.grid_upper        = GRID_UPPER
        self.grid_levels_count = GRID_LEVELS
        self.order_size        = ORDER_SIZE
        self.max_catchup_zones = MAX_CATCHUP_ZONES

        log.info(f"Grid step: ${self.step_size:.4f} | "
                 f"Min sell move: ${self.min_move:.4f}")

    def _sync_zone_holding_from_exposure(self):
        self.zone_holding = [exposure > 0 for exposure in self.zone_exposure]

    def _is_long_zone(self, zone):
        return self.zone_exposure[zone] > 0

    def _is_short_zone(self, zone):
        return self.zone_exposure[zone] < 0

    def _is_flat_zone(self, zone):
        return self.zone_exposure[zone] == 0

    def _set_zone_long(self, zone, buy_price, sell_target, is_catchup=False, base_amount=None, entry_notional=None):
        self.zone_exposure[zone] = 1
        amount = base_amount if base_amount is not None else (self.order_size / buy_price if buy_price else 0.0)
        notional = entry_notional if entry_notional is not None else self.order_size
        self.zone_position_amount[zone] = max(0.0, float(amount or 0.0))
        self.zone_entry_notional[zone] = max(0.0, float(notional or 0.0))
        self.zone_buy_price[zone] = buy_price
        self.zone_sell_target[zone] = sell_target
        self.zone_is_catchup[zone] = is_catchup
        self._sync_zone_holding_from_exposure()

    def _set_zone_short(self, zone, entry_price, cover_target, base_amount, entry_notional):
        self.zone_exposure[zone] = -1
        self.zone_position_amount[zone] = max(0.0, float(base_amount or 0.0))
        self.zone_entry_notional[zone] = max(0.0, float(entry_notional or 0.0))
        self.zone_buy_price[zone] = entry_price
        self.zone_sell_target[zone] = cover_target
        self.zone_is_catchup[zone] = False
        self._sync_zone_holding_from_exposure()

    def _clear_zone(self, zone):
        self.zone_exposure[zone] = 0
        self.zone_position_amount[zone] = 0.0
        self.zone_entry_notional[zone] = 0.0
        self.zone_buy_price[zone] = 0.0
        self.zone_sell_target[zone] = 0.0
        self.zone_is_catchup[zone] = False
        self._sync_zone_holding_from_exposure()

    def _long_zone_count(self):
        return sum(1 for exposure in self.zone_exposure if exposure > 0)

    def _short_zone_count(self):
        return sum(1 for exposure in self.zone_exposure if exposure < 0)

    def _short_notional_open(self):
        total = 0.0
        for idx in range(self.num_zones):
            if self.zone_exposure[idx] < 0:
                total += self.zone_entry_notional[idx]
        return total

    def _margin_ratio_estimate(self, current_price):
        # Simple paper-mode estimate: equity / gross short notional.
        short_notional = 0.0
        short_upnl = 0.0
        for idx in range(self.num_zones):
            if self.zone_exposure[idx] >= 0:
                continue
            amount = self.zone_position_amount[idx]
            entry = self.zone_buy_price[idx]
            short_notional += amount * current_price
            short_upnl += (entry - current_price) * amount
        if short_notional <= 0:
            return float("inf")
        equity = self.usdt_balance + self.xrp_balance * current_price + short_upnl
        return equity / short_notional if short_notional else float("inf")

    def get_positions_snapshot(self, current_price):
        """Return a summary dict of long/short/flat zone exposure and notional."""
        long_zones = []
        short_zones = []
        for idx in range(self.num_zones):
            exposure = self.zone_exposure[idx]
            if exposure > 0:
                long_zones.append(idx)
            elif exposure < 0:
                short_zones.append(idx)
        return {
            "long_zones": long_zones,
            "short_zones": short_zones,
            "flat_zones": self.num_zones - len(long_zones) - len(short_zones),
            "short_notional_open": self._short_notional_open(),
            "margin_ratio_estimate": self._margin_ratio_estimate(current_price),
        }

    def get_risk_snapshot(self, current_price):
        """Return current short risk posture against configured limits."""
        short_count = self._short_zone_count()
        short_notional = self._short_notional_open()
        margin_ratio = self._margin_ratio_estimate(current_price)
        return {
            "allow_shorts": ALLOW_SHORTS,
            "short_mode": SHORT_MODE,
            "short_count": short_count,
            "short_count_limit": MAX_SHORT_ZONES,
            "short_notional": short_notional,
            "short_notional_limit": MAX_SHORT_NOTIONAL,
            "margin_ratio": margin_ratio,
            "min_margin_ratio": MIN_MARGIN_RATIO,
            "short_count_ok": short_count <= MAX_SHORT_ZONES,
            "short_notional_ok": short_notional <= MAX_SHORT_NOTIONAL,
            "margin_ratio_ok": margin_ratio == float("inf") or margin_ratio >= MIN_MARGIN_RATIO,
        }

    def _force_close_all_shorts(self, current_price, reason):
        closed = 0
        for zone in range(self.num_zones):
            if not self._is_short_zone(zone):
                continue
            if self.close_short(current_price, self.order_size, zone, force=True):
                closed += 1
        if closed:
            send_telegram(
                f"🚨 <b>Emergency Short Close</b>\n"
                f"Reason: {reason}\n"
                f"Closed shorts: {closed}"
            )
        return closed

    def emergency_close_shorts(self, current_price, reason="manual command"):
        """Public wrapper for operator-initiated short emergency closure."""
        return self._force_close_all_shorts(current_price, reason=reason)

    def restore_open_trades_from_db(self):
        """Restore open trades and order mappings into in-memory grid state on restart."""
        self._restore_from_trades_table()
        exchange_id = getattr(getattr(self, 'exchange', None), 'exchange_id', EXCHANGE_ID)
        try:
            mappings = orders_db.get_mappings_by_exchange(exchange_id)
            self._restore_from_order_mappings(exchange_id, mappings)
        except Exception as e:
            log.debug("Mapping restore skipped or failed: %s", e)

    def _restore_from_trades_table(self):
        """First-pass restore: populate zone state from the trades table."""
        open_trades = get_open_trades_from_db(limit=100)
        restored = 0
        for t in open_trades:
            zone = t["zone"]
            if zone is None or zone < 0 or zone >= self.num_zones:
                continue
            price = t["price"]
            sell_target = t["sell_target"] or round(price + self.step_size, 4)
            self._set_zone_long(zone, price, sell_target, is_catchup=False, base_amount=t.get("amount"), entry_notional=(t.get("amount") or 0) * price)
            restored += 1
        log.info("Restored %d open trades from DB into grid state.", restored)

    def _parse_mapping_trade_context(self, mapping):
        trade_row = orders_db.get_trade_by_id(mapping['trade_id'])
        if not trade_row:
            return None
        side = (mapping['side'] or '').upper() if mapping.get('side') else ''
        price = (
            float(trade_row['price'])
            if trade_row and trade_row['price'] is not None
            else float(mapping.get('price') or 0)
        )
        zone = trade_row['grid_level'] if 'grid_level' in trade_row.keys() else None
        if zone is None:
            try:
                zone = get_current_zone(price, self.grid_levels)
            except Exception:
                zone = None
        if zone is None or zone < 0 or zone >= self.num_zones:
            return None
        return trade_row, side, price, zone

    def _apply_mapping_to_zone_state(self, trade_row, side, price, zone):
        if side == 'BUY' and self._is_flat_zone(zone):
            st = trade_row['sell_target'] if 'sell_target' in trade_row.keys() else None
            notes = trade_row['notes'] if 'notes' in trade_row.keys() else ''
            self._set_zone_long(
                zone,
                price,
                st or round(price + self.step_size, 4),
                is_catchup=bool(notes and 'catch' in notes.lower()),
                base_amount=trade_row['amount'] if 'amount' in trade_row.keys() else None,
                entry_notional=(trade_row['amount'] * price) if 'amount' in trade_row.keys() and trade_row['amount'] else None,
            )
            return 1
        if side == 'SELL' and self._is_long_zone(zone):
            self._clear_zone(zone)
            return 1
        return 0

    def _restore_from_order_mappings(self, exchange_id, mappings):
        """Second-pass restore: fill gaps and clear sold zones using order→trade map."""
        mapping_restored = 0
        for m in mappings:
            try:
                parsed = self._parse_mapping_trade_context(m)
                if parsed is None:
                    continue
                trade_row, side, price, zone = parsed
                mapping_restored += self._apply_mapping_to_zone_state(
                    trade_row, side, price, zone
                )
                try:
                    orders_db.update_order_by_exchange_id(
                        exchange_id, m['exchange_order_id'], processed=1
                    )
                except Exception:
                    pass
            except Exception:
                continue
        if mapping_restored:
            log.info(
                "Restored %d zones from order->trade mappings for %s.",
                mapping_restored, exchange_id,
            )

    def update_grid(self, new_grid_levels, lower, upper,
                    levels, order_size, max_catchup):
        self.grid_levels       = new_grid_levels
        self.num_zones         = len(new_grid_levels) - 1
        self.step_size         = new_grid_levels[1] - new_grid_levels[0]
        self.min_move          = self.step_size * MIN_PROFIT_RATIO
        self.zone_exposure     = [0] * self.num_zones
        self.zone_position_amount = [0.0] * self.num_zones
        self.zone_entry_notional = [0.0] * self.num_zones
        self.zone_holding      = [False] * self.num_zones
        self.zone_buy_price    = [0.0]  * self.num_zones
        self.zone_sell_target  = [0.0]  * self.num_zones
        self.zone_is_catchup   = [False] * self.num_zones
        self.grid_lower        = lower
        self.grid_upper        = upper
        self.grid_levels_count = levels
        self.order_size        = order_size
        self.max_catchup_zones = max_catchup
        log.info(f"Grid updated: ${lower}-${upper} "
                 f"{levels} levels | max catchup: {max_catchup}")

    def initialize_zones(self, current_price, trend='SIDEWAYS'):
        current_zone           = get_current_zone(current_price, self.grid_levels)
        self.zone_exposure     = [0] * self.num_zones
        self.zone_position_amount = [0.0] * self.num_zones
        self.zone_entry_notional = [0.0] * self.num_zones
        self.zone_holding      = [False] * self.num_zones
        self.zone_buy_price    = [0.0]  * self.num_zones
        self.zone_sell_target  = [0.0]  * self.num_zones
        self.zone_is_catchup   = [False] * self.num_zones

        # NEW: Trend-aware initialization
        if TREND_BIAS_ENABLED:
            self._open_initial_position_trend_aware(current_price, current_zone, trend)
        else:
            self._open_initial_position(current_price, current_zone)

        send_telegram(
            f"🤖 <b>Grid Bot + Agent Started</b>\n"
            f"Pair: {SYMBOL}\n"
            f"Mode: {'LIVE' if config.EXECUTE_LIVE else 'Paper Trading'}\n"
            f"Runtime Mode: {get_runtime_mode_label()}\n"
            f"Balance: ${PAPER_BALANCE} USDT\n"
            f"Grid: ${GRID_LOWER} - ${GRID_UPPER}\n"
            f"Levels: {GRID_LEVELS} | Step: ${self.step_size:.4f}\n"
            f"Order Size: ${ORDER_SIZE} USDT\n"
            f"Min Move to Sell: ${self.min_move:.4f}\n"
            f"Max Catch-up Zones: {self.max_catchup_zones}\n"
            f"Starting Price: ${current_price:.4f}\n"
            f"Starting Zone: {current_zone}/{self.num_zones - 1}\n"
            f"Trend: {trend}\n"
            f"Sell target: ${round(current_price + self.step_size, 4)}\n"
            f"Agent analysis every {AGENT_INTERVAL_HOURS}h\n\n"
            f"<i>Commands: /status /summary /analytics /trades /grid /agent /testmode on|run|off /apply /reject /export /stop /help</i>"
        )

    def _open_initial_position(self, price, zone):
        if self.usdt_balance < self.order_size:
            log.warning("Insufficient balance for initial position")
            return
        xrp_amount                      = self.order_size / price
        sell_target                     = round(price + self.step_size, 4)
        self.usdt_balance              -= self.order_size
        self.xrp_balance               += xrp_amount
        self._set_zone_long(zone, price, sell_target, is_catchup=False, base_amount=xrp_amount, entry_notional=self.order_size)
        log_trade("BUY", price, xrp_amount, self.order_size, zone, 0,
                  self.portfolio_value(price), price, price, sell_target,
                  0, "initial position")
        log.info(f"Initial position: {xrp_amount:.2f} XRP "
                 f"@ ${price:.4f} sell target ${sell_target:.4f}")

    def _open_initial_position_trend_aware(self, price, current_zone, trend):
        """Open initial positions with trend bias"""
        if trend == 'BULLISH':
            # Start with more positions (expect upward movement)
            zones_to_fill = min(3, self.max_catchup_zones, current_zone + 1)
            log.info(f"Bullish trend detected - opening {zones_to_fill} initial positions")
            for i in range(current_zone, max(-1, current_zone - zones_to_fill), -1):
                if i >= 0 and self.usdt_balance >= self.order_size:
                    self._open_position_at_zone(i, price)
        elif trend == 'BEARISH':
            # Start conservative (expect downward movement)
            log.info("Bearish trend detected - opening single conservative position")
            self._open_initial_position(price, current_zone)
        else:
            # Normal grid start
            log.info("Sideways trend - normal grid initialization")
            self._open_initial_position(price, current_zone)

    def _open_position_at_zone(self, zone, price):
        """Helper to open position at specific zone"""
        if self.usdt_balance < self.order_size:
            return
        xrp_amount = self.order_size / price
        sell_target = round(price + self.step_size, 4)
        self.usdt_balance -= self.order_size
        self.xrp_balance += xrp_amount
        self._set_zone_long(zone, price, sell_target, is_catchup=False, base_amount=xrp_amount, entry_notional=self.order_size)
        log_trade("BUY", price, xrp_amount, self.order_size, zone, 0,
                  self.portfolio_value(price), price, price, sell_target,
                  0, "trend-biased initial")

    # NEW: Grid rebalancing
    def check_grid_rebalance(self, current_price):
        """Auto-rebalance grid when price drifts too far"""
        if not AUTO_REBALANCE_ENABLED:
            return False

        grid_center = (self.grid_lower + self.grid_upper) / 2
        drift_pct = abs(current_price - grid_center) / grid_center

        if drift_pct > GRID_REBALANCE_THRESHOLD:
            log.info(f"Grid rebalance triggered: {drift_pct:.1%} drift")

            # Calculate new grid centered on current price
            new_range = self.grid_upper - self.grid_lower
            new_lower = round(current_price - new_range / 2, 4)
            new_upper = round(current_price + new_range / 2, 4)

            # Ensure bounds
            new_lower = max(0.50, new_lower)
            new_upper = min(5.00, new_upper)

            new_grid_levels = calculate_grid_levels(new_lower, new_upper, self.grid_levels_count)
            self.update_grid(new_grid_levels, new_lower, new_upper,
                            self.grid_levels_count, self.order_size, self.max_catchup_zones)

            send_telegram(
                f"🔄 <b>Auto Grid Rebalance</b>\n"
                f"Drift: {drift_pct:.1%}\n"
                f"Old: ${self.grid_lower} - ${self.grid_upper}\n"
                f"New: ${new_lower} - ${new_upper}\n"
                f"Price: ${current_price:.4f}"
            )
            return True
        return False

    # NEW: Position health monitoring
    def get_position_health(self, current_price):
        """Return detailed health report of all positions"""
        health = []
        for i in range(self.num_zones):
            if not self._is_long_zone(i):
                continue

            buy_price = self.zone_buy_price[i]
            current_pnl_pct = (current_price - buy_price) / buy_price * 100
            distance_to_target = (self.zone_sell_target[i] - current_price) / current_price * 100

            status = "🟢" if current_pnl_pct > 0 else "🔴"
            health.append({
                "zone": i,
                "buy_price": buy_price,
                "current_pnl_pct": current_pnl_pct,
                "distance_to_target_pct": distance_to_target,
                "is_catchup": self.zone_is_catchup[i],
                "status": status
            })

        return sorted(health, key=lambda x: x["current_pnl_pct"])

    def check_underwater_positions(self, current_price, threshold=UNDERWATER_ALERT_THRESHOLD):
        """Alert if any position is down more than threshold %"""
        health = self.get_position_health(current_price)
        underwater = [h for h in health if h["current_pnl_pct"] < threshold]

        if underwater:
            msg = f"⚠️ <b>Underwater Positions ({len(underwater)})</b>\n\n"
            for pos in underwater:
                msg += (
                    f"{pos['status']} Zone {pos['zone']}: "
                    f"{pos['current_pnl_pct']:.1f}%\n"
                    f"  Entry: ${pos['buy_price']:.4f}\n"
                    f"  Needs: {abs(pos['distance_to_target_pct']):.1f}% to target\n"
                )
            send_telegram(msg)
            log.warning(f"{len(underwater)} positions underwater > {threshold}%")

    def safety_check(self, current_price):
        if ALLOW_SHORTS and self._short_zone_count() > 0:
            margin_ratio = self._margin_ratio_estimate(current_price)
            if margin_ratio < MIN_MARGIN_RATIO:
                self._force_close_all_shorts(
                    current_price,
                    reason=(
                        f"margin ratio {margin_ratio:.3f} below min {MIN_MARGIN_RATIO:.3f}"
                    ),
                )

        portfolio = self.portfolio_value(current_price)
        loss_pct  = ((self.start_balance - portfolio) / self.start_balance) * 100
        if loss_pct >= MAX_LOSS_PCT:
            send_telegram(
                f"🚨 <b>SAFETY STOP — Max Loss</b>\n"
                f"Portfolio dropped {loss_pct:.1f}%\n"
                f"Limit: {MAX_LOSS_PCT}%\n"
                f"Bot stopping to protect capital."
            )
            stop_flag.set()
            return False
        if self.total_trades >= MAX_TOTAL_TRADES:
            send_telegram(
                f"🚨 <b>SAFETY STOP — Max Trades</b>\n"
                f"Total trades: {self.total_trades}"
            )
            stop_flag.set()
            return False
        return True

    def check_missed_sells(self, current_price):
        """
        Catch-up sells: fire any holding zones where current price
        has already reached or exceeded the zone sell target.
        Handles missed sells from restarts or connection drops.
        """
        for i in range(self.num_zones):
            if not self._is_long_zone(i):
                continue
            sell_target = self.zone_sell_target[i]
            if current_price >= sell_target:
                log.info(f"Catch-up sell: zone {i} "
                         f"target ${sell_target:.4f} "
                         f"current ${current_price:.4f}")
                self.sell(current_price, self.order_size, i)

    def _has_trade_history(self):
        """Check if the bot has previous trade history in the DB.

        Returns True if there are existing trades, indicating this is a
        restart (possible network disconnection) rather than a cold start.
        """
        try:
            conn = orders_db.get_conn()
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM trades")
            row = c.fetchone()
            count = row[0] if row else 0
            return count > 0
        except Exception:
            return False

    def check_missed_buys(self, current_price):
        """
        Catch-up buys: recover zones missed during a network disconnection.

        IMPORTANT: This ONLY triggers when there is existing trade history
        in the database, indicating the bot was previously running and went
        offline (e.g. lost cell connection). On a cold/fresh start with no
        history, catch-up is skipped entirely — there is nothing to recover.

        When catch-up IS appropriate:
        - Only fills zones within CATCHUP_PROXIMITY (3) grid levels of the
          current price, not all empty zones below.
        - Buys at the zone's upper boundary (where the grid trigger would
          have fired), not at the current market price.
        - Sell target is set one full grid step above the zone entry price.
        """
        # ── Guard: skip on cold starts ──────────────────────────────
        if not self._has_trade_history():
            log.info("Cold start detected (no trade history) — "
                     "skipping catch-up buys")
            return

        CATCHUP_PROXIMITY = 3  # only recover zones within 3 levels

        current_zone  = get_current_zone(current_price, self.grid_levels)
        catchup_count = 0
        caught_zones  = []

        # Only look at zones close to the current price (within proximity)
        lowest_zone = max(0, current_zone - CATCHUP_PROXIMITY)

        # Check zones below current zone, closest first
        for i in range(current_zone - 1, lowest_zone - 1, -1):
            if catchup_count >= self.max_catchup_zones:
                break
            if not self._is_flat_zone(i):
                continue
            if self.usdt_balance < self.order_size:
                log.warning("Insufficient USDT for catch-up buy — stopping")
                break

            # Use the zone's upper boundary as the entry price
            # (this is where the grid would have triggered the buy)
            zone_entry_price = self.grid_levels[i + 1]
            xrp_amount       = self.order_size / zone_entry_price
            sell_target       = round(zone_entry_price + self.step_size, 4)

            self.usdt_balance              -= self.order_size
            self.xrp_balance               += xrp_amount
            self.total_trades              += 1
            self._set_zone_long(i, zone_entry_price, sell_target, is_catchup=True, base_amount=xrp_amount, entry_notional=self.order_size)
            catchup_count                  += 1

            # Calculate fees for catch-up (taker fee — market order)
            fees = self.order_size * TAKER_FEE_PCT
            self.total_fees_paid += fees

            log_trade("BUY", zone_entry_price, xrp_amount, self.order_size,
                      i, 0, self.portfolio_value(current_price),
                      current_price, zone_entry_price, sell_target,
                      fees, "catch-up buy (reconnect)")
            log_catchup_trade(i, zone_entry_price, sell_target)
            caught_zones.append(
                f"Zone {i} (${self.grid_levels[i]:.4f}-"
                f"${self.grid_levels[i+1]:.4f}): "
                f"entry ${zone_entry_price:.4f} → sell ${sell_target:.4f}"
            )
            log.info(f"Catch-up buy zone {i} @ ${zone_entry_price:.4f} "
                     f"(zone boundary) sell target ${sell_target:.4f}")

        if caught_zones:
            send_telegram(
                f"🔄 <b>Catch-up Buys ({len(caught_zones)})</b>\n"
                f"Recovering zones near ${current_price:.4f} "
                f"after reconnection\n\n"
                + "\n".join(caught_zones) +
                f"\n\nUSDT left: ${self.usdt_balance:.2f}\n"
                f"Each zone sells one full step above zone entry."
            )

    def buy(self, price, usdt_amount, zone, is_taker=False):
        if not self._is_flat_zone(zone):
            log.warning(f"Zone {zone} already holding — skipping")
            return False
        if self._long_zone_count() >= MAX_OPEN_ZONES:
            log.warning("Max open zones reached — skipping")
            return False
        if self.usdt_balance < usdt_amount:
            log.warning("Insufficient USDT — skipping")
            return False

        xrp_amount = usdt_amount / price
        sell_target = round(price + self.step_size, 4)

        # NEW: Calculate fees
        fee_rate = TAKER_FEE_PCT if is_taker else MAKER_FEE_PCT
        fees = usdt_amount * fee_rate
        self.total_fees_paid += fees

        notes = f"{'taker' if is_taker else 'maker'}"
        exchange_order_id = None
        order_status = None

        # Attempt live order placement when enabled and adapter attached
        if config.EXECUTE_LIVE and getattr(self, 'exchange', None):
            try:
                if is_taker:
                    order = self.exchange.create_market_order(SYMBOL, 'buy', xrp_amount)
                else:
                    order = self.exchange.create_limit_order(SYMBOL, 'buy', price, xrp_amount)

                if isinstance(order, dict):
                    exchange_order_id = order.get('id') or order.get('orderId') or order.get('clientOrderId')
                    order_status = order.get('status') or order.get('state') or 'open'
                else:
                    exchange_order_id = str(order)

                try:
                    orders_db.insert_order(getattr(self.exchange, 'exchange_id', EXCHANGE_ID),
                                            exchange_order_id or 'unknown', SYMBOL, 'BUY', price, xrp_amount,
                                            status=order_status or 'open', processed=1)
                except Exception as e:
                    log.warning(f"Could not insert exchange order into orders DB: {e}")

                notes += f";live_order={exchange_order_id}"
            except Exception as e:
                log.error(f"Live buy failed: {e}")
                notes += ";live_failed"

        # Reserve funds / update in-memory state (same as paper mode)
        self.usdt_balance          -= usdt_amount
        self.xrp_balance           += xrp_amount
        self.total_trades          += 1
        self._set_zone_long(zone, price, sell_target, is_catchup=False, base_amount=xrp_amount, entry_notional=usdt_amount)

        if exchange_order_id:
            with orders_db.transaction() as tx_conn:
                trade_id = log_trade("BUY", price, xrp_amount, usdt_amount, zone, 0,
                          self.portfolio_value(price), price, price, sell_target,
                          fees, notes, conn=tx_conn, commit=False)
                orders_db.insert_order_trade_mapping(getattr(self.exchange, 'exchange_id', EXCHANGE_ID),
                                                     exchange_order_id, trade_id, 'BUY', price, xrp_amount, fees, conn=tx_conn, commit=False)
        else:
            trade_id = log_trade("BUY", price, xrp_amount, usdt_amount, zone, 0,
                      self.portfolio_value(price), price, price, sell_target,
                      fees, notes)

        header = "LIVE BUY" if config.EXECUTE_LIVE else "PAPER BUY"
        send_telegram(
            f"🟢 <b>{header}</b>\n"
            f"Price: ${price:.4f}\n"
            f"Amount: {xrp_amount:.2f} XRP (${usdt_amount:.2f})\n"
            f"Zone: {zone}/{self.num_zones - 1}\n"
            f"Sell target: ${sell_target:.4f}\n"
            f"Fee: ${fees:.4f}\n"
            f"Expected profit: ${round((sell_target - price) * xrp_amount - fees, 4)}\n"
            f"USDT left: ${self.usdt_balance:.2f}\n"
            f"Portfolio: ${self.portfolio_value(price):.2f}"
        )
        log.info(f"BUY zone {zone}: {xrp_amount:.2f} XRP @ ${price:.4f} "
                 f"sell target ${sell_target:.4f} | fee: ${fees:.4f} | notes: {notes}")
        return True

    def open_short(self, price, usdt_amount, zone, is_taker=False):  # noqa: C901
        """Open a short position for a zone, if enabled and risk checks pass."""
        if not ALLOW_SHORTS:
            log.warning("Short entry requested but ALLOW_SHORTS=false")
            return False
        if not self._is_flat_zone(zone):
            log.warning("Zone %s already has exposure — skipping short", zone)
            return False
        if self._short_zone_count() >= MAX_SHORT_ZONES:
            log.warning("Max short zones reached — skipping short")
            return False
        if usdt_amount > MAX_SHORT_NOTIONAL:
            log.warning("Short notional %.2f exceeds MAX_SHORT_NOTIONAL %.2f", usdt_amount, MAX_SHORT_NOTIONAL)
            return False
        if (self._short_notional_open() + usdt_amount) > MAX_SHORT_NOTIONAL:
            log.warning("Total short notional limit reached — skipping short")
            return False

        if self._margin_ratio_estimate(price) < MIN_MARGIN_RATIO:
            log.warning("Margin ratio below MIN_MARGIN_RATIO before short entry")
            return False

        if config.EXECUTE_LIVE and getattr(self, "exchange", None):
            if hasattr(self.exchange, "validate_shorting_requirements"):
                try:
                    self.exchange.validate_shorting_requirements(short_mode=SHORT_MODE)
                except Exception as exc:
                    log.error("Short capability check failed: %s", exc)
                    return False

        base_amount = usdt_amount / price
        cover_target = round(price - self.step_size, 4)
        fee_rate = TAKER_FEE_PCT if is_taker else MAKER_FEE_PCT
        fees = usdt_amount * fee_rate
        self.total_fees_paid += fees

        notes = f"short_entry;{('taker' if is_taker else 'maker')}"
        exchange_order_id = None
        order_status = None

        if config.EXECUTE_LIVE and getattr(self, "exchange", None):
            try:
                if is_taker:
                    order = self.exchange.create_market_order(SYMBOL, "sell", base_amount)
                else:
                    order = self.exchange.create_limit_order(SYMBOL, "sell", price, base_amount)
                if isinstance(order, dict):
                    exchange_order_id = order.get("id") or order.get("orderId") or order.get("clientOrderId")
                    order_status = order.get("status") or order.get("state") or "open"
                else:
                    exchange_order_id = str(order)
                try:
                    orders_db.insert_order(
                        getattr(self.exchange, "exchange_id", EXCHANGE_ID),
                        exchange_order_id or "unknown",
                        SYMBOL,
                        "SELL",
                        price,
                        base_amount,
                        status=order_status or "open",
                        processed=1,
                    )
                except Exception as e:
                    log.warning("Could not insert exchange short order into orders DB: %s", e)
                notes += f";live_order={exchange_order_id}"
            except Exception as e:
                log.error("Live short entry failed: %s", e)
                notes += ";live_failed"

        self.total_trades += 1
        self._set_zone_short(zone, price, cover_target, base_amount, usdt_amount)

        trade_id = log_trade(
            "SELL",
            price,
            base_amount,
            usdt_amount,
            zone,
            0,
            self.portfolio_value(price),
            price,
            price,
            cover_target,
            fees,
            notes,
        )
        if exchange_order_id and trade_id:
            try:
                orders_db.insert_order_trade_mapping(
                    getattr(self.exchange, "exchange_id", EXCHANGE_ID),
                    exchange_order_id,
                    trade_id,
                    "SELL",
                    price,
                    base_amount,
                    fees,
                )
            except Exception as e:
                log.warning("Could not map short entry order %s: %s", exchange_order_id, e)

        header = "LIVE SHORT OPEN" if config.EXECUTE_LIVE else "PAPER SHORT OPEN"
        send_telegram(
            f"🔻 <b>{header}</b>\n"
            f"Price: ${price:.4f}\n"
            f"Amount: {base_amount:.2f} XRP (${usdt_amount:.2f})\n"
            f"Zone: {zone}/{self.num_zones - 1}\n"
            f"Cover target: ${cover_target:.4f}\n"
            f"Fee: ${fees:.4f}"
        )
        return True

    def close_short(self, price, usdt_amount, zone, force=False):
        """Close a short position in a zone and realize directional PnL."""
        if not ALLOW_SHORTS:
            return False
        if not self._is_short_zone(zone):
            log.warning("Zone %s has no short exposure to close", zone)
            return False

        cover_target = self.zone_sell_target[zone]
        if not force and price > cover_target:
            return False

        entry_price = self.zone_buy_price[zone]
        base_amount = self.zone_position_amount[zone] or (usdt_amount / price)
        entry_notional = self.zone_entry_notional[zone] or (base_amount * entry_price)
        cover_notional = base_amount * price

        buy_fee = cover_notional * MAKER_FEE_PCT
        sell_fee = entry_notional * MAKER_FEE_PCT
        total_fee = buy_fee + sell_fee
        self.total_fees_paid += buy_fee

        notes = "short_close"
        exchange_order_id = None
        order_status = None

        if config.EXECUTE_LIVE and getattr(self, "exchange", None):
            try:
                order = self.exchange.create_limit_order(SYMBOL, "buy", price, base_amount)
                if isinstance(order, dict):
                    exchange_order_id = order.get("id") or order.get("orderId") or order.get("clientOrderId")
                    order_status = order.get("status") or order.get("state") or "open"
                else:
                    exchange_order_id = str(order)
                try:
                    orders_db.insert_order(
                        getattr(self.exchange, "exchange_id", EXCHANGE_ID),
                        exchange_order_id or "unknown",
                        SYMBOL,
                        "BUY",
                        price,
                        base_amount,
                        status=order_status or "open",
                        processed=1,
                    )
                except Exception as e:
                    log.warning("Could not insert exchange short close order into orders DB: %s", e)
                notes += f";live_order={exchange_order_id}"
            except Exception as e:
                log.error("Live short close failed: %s", e)
                notes += ";live_failed"

        gross_profit = (entry_price - price) * base_amount
        net_profit = gross_profit - total_fee
        self.total_profit += net_profit
        self.total_trades += 1

        trade_id = log_trade(
            "BUY",
            price,
            base_amount,
            cover_notional,
            zone,
            net_profit,
            self.portfolio_value(price),
            price,
            entry_price,
            cover_target,
            total_fee,
            notes,
        )
        if exchange_order_id and trade_id:
            try:
                orders_db.insert_order_trade_mapping(
                    getattr(self.exchange, "exchange_id", EXCHANGE_ID),
                    exchange_order_id,
                    trade_id,
                    "BUY",
                    price,
                    base_amount,
                    total_fee,
                )
            except Exception as e:
                log.warning("Could not map short close order %s: %s", exchange_order_id, e)

        self._clear_zone(zone)

        header = "LIVE SHORT CLOSE" if config.EXECUTE_LIVE else "PAPER SHORT CLOSE"
        send_telegram(
            f"🔺 <b>{header}</b>\n"
            f"Price: ${price:.4f}\n"
            f"Amount: {base_amount:.2f} XRP\n"
            f"Zone: {zone}/{self.num_zones - 1}\n"
            f"Entry: ${entry_price:.4f}\n"
            f"Net Profit: ${net_profit:.4f}\n"
            f"Forced: {'yes' if force else 'no'}"
        )
        return True

    def sell(self, price, usdt_amount, zone):
        if not self._is_long_zone(zone):
            log.warning(f"Zone {zone} not holding — skipping phantom sell")
            return False

        sell_target = self.zone_sell_target[zone]
        buy_price   = self.zone_buy_price[zone]

        # Only sell if price has reached the sell target
        if price < sell_target:
            log.info(f"Zone {zone} price ${price:.4f} "
                     f"below sell target ${sell_target:.4f} — holding")
            return False

        xrp_amount = self.zone_position_amount[zone] or (usdt_amount / price)
        if self.xrp_balance < xrp_amount:
            log.warning("Insufficient XRP — skipping")
            return False

        is_catchup = self.zone_is_catchup[zone]

        # NEW: Calculate fees and net profit
        entry_notional = self.zone_entry_notional[zone] or (xrp_amount * buy_price)
        sell_notional = xrp_amount * price
        buy_fee = entry_notional * MAKER_FEE_PCT
        sell_fee = sell_notional * MAKER_FEE_PCT
        total_fee = buy_fee + sell_fee
        self.total_fees_paid += sell_fee  # Buy fee already counted

        notes = f"{'catch-up' if is_catchup else 'normal'}"
        exchange_order_id = None
        order_status = None

        # Attempt live order placement if enabled
        if config.EXECUTE_LIVE and getattr(self, 'exchange', None):
            try:
                # Use a limit sell at the target price to try and get maker fee
                order = self.exchange.create_limit_order(SYMBOL, 'sell', price, xrp_amount)
                if isinstance(order, dict):
                    exchange_order_id = order.get('id') or order.get('orderId') or order.get('clientOrderId')
                    order_status = order.get('status') or order.get('state') or 'open'
                else:
                    exchange_order_id = str(order)

                try:
                    orders_db.insert_order(getattr(self.exchange, 'exchange_id', EXCHANGE_ID),
                                            exchange_order_id or 'unknown', SYMBOL, 'SELL', price, xrp_amount,
                                            status=order_status or 'open', processed=1)
                except Exception as e:
                    log.warning(f"Could not insert exchange sell order into orders DB: {e}")

                notes += f";live_order={exchange_order_id}"
            except Exception as e:
                log.error(f"Live sell failed: {e}")
                notes += ";live_failed"

        # Update balances / finalize in-memory state
        self.xrp_balance           -= xrp_amount
        self.usdt_balance          += sell_notional
        self.total_trades          += 1

        # Net profit after all fees
        gross_profit = (price - buy_price) * xrp_amount
        net_profit = gross_profit - total_fee
        self.total_profit          += net_profit

        self._clear_zone(zone)

        if exchange_order_id:
            with orders_db.transaction() as tx_conn:
                trade_id = log_trade("SELL", price, xrp_amount, sell_notional, zone, net_profit,
                          self.portfolio_value(price), price, buy_price, sell_target,
                          total_fee, notes, conn=tx_conn, commit=False)
                orders_db.insert_order_trade_mapping(getattr(self.exchange, 'exchange_id', EXCHANGE_ID),
                                                     exchange_order_id, trade_id, 'SELL', price, xrp_amount, total_fee, conn=tx_conn, commit=False)
        else:
            trade_id = log_trade("SELL", price, xrp_amount, sell_notional, zone, net_profit,
                  self.portfolio_value(price), price, buy_price, sell_target,
                  total_fee, notes)

        if is_catchup:
            complete_catchup_trade(zone, price, net_profit)

        tag = "🔄 CATCH-UP " if is_catchup else ""
        header = f"LIVE SELL {tag}" if config.EXECUTE_LIVE else f"PAPER SELL {tag}"
        send_telegram(
            f"🔴 <b>{header}</b>\n"
            f"Price: ${price:.4f}\n"
            f"Amount: {xrp_amount:.2f} XRP (${sell_notional:.2f})\n"
            f"Zone: {zone}/{self.num_zones - 1}\n"
            f"Bought at: ${buy_price:.4f}\n"
            f"Sell target was: ${sell_target:.4f}\n"
            f"Move: ${round(price - buy_price, 4)}\n"
            f"Fees: ${total_fee:.4f}\n"
            f"Net Profit: ${net_profit:.4f}\n"
            f"Total Profit: ${self.total_profit:.4f}\n"
            f"Portfolio: ${self.portfolio_value(price):.2f}"
        )
        log.info(f"SELL zone {zone}: {xrp_amount:.2f} XRP @ ${price:.4f} | "
                 f"Net Profit: ${net_profit:.4f} | Fees: ${total_fee:.4f} | Catch-up: {is_catchup} | notes: {notes}")
        return True

    def check_grid(self, current_price, last_price, exchange):  # noqa: C901
        # Catch-up sells first — handle any missed sells
        self.check_missed_sells(current_price)

        # NEW: Calculate dynamic order size based on volatility
        volatility = calculate_volatility(exchange, SYMBOL, VOLATILITY_LOOKBACK)
        dynamic_size = get_dynamic_order_size(self.order_size, volatility,
                                               VOL_THRESHOLD_LOW, VOL_THRESHOLD_HIGH) if VOLATILITY_ADJUSTMENT else self.order_size

        if dynamic_size != self.order_size:
            log.debug(f"Dynamic size adjustment: ${self.order_size} → ${dynamic_size} (vol: {volatility:.2%})")

        for i in range(self.num_zones):
            upper = self.grid_levels[i + 1]

            # Price dropped into zone from above = BUY
            if last_price >= upper and current_price < upper:
                if self._is_flat_zone(i):
                    # Use dynamic size, maker order (limit)
                    self.buy(current_price, dynamic_size, i, is_taker=False)

            # Price reached sell target for this zone = SELL
            if self._is_long_zone(i):
                if current_price >= self.zone_sell_target[i]:
                    self.sell(current_price, dynamic_size, i)

            # Price rose into zone from below = SHORT entry (when enabled)
            if ALLOW_SHORTS and last_price <= self.grid_levels[i] and current_price > self.grid_levels[i]:
                if self._is_flat_zone(i):
                    self.open_short(current_price, dynamic_size, i, is_taker=False)

            # Price returned to short cover target = close short
            if self._is_short_zone(i):
                if current_price <= self.zone_sell_target[i]:
                    self.close_short(current_price, dynamic_size, i)

    def portfolio_value(self, current_price):
        return self.usdt_balance + (self.xrp_balance * current_price)

    def status_report(self, current_price):
        pnl     = self.portfolio_value(current_price) - self.start_balance
        pnl_pct = (pnl / self.start_balance) * 100
        holding_zones = [i for i, exposure in enumerate(self.zone_exposure) if exposure > 0]
        short_zones = [i for i, exposure in enumerate(self.zone_exposure) if exposure < 0]
        holding_details = ""
        for z in holding_zones:
            target  = self.zone_sell_target[z]
            needed  = target - current_price
            tag     = " 🔄" if self.zone_is_catchup[z] else ""
            holding_details += (
                f"\n  Zone {z}{tag}: bought ${self.zone_buy_price[z]:.4f} "
                f"| target ${target:.4f} "
                f"| needs ${needed:.4f} more"
            )
        with pending_lock:
            pending_str = ""
            if pending_changes:
                mins_left = max(0, int(
                    (pending_changes["apply_at"] -
                     datetime.now()).total_seconds() / 60))
                pending_str = f"\n⏳ Pending agent changes in {mins_left}min"

        catchup = get_catchup_stats()
        return (
            f"📊 <b>BOT STATUS</b>\n"
            f"Mode: {get_runtime_mode_label()}\n"
            f"Price: ${current_price:.4f}\n"
            f"USDT: ${self.usdt_balance:.2f}\n"
            f"XRP: {self.xrp_balance:.2f}\n"
            f"Portfolio: ${self.portfolio_value(current_price):.2f}\n"
            f"P&L: ${pnl:.2f} ({pnl_pct:.1f}%)\n"
            f"Total Trades: {self.total_trades}\n"
            f"Net Profit: ${self.total_profit:.4f}\n"
            f"Total Fees Paid: ${self.total_fees_paid:.4f}\n"
            f"Catch-up Profit: ${catchup['total_profit']}\n"
            f"Open Long Zones: {len(holding_zones)}\n"
            f"Open Short Zones: {len(short_zones)}"
            f"{holding_details}"
            f"{pending_str}"
        )
