import asyncio
import logging
import os
import threading
import time
from datetime import datetime, timedelta

import requests

import core.config as config
import db as orders_db
from core.agent import apply_pending_changes, queue_simulated_agent_change, run_agent
from core.analytics import (
    _analytics_best_hours_block,
    _analytics_catchup_block,
    _analytics_daily_block,
    _analytics_hold_time_block,
    _analytics_overall_block,
    _analytics_worst_hours_block,
    _analytics_zone_block,
    export_performance_csv,
    get_advanced_stats,
    get_catchup_stats,
    get_performance_summary,
    get_trade_stats,
)
from core.config import *

TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
log = logging.getLogger('gridbot.telegram')
CMD_COOLDOWN_SECS = 5
LIVE_ARM_WINDOW_SECS = 45
_live_arm_expires_at = None

def _metrics_inc(*args): pass  # Placeholder for now
def _emit_event(*args, **kwargs): pass  # Placeholder for now
def _metrics_snapshot(): return {}  # Placeholder for now
def get_price(exchange): return 0.0  # Placeholder
def get_last_trades(n=5): return orders_db.get_last_trades(n)
def get_open_trades_from_db(limit=10): return orders_db.get_open_trades_from_db(limit)

def send_telegram(message):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        resp = requests.post(url, data={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        }, timeout=10)
        if resp.ok:
            _metrics_inc("telegram.messages.sent")
        else:
            _metrics_inc("telegram.errors")
            _emit_event(
                "telegram.send_failed",
                level="warning",
                status_code=resp.status_code,
                body=resp.text[:300],
            )
    except Exception as e:
        _metrics_inc("telegram.errors")
        _emit_event("telegram.send_error", level="error", error=str(e))
        log.error(f"Telegram send error: {e}")

def flush_telegram_queue():
    try:
        url      = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
        response = requests.get(url, timeout=10)
        data     = response.json()
        if data.get("result"):
            latest_id = data["result"][-1]["update_id"]
            requests.get(url, params={"offset": latest_id + 1}, timeout=10)
            log.info(f"Flushed {len(data['result'])} pending Telegram messages")
            return latest_id + 1
        return None
    except Exception as e:
        log.error(f"Queue flush error: {e}")
        return None

def get_telegram_updates(offset=None):
    try:
        url    = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
        params = {"timeout": 10, "allowed_updates": ["message"]}
        if offset:
            params["offset"] = offset
        response = requests.get(url, params=params, timeout=15)
        return response.json()
    except Exception as e:
        log.error(f"Telegram polling error: {e}")
        return {"ok": False, "result": []}

async def telegram_listener(trader, exchange, start_offset):

    log.info("Telegram command listener started")
    offset = start_offset

    while not config.stop_flag.is_set():
        try:
            updates = get_telegram_updates(offset)
            if not updates.get("ok"):
                await asyncio.sleep(5)
                continue

            should_stop = False
            for update in updates.get("result", []):
                offset  = update["update_id"] + 1
                message = update.get("message", {})
                chat_id = str(message.get("chat", {}).get("id", ""))

                if chat_id != TELEGRAM_CHAT_ID:
                    log.warning("Blocked unauthorized command from: %s", chat_id)
                    continue

                now = time.time()
                if now - config.last_command_time < CMD_COOLDOWN_SECS:
                    continue
                config.last_command_time = now

                text = message.get("text", "").strip().lower()

                if _handle_live_arm_reply(text):
                    continue

                log.info("Command received: %s", text)

                if not text.startswith("/"):
                    continue

                should_stop = _dispatch_command(text, trader, exchange)
                if should_stop:
                    break

            if should_stop:
                break
            await asyncio.sleep(1)

        except Exception as e:
            log.error("Listener error: %s", e)
            await asyncio.sleep(5)

    log.info("Telegram listener stopped")

def _dispatch_command(text, trader, exchange):
    """Route a Telegram command text to the appropriate handler.
    Returns True if the listener loop should stop."""
    _metrics_inc("telegram.commands.total")
    _emit_event("telegram.command", command=text)
    if text == "/stop":
        _cmd_stop()
        return True
    if text.startswith("/testmode"):
        _cmd_testmode(text, trader, exchange)
    if text.startswith("/live"):
        _cmd_live(text)

    handlers = {
        "/help": _cmd_help,
        "/summary": _cmd_summary,
        "/analytics": _cmd_analytics,
        "/metrics": _cmd_metrics,
        "/export": _cmd_export,
        "/trades": _cmd_trades,
        "/status": lambda: _cmd_status(trader, exchange),
        "/grid": lambda: _cmd_grid(trader),
        "/positions": lambda: _cmd_positions(trader, exchange),
        "/risk": lambda: _cmd_risk(trader, exchange),
        "/close-shorts": lambda: _cmd_close_shorts(trader, exchange),
        "/agent": lambda: _cmd_agent(trader, exchange),
        "/apply": lambda: _cmd_apply(trader),
        "/reject": _cmd_reject,
    }
    handler = handlers.get(text)
    if handler:
        handler()
    return False

def _cmd_help():
    send_telegram(
        "🤖 <b>Available Commands</b>\n\n"
        "<b>Monitoring:</b>\n"
        "/status — price, balances, P&L\n"
        "/summary — quick performance summary\n"
        "/analytics — detailed analytics dashboard\n"
        "/metrics — runtime health counters\n"
        "/trades — last 5 trades\n"
        "/grid — grid levels and zone states\n\n"
        "/positions — long/short exposure snapshot\n"
        "/risk — short risk limits and margin estimate\n\n"
        "<b>Control:</b>\n"
        "/agent — trigger manual agent analysis\n"
        "/testmode on — enable test mode\n"
        "/testmode run — run one test simulation now\n"
        "/testmode off — disable test mode\n"
        "/live status — show execution mode\n"
        "/live arm — arm live toggle, then reply ON or OFF\n"
        "/close-shorts — emergency close all short zones\n"
        "/apply — apply pending agent changes now\n"
        "/reject — reject pending agent changes\n"
        "/stop — safely stop the bot\n\n"
        "<b>Data:</b>\n"
        "/export — download CSV of all trades\n"
        "/help — this message"
    )

def _cmd_metrics():
    m = _metrics_snapshot()
    started_at = m.get("started_at", "n/a")
    msg = "📡 <b>Runtime Metrics</b>\n\n"
    msg += f"Started: {started_at}\n"
    msg += f"Commands: {m.get('telegram.commands.total', 0)}\n"
    msg += f"Telegram sent: {m.get('telegram.messages.sent', 0)}\n"
    msg += f"Telegram errors: {m.get('telegram.errors', 0)}\n"
    msg += f"Trades logged: {m.get('trades.logged.total', 0)}\n"
    msg += (
        f"  BUY: {m.get('trades.logged.buy', 0)} | "
        f"SELL: {m.get('trades.logged.sell', 0)}\n"
    )
    msg += f"Reconcile cycles: {m.get('reconcile.cycles', 0)}\n"
    msg += f"Reconcile inserted: {m.get('reconcile.inserted', 0)}\n"
    msg += f"Reconcile updated: {m.get('reconcile.updated', 0)}\n"
    msg += f"Reconcile processed: {m.get('reconcile.processed', 0)}\n"
    msg += f"Reconcile errors: {m.get('reconcile.errors', 0)}\n"
    msg += f"Main-loop iterations: {m.get('bot.iterations', 0)}\n"
    msg += f"Main-loop errors: {m.get('bot.errors', 0)}"
    send_telegram(msg)

def _cmd_status(trader, exchange):
    try:
        current_price = get_price(exchange)
        send_telegram(trader.status_report(current_price))
    except Exception as e:
        send_telegram(f"⚠️ Could not fetch price: {e}")


def _safe_price(exchange):
    try:
        if exchange and hasattr(exchange, "fetch_ticker"):
            ticker = exchange.fetch_ticker(config.SYMBOL)
            if isinstance(ticker, dict) and ticker.get("last") is not None:
                return float(ticker["last"])
    except Exception:
        pass
    try:
        return float(get_price(exchange))
    except Exception:
        return 0.0


def _cmd_positions(trader, exchange):
    current_price = _safe_price(exchange)
    if not hasattr(trader, "get_positions_snapshot"):
        send_telegram("ℹ️ Position snapshot is not available in this runtime.")
        return
    snap = trader.get_positions_snapshot(current_price)
    send_telegram(
        "📌 <b>Positions</b>\n"
        f"Price: ${current_price:.4f}\n"
        f"Long zones: {len(snap['long_zones'])} {snap['long_zones']}\n"
        f"Short zones: {len(snap['short_zones'])} {snap['short_zones']}\n"
        f"Flat zones: {snap['flat_zones']}\n"
        f"Open short notional: ${snap['short_notional_open']:.2f}\n"
        f"Margin ratio est: {snap['margin_ratio_estimate']:.3f}"
    )


def _cmd_risk(trader, exchange):
    current_price = _safe_price(exchange)
    if not hasattr(trader, "get_risk_snapshot"):
        send_telegram("ℹ️ Risk snapshot is not available in this runtime.")
        return
    risk = trader.get_risk_snapshot(current_price)
    send_telegram(
        "🧯 <b>Risk Snapshot</b>\n"
        f"ALLOW_SHORTS: {'ON' if risk['allow_shorts'] else 'OFF'}\n"
        f"SHORT_MODE: {risk['short_mode']}\n"
        f"Short zones: {risk['short_count']}/{risk['short_count_limit']} ({'OK' if risk['short_count_ok'] else 'LIMIT'})\n"
        f"Short notional: ${risk['short_notional']:.2f}/${risk['short_notional_limit']:.2f} ({'OK' if risk['short_notional_ok'] else 'LIMIT'})\n"
        f"Margin ratio: {risk['margin_ratio']:.3f} (min {risk['min_margin_ratio']:.3f}) ({'OK' if risk['margin_ratio_ok'] else 'ALERT'})"
    )


def _cmd_close_shorts(trader, exchange):
    current_price = _safe_price(exchange)
    if not hasattr(trader, "emergency_close_shorts"):
        send_telegram("⚠️ Emergency short close is not available in this runtime.")
        return
    closed = trader.emergency_close_shorts(
        current_price,
        reason="telegram /close-shorts",
    )
    send_telegram(
        "🛑 <b>Close Shorts</b>\n"
        f"Price: ${current_price:.4f}\n"
        f"Closed zones: {closed}"
    )

def _cmd_summary():
    try:
        summary = get_performance_summary()
        stats = get_trade_stats()
        msg = "📈 <b>Quick Summary</b>\n\n"
        msg += summary + "\n\n"
        msg += "<b>Today's Stats:</b>\n"
        msg += f"  Trades: {stats['total_sells']}\n"
        msg += f"  Avg Profit: ${stats['avg_profit']:.4f}\n"
        msg += f"  Best: ${stats['max_profit']:.4f}\n"
        msg += f"  Worst: ${stats['min_profit']:.4f}\n"
        send_telegram(msg)
    except Exception as e:
        send_telegram(f"⚠️ Summary error: {e}")

def _cmd_analytics():
    try:
        stats = get_trade_stats()
        advanced = get_advanced_stats()
        catchup = get_catchup_stats()

        msg_parts = [
            _analytics_overall_block(stats, advanced),
            _analytics_best_hours_block(advanced),
            _analytics_worst_hours_block(advanced),
            _analytics_zone_block(advanced),
            _analytics_hold_time_block(advanced),
            _analytics_daily_block(advanced),
            _analytics_catchup_block(catchup),
        ]
        send_telegram("".join(msg_parts))
    except Exception as e:
        log.error("Analytics error: %s", e)
        send_telegram(f"⚠️ Analytics error: {e}")

def _cmd_export():
    try:
        filepath = export_performance_csv()
        if filepath:
            with open(filepath, 'rb') as f:
                requests.post(
                    f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument",
                    data={"chat_id": TELEGRAM_CHAT_ID},
                    files={"document": f},
                )
            send_telegram(
                "📁 <b>Performance data exported</b>\n"
                "Open in Excel or Google Sheets for deeper analysis."
            )
        else:
            send_telegram("⚠️ Export failed - check logs")
    except Exception as e:
        log.error("Export error: %s", e)
        send_telegram(f"⚠️ Export error: {e}")

def _cmd_trades():
    rows = get_last_trades(5)
    if not rows:
        send_telegram("📭 No trades recorded yet.")
        return
    msg = "📋 <b>Last 5 Trades</b>\n\n"
    for row in rows:
        ts, side, price, amount, pnl, zone, target = row
        time_str = ts[11:19]
        emoji    = "🟢" if side == "BUY" else "🔴"
        if side == "SELL":
            detail = f"P&L: ${pnl:.4f}"
        else:
            detail = f"sell target: ${target:.4f}" if target else "open"
        msg += (
            f"{emoji} {side} @ ${price:.4f}\n"
            f"   {amount:.2f} XRP | Zone {zone} | {detail}\n"
            f"   {time_str}\n\n"
        )
    send_telegram(msg)

def _cmd_grid(trader):
    msg = (
        f"📐 <b>Grid Levels</b>\n"
        f"Range: ${config.GRID_LOWER} - ${config.GRID_UPPER}\n"
        f"Step: ${trader.step_size:.4f}\n"
        f"Max Catch-up Zones: {trader.max_catchup_zones}\n\n"
    )
    open_zone_count = 0
    for i in range(trader.num_zones):
        lower = trader.grid_levels[i]
        upper = trader.grid_levels[i + 1]
        if trader.zone_holding[i]:
            open_zone_count += 1
            tag   = "🔄" if trader.zone_is_catchup[i] else "🔒"
            buy_p = trader.zone_buy_price[i]
            tgt   = trader.zone_sell_target[i]
            state = f"{tag} HOLDING bought ${buy_p:.4f} → sell ${tgt:.4f}"
        else:
            state = "⬜ empty"
        msg += f"Zone {i}: ${lower:.4f}-${upper:.4f} {state}\n"

    db_open = get_open_trades_from_db(limit=10)
    if db_open:
        msg += "\n📂 <b>Open Trades (DB)</b>\n"
        for t in db_open:
            ts = t["timestamp"][11:19] if t["timestamp"] else "--:--:--"
            tgt_text = (
                f"${t['sell_target']:.4f}"
                if t["sell_target"] and t["sell_target"] > 0
                else "n/a"
            )
            msg += (
                f"🔓 Zone {t['zone']}: buy ${t['price']:.4f} "
                f"→ target {tgt_text} ({ts})\n"
            )

    if open_zone_count == 0 and not db_open:
        msg += "\nℹ️ No open trades currently."
    send_telegram(msg)

def _cmd_agent(trader, exchange):
    try:
        current_price = get_price(exchange)
        send_telegram("🤖 Manual agent analysis triggered...")
        threading.Thread(
            target=run_agent,
            args=(trader, current_price),
            daemon=True,
        ).start()
    except Exception as e:
        send_telegram(f"⚠️ Could not trigger agent: {e}")

def _cmd_testmode(text, trader, exchange):

    if text == "/testmode on":
        config.TEST_MODE_ENABLED = True
        current_price = get_price(exchange)
        queued, msg = queue_simulated_agent_change(trader, current_price, approval_seconds=30)
        send_telegram(
            "🧪 <b>Test mode enabled</b>\n"
            "Agent and status now report TEST mode.\n"
            + ("Simulation queued." if queued else msg)
        )
        log.info("Test mode enabled by user")
    elif text == "/testmode run":
        if not config.TEST_MODE_ENABLED:
            send_telegram("ℹ️ Test mode is OFF. Use /testmode on first.")
        else:
            current_price = get_price(exchange)
            queued, msg = queue_simulated_agent_change(trader, current_price, approval_seconds=15)
            send_telegram("🧪 One-shot test simulation queued." if queued else f"ℹ️ {msg}")
    elif text == "/testmode off":
        config.TEST_MODE_ENABLED = False
        send_telegram("✅ <b>Test mode disabled</b>\nBot returned to LIVE mode.")
        log.info("Test mode disabled by user")
    else:
        send_telegram("ℹ️ Usage:\n/testmode on\n/testmode run\n/testmode off")


def _cmd_live(text):
    """Toggle runtime order execution mode without restarting the bot."""
    global _live_arm_expires_at

    if text == "/live status":
        send_telegram(
            "⚙️ <b>Execution Mode</b>\n"
            f"EXECUTE_LIVE: {'ON' if config.EXECUTE_LIVE else 'OFF'}\n"
            f"Exchange: {config.EXCHANGE_ID}\n"
            f"Testnet: {'ON' if config.EXCHANGE_TESTNET else 'OFF'}"
        )
        return

    if text == "/live arm":
        _live_arm_expires_at = datetime.now() + timedelta(seconds=LIVE_ARM_WINDOW_SECS)
        send_telegram(
            "🛡️ <b>Live toggle armed</b>\n"
            f"Reply with <b>ON</b> or <b>OFF</b> within {LIVE_ARM_WINDOW_SECS}s.\n"
            "Direct /live on and /live off are disabled."
        )
        log.warning("Runtime EXECUTE_LIVE arm enabled by Telegram command")
        return

    send_telegram("ℹ️ Usage:\n/live status\n/live arm")


def _handle_live_arm_reply(text):
    """Handle ON/OFF replies after /live arm. Returns True if consumed."""
    global _live_arm_expires_at

    if _live_arm_expires_at is None:
        return False

    if datetime.now() > _live_arm_expires_at:
        _live_arm_expires_at = None
        send_telegram("⌛ Live toggle arm expired. Send /live arm again.")
        return False

    if text in {"on", "off"}:
        desired_live = text == "on"
        if desired_live and not config.EXCHANGE_TESTNET:
            send_telegram(
                "⚠️ Refusing ON because EXCHANGE_TESTNET is OFF.\n"
                "Set EXCHANGE_TESTNET=true before enabling live execution."
            )
            _live_arm_expires_at = None
            return True

        config.EXECUTE_LIVE = desired_live
        _live_arm_expires_at = None
        mode_label = "ON" if config.EXECUTE_LIVE else "OFF"
        send_telegram(
            "✅ <b>Live toggle applied</b>\n"
            f"EXECUTE_LIVE: <b>{mode_label}</b>"
        )
        log.warning("Runtime EXECUTE_LIVE set by armed Telegram reply: %s", mode_label)
        return True

    if text in {"cancel", "/cancel"}:
        _live_arm_expires_at = None
        send_telegram("🛑 Live toggle canceled.")
        return True

    if text and not text.startswith("/"):
        send_telegram("ℹ️ Live toggle armed: reply ON or OFF, or send cancel.")
        return True

    return False

def _cmd_apply(trader):

    should_apply_now = False
    with config.pending_lock:
        if config.pending_changes:
            config.pending_changes["apply_at"] = datetime.now()
            should_apply_now = True
            log.info("Agent changes set to apply immediately by user command")
        else:
            send_telegram("ℹ️ No pending changes to apply.")
    if should_apply_now:
        send_telegram("⚡ Applying pending agent changes now...")
        apply_pending_changes(trader)

def _cmd_reject():

    with config.pending_lock:
        if config.pending_changes:
            config.pending_changes = None
            send_telegram("❌ <b>Pending changes rejected.</b>\nGrid settings unchanged.")
            log.info("Agent changes rejected by user")
        else:
            send_telegram("ℹ️ No pending changes to reject.")

def _cmd_stop():
    send_telegram(
        "🛑 <b>Stop command received</b>\n"
        "Finishing current cycle then shutting down..."
    )
    config.stop_flag.set()
