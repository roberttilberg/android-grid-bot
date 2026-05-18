import json
import logging
from datetime import datetime, timedelta

import core.config as config
from core.analytics import get_trade_stats
from core.config import *

log = logging.getLogger('gridbot.agent')

def queue_simulated_agent_change(trader, current_price, approval_seconds=30):
    # global config.pending_changes handled by config

    with config.pending_lock:
        if config.pending_changes:
            return False, "There are already pending agent changes. Use /apply or /reject first."

        new_lower = max(0.50, round(trader.grid_lower - 0.01, 4))
        new_upper = min(5.00, round(trader.grid_upper + 0.01, 4))
        new_levels = min(50, max(10, trader.grid_levels_count + 1))

        decision = {
            "new_lower": new_lower,
            "new_upper": new_upper,
            "new_levels": new_levels,
            "new_order_size": trader.order_size,
            "new_max_catchup_zones": trader.max_catchup_zones,
            "next_interval_hours": config.AGENT_INTERVAL_HOURS,
            "reasoning": "Test mode simulation queued a safe parameter tweak.",
            "changes_needed": True,
            "old_lower": trader.grid_lower,
            "old_upper": trader.grid_upper,
            "old_levels": trader.grid_levels_count,
            "old_order_size": trader.order_size,
            "old_max_catchup": trader.max_catchup_zones,
            "new_max_catchup": trader.max_catchup_zones,
        }

        apply_time = datetime.now() + timedelta(seconds=approval_seconds)
        config.pending_changes = {"decision": decision, "apply_at": apply_time}

    send_telegram(
        f"🧪 <b>Test Mode Simulation Queued</b>\n\n"
        f"Current price: ${current_price:.4f}\n"
        f"Grid: ${decision['new_lower']} - ${decision['new_upper']}\n"
        f"Levels: {decision['new_levels']} (was {decision['old_levels']})\n"
        f"Order Size: ${decision['new_order_size']}\n\n"
        f"⏳ Auto-apply in {approval_seconds}s\n"
        f"Use /apply to apply now or /reject to cancel."
    )
    log.info(f"[TESTMODE] Simulated agent decision queued for {apply_time}")
    return True, "Simulated agent change queued."

def call_groq(prompt):
    try:
        from groq import Groq
        client   = Groq(api_key=GROQ_API_KEY)
        response = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are an expert algorithmic trading agent analyzing "
                        "a grid trading bot's performance. Your job is to analyze "
                        "trade statistics and recommend grid parameter adjustments "
                        "to improve profitability. Always respond with valid JSON only. "
                        "No markdown, no explanation outside the JSON."
                    )
                },
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=1000
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        log.error(f"Groq API error: {e}")
        return None

def run_agent(trader, current_price):


    if config.TEST_MODE_ENABLED:
        log.info("[TESTMODE] run_agent intercepted; queuing simulated decision.")
        queue_simulated_agent_change(trader, current_price, approval_seconds=30)
        return

    log.info("Agent analysis starting...")
    stats = get_trade_stats()

    if stats["total_sells"] < AGENT_MIN_TRADES:
        send_telegram(
            f"🤖 <b>Agent Analysis</b>\n"
            f"Not enough completed trades yet.\n"
            f"Completed sells: {stats['total_sells']}/{AGENT_MIN_TRADES} needed\n"
            f"Next analysis in {config.AGENT_INTERVAL_HOURS}h"
        )
        log.info(f"Agent skipped — only {stats['total_sells']} sells so far")
        return

    prompt = f"""
You are analyzing a XRP/USDT grid trading bot. Current settings and performance:

CURRENT SETTINGS:
- Grid lower: ${trader.grid_lower}
- Grid upper: ${trader.grid_upper}
- Number of levels: {trader.grid_levels_count}
- Order size: ${trader.order_size} USDT
- Max catch-up zones on restart: {trader.max_catchup_zones}
- Current price: ${current_price}
- Sell logic: each buy sells at exactly one full grid step above entry price

PERFORMANCE STATISTICS:
- Total buys: {stats['total_buys']}
- Total completed sells: {stats['total_sells']}
- Win rate: {stats['win_rate_pct']}%
- Total profit: ${stats['total_profit']}
- Average profit per trade: ${stats['avg_profit']}
- Best trade: ${stats['max_profit']}
- Worst trade: ${stats['min_profit']}
- Average hold time: {stats['avg_hold_mins']} minutes
- Most active zones: {stats['most_active_zones']}
- Recent 6h price range: ${stats['recent_low']} - ${stats['recent_high']}
- Recent 6h volatility: ${round(stats['recent_high'] - stats['recent_low'], 4)}

CATCH-UP TRADE PERFORMANCE:
- Total catch-up trades completed: {stats['catchup_total']}
- Total catch-up profit: ${stats['catchup_profit']}
- Average catch-up profit: ${stats['catchup_avg_profit']}

CONSTRAINTS:
- Grid lower must be between $0.50 and current price
- Grid upper must be between current price and $5.00
- Levels must be between 10 and 50
- Order size must be between $0.50 and $10
- Max catch-up zones must be between 1 and 10
- Next analysis interval must be between 1 and 6 hours

Respond with ONLY this JSON, no other text:
{{
    "new_lower": <float>,
    "new_upper": <float>,
    "new_levels": <int>,
    "new_order_size": <float>,
    "new_max_catchup_zones": <int>,
    "next_interval_hours": <float>,
    "reasoning": "<clear explanation max 3 sentences>",
    "changes_needed": <true or false>
}}
"""

    log.info("Calling Groq LLM for analysis...")
    response = call_groq(prompt)

    if not response:
        send_telegram("⚠️ Agent analysis failed — LLM unavailable. Will retry next cycle.")
        return

    try:
        decision = json.loads(response)
        required = ["new_lower", "new_upper", "new_levels", "new_order_size",
                    "new_max_catchup_zones", "next_interval_hours",
                    "reasoning", "changes_needed"]
        if not all(k in decision for k in required):
            raise ValueError("Missing required fields in LLM response")

        decision["old_lower"]       = trader.grid_lower
        decision["old_upper"]       = trader.grid_upper
        decision["old_levels"]      = trader.grid_levels_count
        decision["old_order_size"]  = trader.order_size
        decision["old_max_catchup"] = trader.max_catchup_zones
        decision["new_max_catchup"] = decision["new_max_catchup_zones"]

        config.AGENT_INTERVAL_HOURS = decision["next_interval_hours"]

        if not decision["changes_needed"]:
            send_telegram(
                f"🤖 <b>Agent Analysis Complete</b>\n\n"
                f"<b>Decision: No changes needed</b>\n\n"
                f"<b>Reasoning:</b>\n{decision['reasoning']}\n\n"
                f"📊 {stats['total_sells']} trades | "
                f"{stats['win_rate_pct']}% win rate | "
                f"${stats['total_profit']} profit\n"
                f"🔄 Catch-up: {stats['catchup_total']} trades | "
                f"${stats['catchup_profit']} profit\n\n"
                f"Next analysis in {config.AGENT_INTERVAL_HOURS}h"
            )
            log_agent_decision(decision, applied=False, rejected=False)
            return

        apply_time = datetime.now() + timedelta(minutes=AGENT_APPROVAL_MINS)
        log.info(f"[AGENT] Setting config.pending_changes to apply at {apply_time} with decision: {decision}")
        with config.pending_lock:
            config.pending_changes = {"decision": decision, "apply_at": apply_time}

        send_telegram(
            f"🤖 <b>Agent Recommends Changes</b>\n\n"
            f"<b>Reasoning:</b>\n{decision['reasoning']}\n\n"
            f"<b>Proposed Changes:</b>\n"
            f"Grid: ${decision['new_lower']} - ${decision['new_upper']}\n"
            f"  (was ${decision['old_lower']} - ${decision['old_upper']})\n"
            f"Levels: {decision['new_levels']} (was {decision['old_levels']})\n"  # No $ sign here
            f"Order Size: ${decision['new_order_size']} (was ${decision['old_order_size']})\n"
            f"Max Catch-up: {decision['new_max_catchup_zones']} (was {decision['old_max_catchup']})\n"
            f"Next analysis: {decision['next_interval_hours']}h\n\n"
            f"📊 {stats['total_sells']} trades | {stats['win_rate_pct']}% win rate | ${stats['total_profit']} profit\n\n"
            f"⏳ <b>Applying in {AGENT_APPROVAL_MINS} minutes</b>\n"
            f"Send /apply to apply now, or /reject to cancel."
        )
        log.info(f"Agent proposed changes — applying at {apply_time}")

    except (json.JSONDecodeError, ValueError, KeyError) as e:
        log.error(f"Agent parse error: {e} | Response: {response}")
        send_telegram(f"⚠️ Agent error — could not parse LLM response.\n{e}")

def apply_pending_changes(trader):
    # global config.pending_changes handled by config, config.GRID_LOWER, config.GRID_UPPER, config.GRID_LEVELS



    try:
        with config.pending_lock:
            log.debug(f"[APPLY] Checking config.pending_changes: {config.pending_changes}")
            if not config.pending_changes:
                log.debug("[APPLY] No pending changes to apply.")
                return
            now = datetime.now()
            apply_at = config.pending_changes["apply_at"]
            log.info(f"[APPLY] Now: {now}, apply_at: {apply_at}")
            if now < apply_at:
                log.info("[APPLY] Not time yet. Waiting for approval window.")
                return
            decision = config.pending_changes["decision"]
            log.info(f"[APPLY] Applying changes: {decision}")
            config.pending_changes = None

        old_lower   = config.GRID_LOWER
        old_upper   = config.GRID_UPPER
        old_levels  = config.GRID_LEVELS
        old_size    = config.ORDER_SIZE
        old_catchup = config.MAX_CATCHUP_ZONES

        try:
            config.GRID_LOWER        = decision["new_lower"]
            config.GRID_UPPER        = decision["new_upper"]
            config.GRID_LEVELS       = decision["new_levels"]
            config.ORDER_SIZE        = decision["new_order_size"]
            config.MAX_CATCHUP_ZONES = decision["new_max_catchup_zones"]
            log.info("[APPLY] Updated global config variables.")
        except Exception as e:
            log.error(f"[APPLY] Error updating global config: {e}")
            send_telegram(f"⚠️ Error updating config: {e}")
            return

        try:
            new_grid_levels = calculate_grid_levels(config.GRID_LOWER, config.GRID_UPPER, config.GRID_LEVELS)
            trader.update_grid(new_grid_levels, config.GRID_LOWER, config.GRID_UPPER,
                               config.GRID_LEVELS, config.ORDER_SIZE, config.MAX_CATCHUP_ZONES)
            log.info("[APPLY] Called trader.update_grid with new settings.")
        except Exception as e:
            log.error(f"[APPLY] Error updating trader grid: {e}")
            send_telegram(f"⚠️ Error updating trader grid: {e}")
            return

        try:
            log_agent_decision(decision, applied=True)
            _emit_event(
                "agent.decision_applied",
                old_lower=old_lower,
                old_upper=old_upper,
                old_levels=old_levels,
                old_order_size=old_size,
                old_max_catchup=old_catchup,
                new_lower=config.GRID_LOWER,
                new_upper=config.GRID_UPPER,
                new_levels=config.GRID_LEVELS,
                new_order_size=config.ORDER_SIZE,
                new_max_catchup=config.MAX_CATCHUP_ZONES,
            )
        except Exception as e:
            log.error(f"[APPLY] Error logging agent decision: {e}")

        try:
            send_telegram(
                f"✅ <b>Agent Changes Applied</b>\n\n"
                f"Grid: ${old_lower}-${old_upper} → ${config.GRID_LOWER}-${config.GRID_UPPER}\n"
                f"Levels: {old_levels} → {config.GRID_LEVELS}\n"
                f"Order Size: ${old_size} → ${config.ORDER_SIZE}\n"
                f"Max Catch-up: {old_catchup} → {config.MAX_CATCHUP_ZONES}\n\n"
                f"Bot continuing with new settings."
            )
        except Exception as e:
            log.error(f"[APPLY] Error sending Telegram notification: {e}")

        log.info("Agent changes applied")
    except Exception as e:
        log.error(f"[APPLY] Unexpected error in apply_pending_changes: {e}")
        send_telegram(f"⚠️ Unexpected error in apply_pending_changes: {e}")
