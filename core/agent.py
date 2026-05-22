import json
import logging
from datetime import datetime, timedelta

import core.config as config
from core.analytics import get_trade_stats
from db import log_agent_decision

# Lazy import of SR helpers is done inside run_agent to avoid extra startup work

log = logging.getLogger('gridbot.agent')


def _emit_event(*args, **kwargs):
    pass  # Placeholder for now


def _send_telegram(message):
    # Lazy import to avoid circular import with core.telegram_handler.
    from core.telegram_handler import send_telegram

    send_telegram(message)


def _detect_sr_summary(symbol, timeframe='1h', lookback=300, max_items=8):
    """Return (sr_list, sr_summary_text) for inclusion in LLM prompts."""
    try:
        from core.sr import detect_support_resistance

        sr = detect_support_resistance(
            symbol=symbol, timeframe=timeframe, lookback=lookback
        )
    except Exception:
        sr = []

    if not sr:
        return [], ""

    lines = []
    for _idx, s in enumerate(sr[:max_items]):
        lines.append(
            f"{s['type'].upper()}: ${s['price']} (strength={s['strength']})"
        )
    summary = "Detected S/R levels:\n" + "\n".join(lines) + "\n\n"
    return sr, summary


def _build_prompt(trader, current_price, stats, sr_summary):
    """Construct the LLM prompt from pieces to satisfy line width limits."""
    parts = [
        "You are analyzing a XRP/USDT grid trading bot. Current settings and",
        "performance:",
        "",
        "CURRENT SETTINGS:",
        f"- Grid lower: ${trader.grid_lower}",
        f"- Grid upper: ${trader.grid_upper}",
        f"- Number of levels: {trader.grid_levels_count}",
        f"- Order size: ${trader.order_size} USDT",
        f"- Max catch-up zones on restart: {trader.max_catchup_zones}",
        f"- Current price: ${current_price}",
        "- Sell logic: each buy sells at exactly one full grid step",
        "  above entry price",
        "",
    ]

    if sr_summary:
        parts.append(sr_summary)

    # compute volatility once to avoid long inline expressions
    volatility = round(stats['recent_high'] - stats['recent_low'], 4)

    parts.extend([
        "PERFORMANCE STATISTICS:",
        f"- Total buys: {stats['total_buys']}",
        f"- Total completed sells: {stats['total_sells']}",
        f"- Win rate: {stats['win_rate_pct']}%",
        f"- Total profit: ${stats['total_profit']}",
        f"- Average profit per trade: ${stats['avg_profit']}",
        f"- Best trade: ${stats['max_profit']}",
        f"- Worst trade: ${stats['min_profit']}",
        f"- Average hold time: {stats['avg_hold_mins']} minutes",
        f"- Most active zones: {stats['most_active_zones']}",
        f"- Recent 6h price range: ${stats['recent_low']} - ${stats['recent_high']}",
        # compute volatility in a short variable to avoid long inline expr
        f"- Recent 6h volatility: {volatility}",
        "",
        "CATCH-UP TRADE PERFORMANCE:",
        f"- Total catch-up trades completed: {stats['catchup_total']}",
        f"- Total catch-up profit: ${stats['catchup_profit']}",
        f"- Average catch-up profit: ${stats['catchup_avg_profit']}",
        "",
        "CONSTRAINTS:",
        "- Grid lower must be between $0.50 and current price",
        "- Grid upper must be between current price and $5.00",
        "- Levels must be between 10 and 50",
        "- Order size must be between $0.50 and $10",
        "- Max catch-up zones must be between 1 and 10",
        "- Next analysis interval must be between 1 and 6 hours",
        "",
        "Respond with ONLY this JSON, no other text:",
        "{",
        "    \"new_lower\": <float>,",
        "    \"new_upper\": <float>,",
        "    \"new_levels\": <int>,",
        "    \"new_order_size\": <float>,",
        "    \"new_max_catchup_zones\": <int>,",
        "    \"next_interval_hours\": <float>,",
        "    \"reasoning\": \"<clear explanation max 3 sentences>\",",
        "    \"changes_needed\": <true or false>",
        "}",
    ])

    # Add a short optional instruction for explicit grid arrays
    parts.append(
        "OPTIONAL: You may include an additional JSON field 'new_grid_levels'",
    )
    parts.append(
        "containing an array of explicit price levels to use for the updated",
    )
    parts.append("grid. If provided it will be preferred over evenly spaced levels.")

    return "\n".join(parts)


def queue_simulated_agent_change(trader, current_price, approval_seconds=30):
    # global config.pending_changes handled by config

    with config.pending_lock:
        if config.pending_changes:
            return (
                False,
                "There are already pending agent changes. "
                "Use /apply or /reject first.",
            )

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
            "sr_levels": [],
        }

        apply_time = datetime.now() + timedelta(seconds=approval_seconds)
        config.pending_changes = {"decision": decision, "apply_at": apply_time}

    _send_telegram(
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
        client = Groq(api_key=config.GROQ_API_KEY)
        response = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are an expert algorithmic trading agent analyzing "
                        "a grid trading bot's performance. Your job is to analyze "
                        "trade statistics and recommend grid parameter adjustments "
                        "to improve profitability. "
                        "Always respond with valid JSON only. "
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


def _handle_llm_decision(response_text, trader, current_price, stats, sr):
    """Parse LLM response and either schedule or log the agent decision."""
    try:
        decision = json.loads(response_text)
    except Exception as e:
        log.error("Agent parse error: %s | Response: %s", e, response_text)
        _send_telegram(f"⚠️ Agent error - could not parse LLM response.\n{e}")
        return

    required = [
        "new_lower",
        "new_upper",
        "new_levels",
        "new_order_size",
        "new_max_catchup_zones",
        "next_interval_hours",
        "reasoning",
        "changes_needed",
    ]

    if "new_grid_levels" in decision and isinstance(decision["new_grid_levels"], list):
        try:
            gl = [float(x) for x in decision["new_grid_levels"]]
            if "new_lower" not in decision:
                decision["new_lower"] = min(gl)
            if "new_upper" not in decision:
                decision["new_upper"] = max(gl)
            if "new_levels" not in decision:
                decision["new_levels"] = max(1, len(gl) - 1)
        except (ValueError, TypeError) as e:
            log.error("Invalid explicit new_grid_levels from agent: %s", e)
            _send_telegram(f"⚠️ Agent returned invalid explicit grid levels: {e}")
    else:
        if not all(k in decision for k in required):
            log.error("Missing required fields in LLM response: %s", decision)
            _send_telegram("⚠️ Agent error - invalid response from LLM.")
            return

    # Attach SR snapshot (may be empty) for auditing and later replay
    decision["sr_levels"] = sr if sr is not None else []
    decision["sr_snapshot_ts"] = datetime.now().isoformat()

    decision["old_lower"] = trader.grid_lower
    decision["old_upper"] = trader.grid_upper
    decision["old_levels"] = trader.grid_levels_count
    decision["old_order_size"] = trader.order_size
    decision["old_max_catchup"] = trader.max_catchup_zones
    decision["new_max_catchup"] = decision.get("new_max_catchup_zones")

    config.AGENT_INTERVAL_HOURS = decision["next_interval_hours"]

    if not decision["changes_needed"]:
        part_stats = (
            f"📊 {stats['total_sells']} trades | {stats['win_rate_pct']}% "
            f"win rate | ${stats['total_profit']} profit\n"
        )
        part_catch = (
            f"🔄 Catch-up: {stats['catchup_total']} trades | "
            f"${stats['catchup_profit']} profit\n\n"
        )
        msg = "".join([
            "🤖 <b>Agent Analysis Complete</b>\n\n",
            "<b>Decision: No changes needed</b>\n\n",
            f"<b>Reasoning:</b>\n{decision['reasoning']}\n\n",
            part_stats,
            part_catch,
            f"Next analysis in {config.AGENT_INTERVAL_HOURS}h",
        ])
        _send_telegram(msg)
        log_agent_decision(decision, applied=False, rejected=False)
        return

    apply_time = datetime.now() + timedelta(minutes=config.AGENT_APPROVAL_MINS)
    log.info(
        "[AGENT] Setting config.pending_changes to apply at %s with decision: %s",
        apply_time,
        decision,
    )
    with config.pending_lock:
        config.pending_changes = {"decision": decision, "apply_at": apply_time}

    prop_lines = "".join([
        f"Grid: ${decision['new_lower']} - ${decision['new_upper']}\n",
        f"  (was ${decision['old_lower']} - ${decision['old_upper']})\n",
        f"Levels: {decision['new_levels']} (was {decision['old_levels']})\n",
    ])
    prop_sizes = "".join([
        f"Order Size: ${decision['new_order_size']} ",
        f"(was ${decision['old_order_size']})\n",
        f"Max Catch-up: {decision['new_max_catchup_zones']} ",
        f"(was {decision['old_max_catchup']})\n",
    ])
    part_stats2 = (
        f"📊 {stats['total_sells']} trades | {stats['win_rate_pct']}% "
        f"win rate | ${stats['total_profit']} profit\n\n"
    )

    msg = "".join([
        "🤖 <b>Agent Recommends Changes</b>\n\n",
        f"<b>Reasoning:</b>\n{decision['reasoning']}\n\n",
        "<b>Proposed Changes:</b>\n",
        prop_lines,
        prop_sizes,
        part_stats2,
        f"⏳ <b>Applying in {config.AGENT_APPROVAL_MINS} minutes</b>\n",
        "Send /apply to apply now, or /reject to cancel.",
    ])
    _send_telegram(msg)
    log.info(f"Agent proposed changes — applying at {apply_time}")

def run_agent(trader, current_price):
    if config.TEST_MODE_ENABLED:
        log.info("[TESTMODE] run_agent intercepted; queuing simulated decision.")
        queue_simulated_agent_change(trader, current_price, approval_seconds=30)
        return

    log.info("Agent analysis starting...")
    stats = get_trade_stats()

    if stats["total_sells"] < config.AGENT_MIN_TRADES:
        _send_telegram(
            f"🤖 <b>Agent Analysis</b>\n"
            f"Not enough completed trades yet.\n"
            "Completed sells: "
            f"{stats['total_sells']}/{config.AGENT_MIN_TRADES} needed\n"
            f"Next analysis in {config.AGENT_INTERVAL_HOURS}h"
        )
        log.info(f"Agent skipped — only {stats['total_sells']} sells so far")
        return

    # Detect support/resistance and build the LLM prompt
    sr, sr_summary = _detect_sr_summary(getattr(config, "SYMBOL", None))
    prompt = _build_prompt(trader, current_price, stats, sr_summary)

    log.info("Calling Groq LLM for analysis...")
    response = call_groq(prompt)

    if not response:
        _send_telegram(
            "⚠️ Agent analysis failed - LLM unavailable. Will retry next cycle."
        )
        return

    # Hand off parsing/scheduling to helper to simplify function
    _handle_llm_decision(response, trader, current_price, stats, sr)
    return

def apply_pending_changes(trader):
    # global config.pending_changes handled by config
    try:
        with config.pending_lock:
            log.debug(
                "[APPLY] Checking config.pending_changes: %s",
                config.pending_changes,
            )
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
            # If the agent provided an explicit list of grid levels, prefer it.
            explicit_levels = decision.get("new_grid_levels")
            if isinstance(explicit_levels, list) and len(explicit_levels) >= 2:
                try:
                    gl = [round(float(x), 4) for x in explicit_levels]
                    lower = min(gl)
                    upper = max(gl)
                    levels_count = max(1, len(gl) - 1)

                    # Populate missing numeric fields so logging remains
                    # consistent
                    decision.setdefault("new_lower", lower)
                    decision.setdefault("new_upper", upper)
                    decision.setdefault("new_levels", levels_count)
                    decision.setdefault(
                        "new_order_size",
                        decision.get("new_order_size", config.ORDER_SIZE),
                    )
                    decision.setdefault(
                        "new_max_catchup_zones",
                        decision.get(
                            "new_max_catchup_zones", config.MAX_CATCHUP_ZONES
                        ),
                    )

                    config.GRID_LOWER = lower
                    config.GRID_UPPER = upper
                    config.GRID_LEVELS = levels_count
                    config.ORDER_SIZE = decision["new_order_size"]
                    config.MAX_CATCHUP_ZONES = decision["new_max_catchup_zones"]

                    new_grid_levels = gl
                    log.info("[APPLY] Using explicit new_grid_levels from agent.")
                except Exception as e:
                    log.error("[APPLY] Invalid explicit new_grid_levels: %s", e)
                    err_msg = (
                        f"⚠️ Invalid explicit grid levels provided by agent: {e}"
                    )
                    _send_telegram(err_msg)
                    return
            else:
                # Fallback to canonical numeric fields
                config.GRID_LOWER        = decision["new_lower"]
                config.GRID_UPPER        = decision["new_upper"]
                config.GRID_LEVELS       = decision["new_levels"]
                config.ORDER_SIZE        = decision["new_order_size"]
                config.MAX_CATCHUP_ZONES = decision["new_max_catchup_zones"]
                from core.trading import calculate_grid_levels

                new_grid_levels = calculate_grid_levels(
                    config.GRID_LOWER,
                    config.GRID_UPPER,
                    config.GRID_LEVELS,
                )

            # Update trader with the chosen grid levels
            trader.update_grid(
                new_grid_levels,
                config.GRID_LOWER,
                config.GRID_UPPER,
                config.GRID_LEVELS,
                config.ORDER_SIZE,
                config.MAX_CATCHUP_ZONES,
            )
            log.info("[APPLY] Called trader.update_grid with new settings.")
        except Exception as e:
            log.error(f"[APPLY] Error updating trader grid: {e}")
            _send_telegram(f"⚠️ Error updating trader grid: {e}")
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
            _send_telegram(
                f"✅ <b>Agent Changes Applied</b>\n\n"
                "Grid: "
                f"${old_lower}-${old_upper} "
                f"→ ${config.GRID_LOWER}-${config.GRID_UPPER}\n"
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
        _send_telegram(f"⚠️ Unexpected error in apply_pending_changes: {e}")
