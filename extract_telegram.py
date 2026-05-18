import ast

with open("original_bot.py", "r", encoding="utf-8") as f:
    code = f.read()

tree = ast.parse(code)

telegram_funcs = [
    "send_telegram", "flush_telegram_queue", "get_telegram_updates", "telegram_listener",
    "_dispatch_command", "_cmd_help", "_cmd_metrics", "_cmd_status", "_cmd_summary",
    "_cmd_analytics", "_cmd_export", "_cmd_trades", "_cmd_grid", "_cmd_agent",
    "_cmd_testmode", "_cmd_apply", "_cmd_reject", "_cmd_stop"
]

lines = code.split("\n")
telegram_code = []
telegram_code.append("import time")
telegram_code.append("import requests")
telegram_code.append("import logging")
telegram_code.append("import threading")
telegram_code.append("from datetime import datetime")
telegram_code.append("import core.config as config")
telegram_code.append("from core.analytics import get_trade_stats, get_advanced_stats, get_catchup_stats, get_performance_summary, export_performance_csv")
telegram_code.append("from core.agent import queue_simulated_agent_change, apply_pending_changes, run_agent")
telegram_code.append("from core.analytics import _analytics_overall_block, _analytics_best_hours_block, _analytics_worst_hours_block")
telegram_code.append("from core.analytics import _analytics_zone_block, _analytics_hold_time_block, _analytics_daily_block, _analytics_catchup_block")
telegram_code.append("import db as orders_db")
telegram_code.append("import os")
telegram_code.append("TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')")
telegram_code.append("TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')")
telegram_code.append("log = logging.getLogger('gridbot.telegram')")
telegram_code.append("CMD_COOLDOWN_SECS = 5")
telegram_code.append("")
telegram_code.append("def _metrics_inc(*args): pass  # Placeholder for now")
telegram_code.append("def _emit_event(*args, **kwargs): pass  # Placeholder for now")
telegram_code.append("def _metrics_snapshot(): return {}  # Placeholder for now")
telegram_code.append("def get_price(exchange): return 0.0  # Placeholder")
telegram_code.append("def get_last_trades(n=5): return orders_db.get_last_trades(n)")
telegram_code.append("def get_open_trades_from_db(limit=10): return orders_db.get_open_trades_from_db(limit)")
telegram_code.append("")

for node in ast.walk(tree):
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        if node.name in telegram_funcs:
            start_line = node.lineno - 1
            if hasattr(node, 'decorator_list') and node.decorator_list:
                start_line = node.decorator_list[0].lineno - 1

            end_line = node.end_lineno
            func_code = "\n".join(lines[start_line:end_line])

            # Simple global string replacement for config
            func_code = func_code.replace("global last_command_time", "")
            func_code = func_code.replace("global TEST_MODE_ENABLED", "")
            func_code = func_code.replace("global pending_changes", "")

            func_code = func_code.replace("last_command_time", "config.last_command_time")
            func_code = func_code.replace("stop_flag", "config.stop_flag")
            func_code = func_code.replace("TEST_MODE_ENABLED", "config.TEST_MODE_ENABLED")
            func_code = func_code.replace("pending_changes", "config.pending_changes")
            func_code = func_code.replace("pending_lock", "config.pending_lock")
            func_code = func_code.replace("GRID_LOWER", "config.GRID_LOWER")
            func_code = func_code.replace("GRID_UPPER", "config.GRID_UPPER")
            func_code = func_code.replace("GRID_LEVELS", "config.GRID_LEVELS")
            func_code = func_code.replace("MAX_CATCHUP_ZONES", "config.MAX_CATCHUP_ZONES")

            telegram_code.append(func_code)
            telegram_code.append("")

with open("core/telegram_handler.py", "w", encoding="utf-8") as f:
    f.write("\n".join(telegram_code))

print("Created core/telegram_handler.py")
