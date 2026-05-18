import ast

with open("original_bot.py", "r", encoding="utf-8") as f:
    code = f.read()

tree = ast.parse(code)

trading_funcs = [
    "calculate_volatility", "get_dynamic_order_size", "detect_trend",
    "reconcile_once", "reconcile_worker"
]

# We also need to extract PaperTrader class
lines = code.split("\n")
trading_code = []
trading_code.append("import time")
trading_code.append("import logging")
trading_code.append("import threading")
trading_code.append("from datetime import datetime")
trading_code.append("import core.config as config")
trading_code.append("import db as orders_db")
trading_code.append("from core.telegram_handler import send_telegram")
trading_code.append("log = logging.getLogger('gridbot.trading')")
trading_code.append("")
trading_code.append("def _metrics_inc(*args): pass  # Placeholder for now")
trading_code.append("def _emit_event(*args, **kwargs): pass  # Placeholder for now")
trading_code.append("def log_trade(*args, **kwargs): pass  # Placeholder for now")
trading_code.append("")

for node in ast.walk(tree):
    # Handle functions
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        if node.name in trading_funcs:
            start_line = node.lineno - 1
            if hasattr(node, 'decorator_list') and node.decorator_list:
                start_line = node.decorator_list[0].lineno - 1

            end_line = node.end_lineno
            func_code = "\n".join(lines[start_line:end_line])

            func_code = func_code.replace("global stop_flag", "")
            func_code = func_code.replace("stop_flag", "config.stop_flag")
            func_code = func_code.replace("TEST_MODE_ENABLED", "config.TEST_MODE_ENABLED")
            func_code = func_code.replace("RECONCILE_INTERVAL_SECONDS", "300")

            trading_code.append(func_code)
            trading_code.append("")

    # Handle PaperTrader class
    if isinstance(node, ast.ClassDef):
        if node.name == "PaperTrader":
            start_line = node.lineno - 1
            end_line = node.end_lineno
            cls_code = "\n".join(lines[start_line:end_line])
            trading_code.append(cls_code)
            trading_code.append("")

with open("core/trading.py", "w", encoding="utf-8") as f:
    f.write("\n".join(trading_code))

print("Created core/trading.py")
