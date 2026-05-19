import ast

# ruff: noqa: E402,E501

source_file = "android_grid_bot_v1.py"
import os

# We will read from the git checkout of the original file
os.system("git show HEAD:android_grid_bot_v1.py > original_bot.py")

with open("original_bot.py", "r", encoding="utf-8") as f:
    code = f.read()

tree = ast.parse(code)

agent_funcs = [
    "call_groq", "run_agent", "queue_simulated_agent_change", "apply_pending_changes",
]

lines = code.split("\n")
agent_code = []
agent_code.append("import json")
agent_code.append("import logging")
agent_code.append("from datetime import datetime, timedelta")
agent_code.append("import core.config as config")
agent_code.append("from core.analytics import get_trade_stats, get_catchup_stats")
agent_code.append("import db as orders_db")
agent_code.append("log = logging.getLogger('gridbot.agent')")
agent_code.append("")

for node in ast.walk(tree):
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        if node.name in agent_funcs:
            # Add decorators if any
            start_line = node.lineno - 1
            if hasattr(node, 'decorator_list') and node.decorator_list:
                start_line = node.decorator_list[0].lineno - 1

            end_line = node.end_lineno
            func_code = "\n".join(lines[start_line:end_line])

            # Very crude replacement of globals with config.
            # A real AST transformer is safer, but this is simple text replacement.
            func_code = func_code.replace("global pending_changes", "# global pending_changes handled by config")
            func_code = func_code.replace("global AGENT_INTERVAL_HOURS, MAX_CATCHUP_ZONES, pending_changes", "")
            func_code = func_code.replace("global pending_changes, GRID_LOWER, GRID_UPPER, GRID_LEVELS", "")
            func_code = func_code.replace("global ORDER_SIZE, MAX_CATCHUP_ZONES", "")
            func_code = func_code.replace("TEST_MODE_ENABLED", "config.TEST_MODE_ENABLED")
            func_code = func_code.replace("AGENT_INTERVAL_HOURS", "config.AGENT_INTERVAL_HOURS")
            func_code = func_code.replace("MAX_CATCHUP_ZONES", "config.MAX_CATCHUP_ZONES")
            func_code = func_code.replace("pending_changes", "config.pending_changes")
            func_code = func_code.replace("pending_lock", "config.pending_lock")
            func_code = func_code.replace("GRID_LOWER", "config.GRID_LOWER")
            func_code = func_code.replace("GRID_UPPER", "config.GRID_UPPER")
            func_code = func_code.replace("GRID_LEVELS", "config.GRID_LEVELS")
            func_code = func_code.replace("ORDER_SIZE", "config.ORDER_SIZE")

            agent_code.append(func_code)
            agent_code.append("")

with open("core/agent.py", "w", encoding="utf-8") as f:
    f.write("\n".join(agent_code))

print("Created core/agent.py")
