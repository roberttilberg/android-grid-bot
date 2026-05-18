import ast

source_file = "andriod_grid_bot_v1.py"

with open(source_file, "r", encoding="utf-8") as f:
    code = f.read()

tree = ast.parse(code)

functions_to_remove = [
    # Analytics
    "get_advanced_stats", "export_performance_csv", "get_performance_summary",
    "_analytics_overall_block", "_analytics_best_hours_block", "_analytics_worst_hours_block",
    "_analytics_zone_block", "_analytics_hold_time_block", "_analytics_daily_block",
    "_analytics_catchup_block",
    # Agent
    "call_groq", "run_agent", "queue_simulated_agent_change", "apply_pending_changes",
    # Telegram
    "send_telegram", "flush_telegram_queue", "get_telegram_updates", "telegram_listener",
    "_dispatch_command", "_cmd_help", "_cmd_metrics", "_cmd_status", "_cmd_summary",
    "_cmd_analytics", "_cmd_export", "_cmd_trades", "_cmd_grid", "_cmd_agent",
    "_cmd_testmode", "_cmd_apply", "_cmd_reject", "_cmd_stop",
    # Trading
    "calculate_volatility", "get_dynamic_order_size", "detect_trend",
    "reconcile_once", "reconcile_worker"
]

# Find the line ranges of functions to remove
lines_to_remove = set()
for node in ast.walk(tree):
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        if node.name in functions_to_remove or node.name == "PaperTrader":
            # node.lineno is 1-indexed, node.end_lineno is inclusive
            for line_idx in range(node.lineno - 1, node.end_lineno):
                lines_to_remove.add(line_idx)

            # also remove decorators if any
            if hasattr(node, 'decorator_list'):
                for dec in node.decorator_list:
                    for line_idx in range(dec.lineno - 1, dec.end_lineno):
                        lines_to_remove.add(line_idx)

code_lines = code.split('\n')
new_lines = []
for i, line in enumerate(code_lines):
    if i not in lines_to_remove:
        new_lines.append(line)

with open(source_file, "w", encoding="utf-8") as f:
    f.write('\n'.join(new_lines))

print("Removed extracted functions from the monolith using AST!")
