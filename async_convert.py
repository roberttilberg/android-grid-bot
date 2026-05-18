
# Convert Telegram Handler
with open("core/telegram_handler.py", "r", encoding="utf-8") as f:
    t_code = f.read()

t_code = t_code.replace("import time", "import time\nimport asyncio")
t_code = t_code.replace("def telegram_listener", "async def telegram_listener")
t_code = t_code.replace("time.sleep(", "await asyncio.sleep(")

with open("core/telegram_handler.py", "w", encoding="utf-8") as f:
    f.write(t_code)

# Convert Trading
with open("core/trading.py", "r", encoding="utf-8") as f:
    tr_code = f.read()

tr_code = tr_code.replace("import time", "import time\nimport asyncio")
tr_code = tr_code.replace("def reconcile_worker", "async def reconcile_worker")
tr_code = tr_code.replace("time.sleep(", "await asyncio.sleep(")

with open("core/trading.py", "w", encoding="utf-8") as f:
    f.write(tr_code)

print("Async conversions applied!")
