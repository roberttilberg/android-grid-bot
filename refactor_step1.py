
bot_file = "android_grid_bot_v1.py"
with open(bot_file, "r", encoding="utf-8") as f:
    code = f.read()

# We need to extract the global state variables and write them to core/config.py
config_content = """# Global configuration and state
import threading

stop_flag       = threading.Event()
pending_changes = None
pending_lock    = threading.Lock()
TEST_MODE_ENABLED = False
TEST_MODE_PRICE = None
_LOCK_HELD = False
last_command_time = 0

# Config vars (can be dynamically updated by agent)
GRID_LOWER = 1.25
GRID_UPPER = 1.45
GRID_LEVELS = 20
ORDER_SIZE = 5.0
MAX_CATCHUP_ZONES = 3
AGENT_INTERVAL_HOURS = 3
"""

with open("core/config.py", "w", encoding="utf-8") as f:
    f.write(config_content)

print("Created core/config.py")

# Now we need to modify android_grid_bot_v1.py to import core.config
# and replace global variable usages.
# This is complex to do with regex alone.
