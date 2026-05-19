"""Compatibility shim for historical misspelling.

Import everything from the canonical module `android_grid_bot_v1` so existing
scripts/tests that import `andriod_grid_bot_v1` continue to work.
"""

from android_grid_bot_v1 import *  # noqa: F401,F403
