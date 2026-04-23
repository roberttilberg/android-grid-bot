#!/usr/bin/env python3
"""Safe launcher for andriod_grid_bot_v1.py.

Checks the single-instance lock file before starting the bot and provides
clear terminal feedback for Termux sessions.
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path

LOCK_FILE = Path(os.path.expanduser("~/andriod_grid_bot_v1.lock"))
BOT_FILE = Path(__file__).resolve().parent / "andriod_grid_bot_v1.py"


def pid_running(pid):
    """Return True if the process appears alive."""
    if not pid or pid <= 0:
        return False
    if os.name == "posix":
        return Path(f"/proc/{pid}").exists()
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def parse_lock_pid(text):
    text = (text or "").strip()
    if not text:
        return 0
    head = text.split("|", 1)[0].strip()
    try:
        return int(head)
    except ValueError:
        return 0


def check_lock(force_clear):
    """Validate lock file and optionally clear stale lock.

    Returns 0 if startup may proceed; non-zero otherwise.
    """
    if not LOCK_FILE.exists():
        return 0

    raw = ""
    try:
        raw = LOCK_FILE.read_text(encoding="utf-8").strip()
    except Exception:
        raw = ""

    pid = parse_lock_pid(raw)
    if pid_running(pid):
        print(
            f"[start-bot] Refusing to start: active lock detected "
            f"(pid={pid}, file={LOCK_FILE})."
        )
        print("[start-bot] If this is unexpected, stop that process first.")
        return 1

    if not force_clear:
        print(
            f"[start-bot] Stale lock detected (pid={pid or 'unknown'}): {LOCK_FILE}"
        )
        print("[start-bot] Re-run with --force-clear-lock to remove it.")
        return 2

    try:
        LOCK_FILE.unlink()
        print(f"[start-bot] Removed stale lock: {LOCK_FILE}")
    except Exception as exc:
        print(f"[start-bot] Could not remove stale lock {LOCK_FILE}: {exc}")
        return 3

    return 0


def main():
    parser = argparse.ArgumentParser(
        description="Safe launcher for grid bot with lock-file guard."
    )
    parser.add_argument(
        "--force-clear-lock",
        action="store_true",
        help="Remove stale lock file if no process is running.",
    )
    args = parser.parse_args()

    if not BOT_FILE.exists():
        print(f"[start-bot] Bot script not found: {BOT_FILE}")
        return 4

    lock_status = check_lock(args.force_clear_lock)
    if lock_status != 0:
        return lock_status

    cmd = [sys.executable, str(BOT_FILE)]
    print(f"[start-bot] Launching: {' '.join(cmd)}")
    try:
        return subprocess.run(cmd, check=False).returncode
    except KeyboardInterrupt:
        print("\n[start-bot] Interrupted by user.")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
