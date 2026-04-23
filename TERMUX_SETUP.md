# Termux setup notes (OnePlus 12R)

These steps assume you are on Termux on Android. They are intentionally
minimal and use a Python virtual environment to avoid polluting the system.

1) Update Termux packages

```bash
pkg update && pkg upgrade -y
pkg install -y python git clang build-essential openssl libffi
```

2) Clone or cd into the bot folder and create a virtualenv

```bash
cd ~/storage/shared/Download  # or wherever you keep the repo
python -m venv .venv
source .venv/bin/activate
pip install -U pip wheel
pip install -r requirements.txt
```

3) Create your `~/.env` (copy from `.env.example`) and set values. Keep
`EXECUTE_LIVE=false` until you have tested thoroughly.

4) Run the reconciliation tool (example)

```bash
source .venv/bin/activate
python tools/reconcile_orders.py --exchange binance --symbol XRP/USDT
```

5) Running the bot (existing main script)

```bash
source .venv/bin/activate
python start_bot.py
```

6) Recommended persistent run mode (Termux session safety)

Use a terminal multiplexer so the bot survives app backgrounding and shell
disconnects.

```bash
pkg install -y tmux
termux-wake-lock
cd /path/to/android-grid-bot
source .venv/bin/activate
tmux new -s gridbot
python start_bot.py
```

Detach from tmux without stopping the bot: press `Ctrl+b`, then `d`.
Reattach later:

```bash
tmux attach -t gridbot
```

7) Startup/restore behavior to expect after restart

- On startup, the bot restores open trades from DB into in-memory zone state.
- It runs an initial reconciliation pass against the exchange.
- A background reconciliation worker continues syncing at
  `RECONCILE_INTERVAL_SECONDS`.
- If this is your first deployment on-device, keep `EXECUTE_LIVE=false` until
  these startup stages complete cleanly.

8) Runtime observability (Telegram + logs)

The bot now exposes runtime metrics and structured events:

- Telegram command: `/metrics`
- Main log file: `~/grid_bot.log`
- Single-instance lock file: `~/andriod_grid_bot_v1.lock`
- Structured event entries are JSON objects embedded in log lines.

Useful checks:

```bash
# tail current bot logs
tail -n 100 ~/grid_bot.log

# filter structured events quickly
grep '"event"' ~/grid_bot.log | tail -n 50

# watch reconciliation cycle events
grep '"event": "reconcile.cycle"' ~/grid_bot.log | tail -n 20
```

9) Safe stop and restart

- From Telegram: `/stop` (preferred graceful stop).
- In tmux: `Ctrl+C` only if Telegram stop is unavailable.
- Restart with the same command sequence as step 6.

10) Optional lightweight auto-restart loop

If the process exits unexpectedly, you can wrap it in a simple shell loop:

```bash
cd /path/to/android-grid-bot
source .venv/bin/activate
while true; do
  python start_bot.py --force-clear-lock
  echo "Bot exited at $(date). Restarting in 10s..."
  sleep 10
done
```

Notes:
- Some scientific packages (pandas/numpy) can be heavy; Termux wheels may
  take time to build. If pip fails, consider using prebuilt wheels or
  building on-device with enough free space.
- This setup intentionally avoids Docker because Termux does not support
  Docker natively. The provided files are designed to run directly under
  Termux + Python.
