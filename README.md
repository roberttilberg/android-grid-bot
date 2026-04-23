# android-grid-bot

[![CI](https://github.com/roberttilberg/android-grid-bot/actions/workflows/ci.yml/badge.svg)](https://github.com/roberttilberg/android-grid-bot/actions)

Lightweight grid trading bot for Android/Termux with Telegram integration,
test mode simulation, and resilient reconciliation/order-tracking.

## Features

- Live and simulated trading modes
- Telegram bot control and status
- Agent recommendations and approval queue
- Auto-evolving SQLite schema
- Structured logs and runtime metrics (`/metrics`)
- Single-instance startup safety via lock file
- CI validation (ruff, compile checks, pytest)

## Usage

1. Clone the repository.
2. Install runtime dependencies with `pip install -r requirements.txt`.
3. For linting and tests, install dev dependencies with `pip install -r requirements-dev.txt`.
4. Configure environment variables (Telegram token, exchange keys, etc.).
5. Start with `python start_bot.py`.

## Telegram Commands

- `/status` - Current status
- `/summary` - Quick performance summary
- `/analytics` - Detailed analytics dashboard
- `/metrics` - Runtime health counters
- `/trades` - Last trades
- `/grid` - Grid zones and holdings
- `/agent` - Manual agent analysis
- `/testmode on|run|off` - Test mode controls
- `/apply` - Apply pending agent changes now
- `/reject` - Reject pending changes
- `/stop` - Graceful shutdown
- `/help` - Command reference

## Docs

- Termux startup and operations guide: [TERMUX_SETUP.md](TERMUX_SETUP.md)

## License

MIT License
