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

## Safe Defaults

Use [.env.example](.env.example) as the source of truth for startup flags.

- Keep `EXECUTE_LIVE=false` by default.
- Keep `ALLOW_SHORTS=false` until paper-mode validation is complete.
- Use `EXCHANGE_TESTNET=true` for first exchange-connected runs.
- Never enable mainnet execution without both `ALLOW_MAINNET_LIVE=true` and `LIVE_ACCOUNT_ISOLATED=true`.
- Never enable mainnet shorts without `ALLOW_MAINNET_SHORTS=true`.

Quick preflight checklist before any live enablement:

1. Bot starts cleanly and reconciliation runs with no startup errors.
2. `/status`, `/metrics`, `/positions`, and `/risk` return expected values.
3. `/close-shorts` command path is verified in a safe environment.
4. First live notional is tiny (canary) and manually monitored.

## Telegram Commands

- `/status` - Current status
- `/summary` - Quick performance summary
- `/analytics` - Detailed analytics dashboard
- `/metrics` - Runtime health counters
- `/trades` - Last trades
- `/grid` - Grid zones and holdings
- `/positions` - Long/short exposure snapshot
- `/risk` - Short risk and margin estimate
- `/agent` - Manual agent analysis
- `/testmode on|run|off` - Test mode controls
- `/live status` - Show runtime execution mode
- `/live arm` then reply `ON` or `OFF` - Two-step live toggle
- `/close-shorts` - Emergency close all short zones
- `/apply` - Apply pending agent changes now
- `/reject` - Reject pending changes
- `/stop` - Graceful shutdown
- `/help` - Command reference

## Short Trading Safety

Short trading is disabled by default and requires explicit opt-in.

- `ALLOW_SHORTS=false` keeps all short paths disabled.
- `SHORT_MODE` must be `futures` or `margin`.
- `MAX_SHORT_ZONES` limits concurrent short zones.
- `MAX_SHORT_NOTIONAL` limits total short exposure.
- `MIN_MARGIN_RATIO` enforces emergency close behavior for risky short exposure.
- `ALLOW_MAINNET_SHORTS=true` is required to allow shorts on mainnet.

## Docs

- Termux startup and operations guide: [TERMUX_SETUP.md](TERMUX_SETUP.md)

## License

MIT License
