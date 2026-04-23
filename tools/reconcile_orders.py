"""tools/reconcile_orders.py
Lightweight reconciliation utility: sync exchange open orders with local orders table.

Usage:
  python tools/reconcile_orders.py --exchange binance --symbol XRP/USDT

This is intentionally conservative: it will insert missing exchange orders into
the local `orders` table and update statuses for DB orders that changed on the
exchange.
"""
import argparse
import logging
import os

from dotenv import load_dotenv

import db
from exchange_adapter import ExchangeAdapter

load_dotenv(os.path.expanduser("~/.env"))

log = logging.getLogger("gridbot.reconcile")
logging.basicConfig(level=logging.INFO)


def get_env_creds(exchange):
    # Try <EXCHANGE>_API_KEY, fallback to generic API_KEY
    ex_up = exchange.upper()
    api_key = os.getenv(f"{ex_up}_API_KEY") or os.getenv("API_KEY")
    api_secret = os.getenv(f"{ex_up}_API_SECRET") or os.getenv("API_SECRET")
    return api_key, api_secret


def normalize_order(o):
    # CCXT order shape varies; pick common fields
    return {
        "id": o.get("id") or o.get("orderId") or o.get("clientOrderId"),
        "symbol": o.get("symbol"),
        "side": o.get("side"),
        "price": float(o.get("price") or 0),
        "amount": float(o.get("amount") or o.get("remaining") or 0),
        "filled": float(o.get("filled") or 0),
        "status": o.get("status")
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--exchange", default="binance", help="Exchange id (ccxt)")
    parser.add_argument(
        "--symbol",
        default=None,
        help="Optional symbol to limit open order fetch",
    )
    args = parser.parse_args()

    api_key, api_secret = get_env_creds(args.exchange)
    adapter = ExchangeAdapter(args.exchange, api_key=api_key, secret=api_secret)

    db.ensure_orders_table()

    log.info(f"Fetching open orders from {args.exchange} (symbol={args.symbol})")
    try:
        remote_orders = adapter.fetch_open_orders(args.symbol)
    except Exception as e:
        log.error(f"Failed to fetch open orders: {e}")
        return

    inserted = 0
    for ro in remote_orders:
        o = normalize_order(ro)
        if not o["id"]:
            continue
        existing = db.find_order_by_exchange_id(args.exchange, o["id"])
        if existing:
            # update filled/status if changed
            db.update_order_by_exchange_id(
                args.exchange,
                o["id"],
                filled=o["filled"],
                status=o["status"],
            )
        else:
            db.insert_order(
                args.exchange,
                o["id"],
                o["symbol"],
                o["side"],
                o["price"],
                o["amount"],
                status=o["status"] or "open",
            )
            inserted += 1

    log.info(f"Inserted {inserted} missing open orders from exchange into local DB")

    # Now reconcile DB-open orders against exchange
    db_open = db.get_open_orders_from_db(args.exchange)
    updated = 0
    for row in db_open:
        exch_id = row["exchange_order_id"]
        try:
            remote = adapter.fetch_order(
                exch_id,
                row["symbol"] if row["symbol"] else None,
            )
            if remote:
                norm = normalize_order(remote)
                db.update_order_by_exchange_id(
                    args.exchange,
                    exch_id,
                    filled=norm["filled"],
                    status=norm["status"],
                )
                updated += 1
        except Exception as e:
            # Could be order not found or exchange error — log and continue
            log.debug(f"Could not fetch order {exch_id}: {e}")

    log.info(f"Reconciled {updated} local open orders against exchange")


if __name__ == "__main__":
    main()
