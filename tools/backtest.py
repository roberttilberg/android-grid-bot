#!/usr/bin/env python3
"""Simple grid backtester for the XRP/USDT grid strategy.

Creates a synthetic price series (or uses optional CSV/ohlcv fetch)
and simulates the PaperTrader grid logic deterministically.
Outputs a short summary and CSV of trades when run.

Usage:
  python tools/backtest.py --ticks 500
"""
import argparse
import csv
import os
import random
from datetime import datetime


def generate_prices(seed_price=1.35, ticks=500, vol=0.0015):
    p = seed_price
    prices = []
    for _i in range(ticks):
        drift = p * random.uniform(-vol, vol)
        p = max(0.01, p + drift)
        prices.append(round(p, 6))
    return prices


def calculate_grid_levels(lower, upper, levels):
    step = (upper - lower) / levels
    return [round(lower + i * step, 6) for i in range(levels + 1)]


def run_backtest(prices, initial_balance=100.0, grid_lower=1.25, grid_upper=1.45,
                 levels=20, order_size=5.0, maker_fee_pct=0.0001, taker_fee_pct=0.0006):
    grid = calculate_grid_levels(grid_lower, grid_upper, levels)
    num_zones = len(grid) - 1
    step_size = grid[1] - grid[0]

    usdt = initial_balance
    base = 0.0
    zone_holding = [False] * num_zones
    zone_buy_price = [0.0] * num_zones
    zone_sell_target = [0.0] * num_zones
    trades = []
    total_fees = 0.0
    total_profit = 0.0

    last_price = prices[0]

    for price in prices[1:]:
        # Check for buys: price crossed down into a zone from above
        for i in range(num_zones):
            upper = grid[i + 1]
            grid[i]
            if last_price >= upper and price < upper and not zone_holding[i]:
                if usdt < order_size:
                    continue
                amt = order_size / price
                fee = order_size * maker_fee_pct
                total_fees += fee
                usdt -= order_size
                base += amt
                zone_holding[i] = True
                zone_buy_price[i] = price
                zone_sell_target[i] = round(price + step_size, 6)
                trades.append((datetime.utcnow().isoformat(), 'BUY', price, amt, order_size, i, fee, 0.0))

        # Check for sells
        for i in range(num_zones):
            if zone_holding[i] and price >= zone_sell_target[i]:
                amt = order_size / price
                if base < amt:
                    continue
                fee = order_size * maker_fee_pct
                total_fees += fee
                usdt += order_size
                base -= amt
                buy_price = zone_buy_price[i]
                gross = (price - buy_price) * amt
                net = gross - (order_size * maker_fee_pct + order_size * maker_fee_pct)
                total_profit += net
                trades.append((datetime.utcnow().isoformat(), 'SELL', price, amt, order_size, i, fee, net))
                zone_holding[i] = False
                zone_buy_price[i] = 0.0
                zone_sell_target[i] = 0.0

        last_price = price

    final_value = usdt + base * last_price
    stats = {
        'initial_balance': initial_balance,
        'final_value': round(final_value, 6),
        'total_profit': round(total_profit, 6),
        'total_fees': round(total_fees, 6),
        'remaining_usdt': round(usdt, 6),
        'remaining_base': round(base, 6),
        'trades': len(trades)
    }
    return stats, trades


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--ticks', type=int, default=500)
    parser.add_argument('--seed-price', type=float, default=1.35)
    parser.add_argument('--lower', type=float, default=1.25)
    parser.add_argument('--upper', type=float, default=1.45)
    parser.add_argument('--levels', type=int, default=20)
    parser.add_argument('--order-size', type=float, default=5.0)
    parser.add_argument('--out', type=str, default='backtest_trades.csv')
    args = parser.parse_args()

    prices = generate_prices(seed_price=args.seed_price, ticks=args.ticks)
    stats, trades = run_backtest(prices, initial_balance=100.0,
                                 grid_lower=args.lower, grid_upper=args.upper,
                                 levels=args.levels, order_size=args.order_size)

    print('Backtest complete')
    for k, v in stats.items():
        print(f'{k}: {v}')

    # Write trades CSV
    out_path = os.path.join(os.getcwd(), args.out)
    with open(out_path, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['timestamp', 'side', 'price', 'amount', 'usdt_value', 'zone', 'fee', 'profit'])
        for row in trades:
            w.writerow(row)

    print(f'Wrote trades to {out_path}')


if __name__ == '__main__':
    main()
