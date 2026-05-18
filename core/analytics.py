import logging
import core.config as config
from core.config import *

import pandas as pd

import db as orders_db

log = logging.getLogger("gridbot.analytics")

def get_advanced_stats():
    """Generate comprehensive performance analytics"""
    conn = orders_db.get_conn()
    c = conn.cursor()

    # Win rate by zone
    c.execute("""
        SELECT grid_level,
               SUM(CASE WHEN profit_loss > 0 THEN 1 ELSE 0 END) as wins,
               COUNT(*) as total,
               AVG(profit_loss) as avg_pnl
        FROM trades WHERE side='SELL'
        GROUP BY grid_level
        ORDER BY grid_level
    """)
    zone_performance = c.fetchall()

    # Best/worst hours to trade
    c.execute("""
        SELECT strftime('%H', timestamp) as hour,
               AVG(profit_loss) as avg_pnl,
               COUNT(*) as trades
        FROM trades WHERE side='SELL'
        GROUP BY hour
        ORDER BY avg_pnl DESC
        LIMIT 5
    """)
    best_hours = c.fetchall()

    # Worst hours to trade
    c.execute("""
        SELECT strftime('%H', timestamp) as hour,
               AVG(profit_loss) as avg_pnl,
               COUNT(*) as trades
        FROM trades WHERE side='SELL'
        GROUP BY hour
        ORDER BY avg_pnl ASC
        LIMIT 3
    """)
    worst_hours = c.fetchall()

    # Average hold time by zone
    c.execute("""
        SELECT buy.grid_level,
               AVG(julianday(sell.timestamp) - julianday(buy.timestamp)) * 24 * 60 as avg_mins
        FROM trades buy
        JOIN trades sell ON buy.grid_level = sell.grid_level
            AND buy.side = 'BUY' AND sell.side = 'SELL'
            AND sell.timestamp > buy.timestamp
        GROUP BY buy.grid_level
        LIMIT 10
    """)
    hold_times = c.fetchall()

    # Profit factor (gross profit / gross loss)
    c.execute("""
        SELECT
            SUM(CASE WHEN profit_loss > 0 THEN profit_loss ELSE 0 END) as gross_profit,
            SUM(CASE WHEN profit_loss < 0 THEN ABS(profit_loss) ELSE 0 END) as gross_loss
        FROM trades WHERE side='SELL'
    """)
    pf_row = c.fetchone()
    gross_profit = pf_row[0] or 0
    gross_loss = pf_row[1] or 0
    profit_factor = round(gross_profit / gross_loss, 2) if gross_loss > 0 else float('inf')

    # Daily breakdown (last 7 days)
    c.execute("""
        SELECT date(timestamp) as day,
               COUNT(*) as trades,
               SUM(profit_loss) as daily_pnl
        FROM trades WHERE side='SELL'
        AND timestamp > datetime('now', '-7 days')
        GROUP BY day
        ORDER BY day DESC
    """)
    daily_breakdown = c.fetchall()

    # Largest wins and losses
    c.execute("""
        SELECT price, profit_loss, grid_level, timestamp
        FROM trades WHERE side='SELL'
        ORDER BY profit_loss DESC LIMIT 3
    """)
    biggest_wins = c.fetchall()

    c.execute("""
        SELECT price, profit_loss, grid_level, timestamp
        FROM trades WHERE side='SELL'
        ORDER BY profit_loss ASC LIMIT 3
    """)
    biggest_losses = c.fetchall()

    return {
        "zone_performance": zone_performance,
        "best_hours": best_hours,
        "worst_hours": worst_hours,
        "hold_times": hold_times,
        "profit_factor": profit_factor,
        "daily_breakdown": daily_breakdown,
        "biggest_wins": biggest_wins,
        "biggest_losses": biggest_losses,
        "gross_profit": round(gross_profit, 4),
        "gross_loss": round(gross_loss, 4)
    }

def export_performance_csv(filename="bot_performance.csv"):
    """Export all trade data to CSV file"""
    try:
        conn = orders_db.get_conn()
        df = pd.read_sql_query("SELECT * FROM trades ORDER BY timestamp DESC", conn)
        df.to_csv(filename, index=False)
        log.info(f"Exported {len(df)} trades to {filename}")
        return filename
    except Exception as e:
        log.error(f"CSV export error: {e}")
        return None

def get_performance_summary(stats, advanced):
    """Quick one-line performance summary"""
    pnl_emoji = "🟢" if stats['total_profit'] > 0 else "🔴"
    return (
        f"{pnl_emoji} Profit: ${stats['total_profit']:.4f} | "
        f"Win Rate: {stats['win_rate_pct']:.1f}% | "
        f"Trades: {stats['total_sells']} | "
        f"Factor: {advanced['profit_factor']}"
    )

def _analytics_overall_block(stats, advanced):
    pnl_emoji = "🟢" if stats['total_profit'] > 0 else "🔴"
    msg = "📊 <b>Advanced Analytics</b>\n"
    msg += "━━━━━━━━━━━━━━━━━━━━\n\n"
    msg += "<b>Overall Performance</b>\n"
    msg += f"{pnl_emoji} Total Profit: ${stats['total_profit']:.4f}\n"
    msg += f"📈 Profit Factor: {advanced['profit_factor']}\n"
    msg += (
        f"🎯 Win Rate: {stats['win_rate_pct']:.1f}%"
        f" ({stats['winning_trades']}/{stats['total_sells']})\n"
    )
    msg += f"💰 Gross Profit: ${advanced['gross_profit']:.4f}\n"
    msg += f"📉 Gross Loss: ${advanced['gross_loss']:.4f}\n\n"
    return msg

def _analytics_best_hours_block(advanced):
    msg = "<b>⏰ Best Trading Hours</b>\n"
    if advanced['best_hours']:
        for hour, avg_pnl, trades in advanced['best_hours'][:3]:
            msg += f"  {hour}:00 - ${avg_pnl:.4f} avg ({trades} trades)\n"
    else:
        msg += "  Not enough data yet\n"
    msg += "\n"
    return msg

def _analytics_worst_hours_block(advanced):
    if not advanced['worst_hours']:
        return ""
    msg = "<b>⚠️ Worst Trading Hours</b>\n"
    for hour, avg_pnl, trades in advanced['worst_hours'][:2]:
        msg += f"  {hour}:00 - ${avg_pnl:.4f} avg ({trades} trades)\n"
    msg += "\n"
    return msg

def _analytics_zone_block(advanced):
    msg = "<b>🎯 Top Performing Zones</b>\n"
    if not advanced['zone_performance']:
        return msg + "  Not enough data yet\n\n"
    sorted_zones = sorted(
        advanced['zone_performance'],
        key=lambda x: x[3] if x[3] else 0,
        reverse=True,
    )[:5]
    for zone, wins, total, avg in sorted_zones:
        win_rate = wins / total * 100 if total > 0 else 0
        msg += f"  Zone {zone}: {win_rate:.0f}% win | ${avg:.4f} avg ({total} trades)\n"
    msg += "\n"
    return msg

def _analytics_hold_time_block(advanced):
    if not advanced['hold_times']:
        return ""
    avg_hold = sum(h[1] for h in advanced['hold_times']) / len(advanced['hold_times'])
    return (
        "<b>⏱️ Average Hold Time</b>\n"
        f"  {avg_hold:.1f} minutes across zones\n\n"
    )

def _analytics_daily_block(advanced):
    msg = "<b>📅 Last 7 Days</b>\n"
    if advanced['daily_breakdown']:
        for day, trades, pnl in advanced['daily_breakdown'][:5]:
            day_emoji = "🟢" if pnl > 0 else "🔴"
            msg += f"  {day_emoji} {day}: ${pnl:.4f} ({trades} trades)\n"
    else:
        msg += "  No recent data\n"
    msg += "\n"
    return msg

def _analytics_catchup_block(catchup):
    msg = "<b>🔄 Catch-up Trades</b>\n"
    msg += f"  Total: {catchup['total']}\n"
    msg += f"  Profit: ${catchup['total_profit']:.4f}\n"
    msg += f"  Avg: ${catchup['avg_profit']:.4f}\n"
    return msg


def get_catchup_stats():
    conn = orders_db.get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT COUNT(*), SUM(profit_loss), AVG(profit_loss)
        FROM catchup_trades WHERE completed=1
    """)
    row = c.fetchone()
    return {
        "total": row[0] or 0,
        "total_profit": round(row[1] or 0, 4),
        "avg_profit": round(row[2] or 0, 4)
    }

def get_trade_stats():
    conn = orders_db.get_conn()
    c = conn.cursor()

    c.execute("SELECT COUNT(*) FROM trades WHERE side='BUY'")
    row = c.fetchone()
    total_buys = row[0] if row else 0

    c.execute("SELECT COUNT(*) FROM trades WHERE side='SELL'")
    row = c.fetchone()
    total_sells = row[0] if row else 0

    c.execute("""SELECT SUM(profit_loss), AVG(profit_loss),
              MAX(profit_loss), MIN(profit_loss)
              FROM trades WHERE side='SELL'""")
    profit_row = c.fetchone()
    total_profit = profit_row[0] or 0 if profit_row else 0
    avg_profit   = profit_row[1] or 0 if profit_row else 0
    max_profit   = profit_row[2] or 0 if profit_row else 0
    min_profit   = profit_row[3] or 0 if profit_row else 0

    c.execute("""SELECT COUNT(*) FROM trades
              WHERE side='SELL' AND profit_loss > 0""")
    row = c.fetchone()
    winning_trades = row[0] if row else 0
    win_rate = (winning_trades / total_sells * 100) if total_sells > 0 else 0

    c.execute("""SELECT grid_level, COUNT(*) as cnt
              FROM trades GROUP BY grid_level ORDER BY cnt DESC LIMIT 5""")
    active_zones = c.fetchall()

    c.execute("""SELECT MAX(market_price), MIN(market_price)
              FROM trades WHERE timestamp > datetime('now', '-6 hours')""")
    price_row   = c.fetchone()
    recent_high = price_row[0] or 0 if price_row else 0
    recent_low  = price_row[1] or 0 if price_row else 0

    c.execute("""
        SELECT AVG(julianday(b.timestamp) - julianday(a.timestamp)) * 24 * 60
        FROM trades a JOIN trades b
        ON a.grid_level = b.grid_level
        AND a.side = 'BUY' AND b.side = 'SELL'
        AND b.timestamp > a.timestamp
    """)
    avg_hold_row  = c.fetchone()
    avg_hold_mins = avg_hold_row[0] or 0 if avg_hold_row else 0

    catchup = get_catchup_stats()

    return {
        "total_buys": total_buys,
        "total_sells": total_sells,
        "total_profit": round(total_profit, 4),
        "avg_profit": round(avg_profit, 4),
        "max_profit": round(max_profit, 4),
        "min_profit": round(min_profit, 4),
        "win_rate_pct": round(win_rate, 1),
        "winning_trades": winning_trades,
        "most_active_zones": active_zones,
        "recent_high": recent_high,
        "recent_low": recent_low,
        "avg_hold_mins": round(avg_hold_mins, 1),
        "catchup_total": catchup["total"],
        "catchup_profit": catchup["total_profit"],
        "catchup_avg_profit": catchup["avg_profit"]
    }
