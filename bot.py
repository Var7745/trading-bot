#!/usr/bin/env python3
"""
Ultimate Telegram Trading Bot – Professional UI with Inline Keyboards
- Uses Binance API for stable data
- Advanced strategy (multi-timeframe, EMA200, ADX, etc.)
- Full risk management & database
- Interactive menus via buttons
- Command support for power users
"""

import logging
import os
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple, Union

import requests
import statistics
import math

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    ContextTypes,
)

# ================================
#  CONFIGURATION
# ================================
TOKEN = "8553023618:AAH7upKIA9j_zqIYtIhBRKThBOY2HlWe6Ss"
CHAT_ID = "1171112800"

# Default watchlist (Binance USDT pairs)
DEFAULT_COINS = ["BTCUSDT", "ETHUSDT", "XRPUSDT", "DOGEUSDT", "SOLUSDT"]
DEFAULT_TIMEFRAME = "15m"
DB_FILE = "bot_data.db"
LOG_FILE = "bot.log"
MIN_GRADE_SCORE = 4  # Only send signals with grade score >= 4 (A+, B)

# Grade mapping
GRADE_SCORES = {"A+": 5, "B": 4, "C": 3}
LEVERAGE_BASE = {"A+": 5, "B": 3, "C": 2}

# Position size limits
MIN_POSITION_INR = 100
MAX_POSITION_PERCENT = 5.0  # % of capital

# BTC trend filter (using Binance USDT pair)
BTC_SYMBOL = "BTCUSDT"

# Market dominance (CoinGecko)
USE_DOMINANCE_FILTER = False
DOMINANCE_THRESHOLD = 50  # % BTC dominance

# News filter (optional, requires API key)
USE_NEWS_FILTER = False
NEWS_API_KEY = ""

# Confidence threshold
MIN_CONFIDENCE_FOR_TRADE = 75

MAX_TRADES_PER_HOUR = 3
MAX_GLOBAL_EXPOSURE_PERCENT = 20  # % of capital at risk simultaneously
MAX_CONCURRENT_TRADES = 3
CONSECUTIVE_LOSSES_PAUSE = 3
CONSECUTIVE_LOSSES_RISK_REDUCTION = 5  # after 5 losses, reduce risk by 50%
RISK_REDUCTION_FACTOR = 0.5

# Fake breakout filter: candle size > 2 * average candle size => reject
FAKE_BREAKOUT_MULTIPLIER = 2.0

# Volume spike requirement
VOLUME_SPIKE_MULTIPLIER = 1.5

# Other parameters
MAX_ATR_PERCENT = 3.0
MIN_ATR_PERCENT = 0.5
COOLDOWN_HOURS = 4

# Advanced thresholds
STRICT_ADX = 30
STRICT_VOL_MULT = 1.8
STRICT_CONFIDENCE = 80
NORMAL_ADX = 25
NORMAL_VOL_MULT = 1.2
NORMAL_CONFIDENCE = 70

# Market regime filter (reduced to generate more signals)
MIN_ADX_FOR_TRADE = 15

# Trailing stop and partial exit parameters
TRAIL_STOP_ATR_MULTIPLIER = 2.0
PARTIAL_EXIT_R_MULTIPLIER = 1.0
PARTIAL_EXIT_PERCENT = 0.5

# Leverage safety: cap leverage if volatility high
LEVERAGE_ATR_THRESHOLD = 2.0

# Max drawdown stop for backtest
MAX_DRAWDOWN_PERCENT = 50.0

# Binance API endpoint
BINANCE_API = "https://api.binance.com"

ANALYSIS_INTERVAL = 600      # 10 minutes
STATUS_INTERVAL = 900        # 15 minutes

# ================================
#  DATABASE & HELPERS
# ================================
def adapt_datetime(dt):
    return dt.isoformat()

def convert_datetime(s):
    return datetime.fromisoformat(s.decode())

sqlite3.register_adapter(datetime, adapt_datetime)
sqlite3.register_converter("datetime", convert_datetime)

def init_db():
    conn = sqlite3.connect(DB_FILE, detect_types=sqlite3.PARSE_DECLTYPES, check_same_thread=False)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS watchlist (coin TEXT PRIMARY KEY)''')
    c.execute('''CREATE TABLE IF NOT EXISTS performance
                 (coin TEXT PRIMARY KEY, total_signals INTEGER DEFAULT 0, wins INTEGER DEFAULT 0)''')
    c.execute('''CREATE TABLE IF NOT EXISTS streaks
                 (coin TEXT PRIMARY KEY,
                  consecutive_wins INTEGER DEFAULT 0,
                  consecutive_losses INTEGER DEFAULT 0,
                  last_outcome TEXT,
                  paused_until DATETIME)''')
    c.execute('''CREATE TABLE IF NOT EXISTS signals
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  coin TEXT,
                  timestamp DATETIME,
                  price REAL,
                  signal TEXT,
                  confidence INTEGER,
                  grade TEXT,
                  grade_score INTEGER,
                  leverage INTEGER,
                  position_size REAL,
                  outcome TEXT,
                  pnl REAL,
                  evaluated BOOLEAN DEFAULT 0,
                  evaluated_at DATETIME)''')
    c.execute('''CREATE TABLE IF NOT EXISTS last_signal
                 (coin TEXT PRIMARY KEY, last_time DATETIME)''')
    c.execute('''CREATE TABLE IF NOT EXISTS params
                 (key TEXT PRIMARY KEY, value REAL, description TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS user_settings
                 (key TEXT PRIMARY KEY, value TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS open_trades
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  coin TEXT,
                  entry_price REAL,
                  quantity REAL,
                  entry_time DATETIME)''')
    c.execute('''CREATE TABLE IF NOT EXISTS backtest
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  coin TEXT,
                  timeframe TEXT,
                  start_date DATETIME,
                  end_date DATETIME,
                  total_trades INTEGER,
                  wins INTEGER,
                  win_rate REAL,
                  total_pnl REAL)''')
    conn.commit()

    # Insert default coins
    c.execute("SELECT COUNT(*) FROM watchlist")
    if c.fetchone()[0] == 0:
        for coin in DEFAULT_COINS:
            c.execute("INSERT OR IGNORE INTO watchlist (coin) VALUES (?)", (coin,))
        conn.commit()

    c.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", ("timeframe", DEFAULT_TIMEFRAME))
    conn.commit()

    default_params = [
        ("buy_threshold", 70.0, "Minimum confidence for BUY signal (15m)"),
        ("sell_threshold", 30.0, "Maximum confidence for SELL signal (15m)"),
        ("holding_period_minutes", 60.0, "Minutes after signal to evaluate outcome"),
        ("profit_target_percent", 1.0, "Required price move in direction to count as win"),
        ("lookback_signals", 50.0, "Number of recent signals to calculate win rate"),
        ("target_win_rate", 55.0, "Desired win rate % (adjusts thresholds)"),
        ("atr_multiplier_sl", 1.5, "ATR multiplier for stop-loss"),
        ("atr_multiplier_tp", 3.0, "ATR multiplier for take-profit"),
        ("min_adx", NORMAL_ADX, "Minimum ADX for A+ grade"),
        ("volume_multiplier", NORMAL_VOL_MULT, "Volume must be > avg * this for A+"),
        ("min_confidence", NORMAL_CONFIDENCE, "Minimum confidence for A+ grade")
    ]
    for key, val, desc in default_params:
        c.execute("INSERT OR IGNORE INTO params (key, value, description) VALUES (?, ?, ?)", (key, val, desc))
    conn.commit()

    default_user_settings = [
        ("capital", "0"),
        ("risk_percent", "1.0"),
        ("default_leverage", "3"),
        ("max_consecutive_losses", str(CONSECUTIVE_LOSSES_PAUSE)),
        ("pause_hours", "24"),
        ("win_multiplier", "1.2"),
        ("loss_multiplier", "0.8"),
        ("filter_mode", "strict"),
        ("max_atr_percent", str(MAX_ATR_PERCENT)),
        ("min_position", str(MIN_POSITION_INR)),
        ("max_position_percent", str(MAX_POSITION_PERCENT)),
        ("use_btc_filter", "true"),
        ("use_dominance_filter", "false"),
        ("use_news_filter", "false"),
        ("max_trades_per_hour", str(MAX_TRADES_PER_HOUR)),
        ("max_global_exposure_percent", str(MAX_GLOBAL_EXPOSURE_PERCENT)),
        ("consecutive_losses_pause", str(CONSECUTIVE_LOSSES_PAUSE)),
        ("consecutive_losses_risk_reduction", str(CONSECUTIVE_LOSSES_RISK_REDUCTION)),
        ("risk_reduction_factor", str(RISK_REDUCTION_FACTOR)),
        ("min_confidence_for_trade", str(MIN_CONFIDENCE_FOR_TRADE)),
        ("max_concurrent_trades", str(MAX_CONCURRENT_TRADES)),
    ]
    for key, val in default_user_settings:
        c.execute("INSERT OR IGNORE INTO user_settings (key, value) VALUES (?, ?)", (key, val))
    conn.commit()
    return conn

db_conn = init_db()
db_lock = threading.Lock()

# ----- Database helper functions (unchanged) -----
def get_watchlist():
    with db_lock:
        c = db_conn.cursor()
        c.execute("SELECT coin FROM watchlist ORDER BY coin")
        return [row[0] for row in c.fetchall()]

def add_to_watchlist(coin):
    with db_lock:
        c = db_conn.cursor()
        c.execute("INSERT OR IGNORE INTO watchlist (coin) VALUES (?)", (coin,))
        db_conn.commit()

def remove_from_watchlist(coin):
    with db_lock:
        c = db_conn.cursor()
        c.execute("DELETE FROM watchlist WHERE coin = ?", (coin,))
        db_conn.commit()

def get_timeframe():
    with db_lock:
        c = db_conn.cursor()
        c.execute("SELECT value FROM settings WHERE key = 'timeframe'")
        row = c.fetchone()
        return row[0] if row else DEFAULT_TIMEFRAME

def set_timeframe(tf):
    with db_lock:
        c = db_conn.cursor()
        c.execute("REPLACE INTO settings (key, value) VALUES (?, ?)", ("timeframe", tf))
        db_conn.commit()

def get_param(name):
    with db_lock:
        c = db_conn.cursor()
        c.execute("SELECT value FROM params WHERE key = ?", (name,))
        row = c.fetchone()
        return row[0] if row else None

def set_param(name, value):
    with db_lock:
        c = db_conn.cursor()
        c.execute("UPDATE params SET value = ? WHERE key = ?", (value, name))
        db_conn.commit()

def get_user_setting(key, default="0"):
    with db_lock:
        c = db_conn.cursor()
        c.execute("SELECT value FROM user_settings WHERE key = ?", (key,))
        row = c.fetchone()
        return row[0] if row else default

def set_user_setting(key, value):
    with db_lock:
        c = db_conn.cursor()
        c.execute("REPLACE INTO user_settings (key, value) VALUES (?, ?)", (key, value))
        db_conn.commit()

def get_cached_pair(symbol):
    with db_lock:
        c = db_conn.cursor()
        c.execute("SELECT pair FROM pair_cache WHERE symbol = ?", (symbol,))
        row = c.fetchone()
        return row[0] if row else None

def cache_pair(symbol, pair):
    with db_lock:
        c = db_conn.cursor()
        c.execute("REPLACE INTO pair_cache (symbol, pair) VALUES (?, ?)", (symbol, pair))
        db_conn.commit()

def record_signal(coin, price, signal, confidence, grade, grade_score, leverage, position_size):
    with db_lock:
        c = db_conn.cursor()
        c.execute('''INSERT INTO signals
                     (coin, timestamp, price, signal, confidence, grade, grade_score, leverage, position_size)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                  (coin, datetime.now(timezone.utc), price, signal, confidence, grade, grade_score, leverage, position_size))
        db_conn.commit()
        return c.lastrowid

def update_signal_outcome(sig_id, outcome, pnl=0.0):
    with db_lock:
        c = db_conn.cursor()
        c.execute('''UPDATE signals SET outcome = ?, pnl = ?, evaluated = 1, evaluated_at = ?
                     WHERE id = ?''', (outcome, pnl, datetime.now(timezone.utc), sig_id))
        db_conn.commit()

def get_pending_signals(older_than_minutes):
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=older_than_minutes)
    with db_lock:
        c = db_conn.cursor()
        c.execute('''SELECT id, coin, price, signal, position_size FROM signals
                     WHERE evaluated = 0 AND timestamp < ?''', (cutoff,))
        return c.fetchall()

def get_recent_win_rate(coin, lookback):
    with db_lock:
        c = db_conn.cursor()
        c.execute('''SELECT outcome FROM signals
                     WHERE coin = ? AND evaluated = 1
                     ORDER BY timestamp DESC LIMIT ?''', (coin, lookback))
        rows = c.fetchall()
        if not rows:
            return None
        wins = sum(1 for r in rows if r[0] == 'win')
        return (wins / len(rows)) * 100

def update_performance(coin, win=False):
    with db_lock:
        c = db_conn.cursor()
        c.execute('''INSERT INTO performance (coin, total_signals, wins)
                     VALUES (?, 1, ?) ON CONFLICT(coin) DO UPDATE SET
                     total_signals = total_signals + 1,
                     wins = wins + ?''',
                  (coin, 1 if win else 0, 1 if win else 0))
        db_conn.commit()

def can_send_signal(coin):
    with db_lock:
        c = db_conn.cursor()
        c.execute("SELECT last_time FROM last_signal WHERE coin = ?", (coin,))
        row = c.fetchone()
        if row:
            last = datetime.fromisoformat(row[0])
            if datetime.now(timezone.utc) - last <= timedelta(hours=COOLDOWN_HOURS):
                return False
        c.execute("SELECT paused_until FROM streaks WHERE coin = ?", (coin,))
        row = c.fetchone()
        if row and row[0]:
            paused_until = datetime.fromisoformat(row[0])
            if datetime.now(timezone.utc) < paused_until:
                return False
        return True

def update_last_signal(coin):
    with db_lock:
        c = db_conn.cursor()
        c.execute("REPLACE INTO last_signal (coin, last_time) VALUES (?, ?)",
                  (coin, datetime.now(timezone.utc).isoformat()))
        db_conn.commit()

def update_streak(coin, outcome):
    with db_lock:
        c = db_conn.cursor()
        c.execute("SELECT consecutive_wins, consecutive_losses, paused_until FROM streaks WHERE coin = ?", (coin,))
        row = c.fetchone()
        if row:
            wins, losses, paused = row
        else:
            wins, losses, paused = 0, 0, None

        if outcome == "win":
            wins += 1
            losses = 0
        else:
            losses += 1
            wins = 0

        max_losses = int(get_user_setting("consecutive_losses_pause", "3"))
        pause_hours = int(get_user_setting("pause_hours", "24"))
        paused_until = None
        if losses >= max_losses:
            paused_until = (datetime.now(timezone.utc) + timedelta(hours=pause_hours)).isoformat()
            # Notify user via callback – we'll handle later
            # We'll send a message in the main thread; here we just store

        c.execute('''REPLACE INTO streaks (coin, consecutive_wins, consecutive_losses, last_outcome, paused_until)
                     VALUES (?, ?, ?, ?, ?)''',
                  (coin, wins, losses, outcome, paused_until))
        db_conn.commit()

def get_streak_multiplier(coin):
    with db_lock:
        c = db_conn.cursor()
        c.execute("SELECT consecutive_wins, consecutive_losses FROM streaks WHERE coin = ?", (coin,))
        row = c.fetchone()
        if not row:
            return 1.0
        wins, losses = row
        win_mult = float(get_user_setting("win_multiplier", "1.2"))
        loss_mult = float(get_user_setting("loss_multiplier", "0.8"))
        if wins > 0:
            return win_mult ** wins
        elif losses > 0:
            return loss_mult ** losses
        return 1.0

def get_open_trades_count():
    with db_lock:
        c = db_conn.cursor()
        c.execute("SELECT COUNT(*) FROM open_trades")
        return c.fetchone()[0]

def get_total_exposure():
    with db_lock:
        c = db_conn.cursor()
        c.execute("SELECT SUM(quantity * entry_price) FROM open_trades")
        total = c.fetchone()[0]
        return total if total else 0.0

def add_open_trade(coin, entry_price, quantity):
    with db_lock:
        c = db_conn.cursor()
        c.execute("INSERT INTO open_trades (coin, entry_price, quantity, entry_time) VALUES (?, ?, ?, ?)",
                  (coin, entry_price, quantity, datetime.now(timezone.utc)))
        db_conn.commit()

def remove_open_trade(coin):
    with db_lock:
        c = db_conn.cursor()
        c.execute("DELETE FROM open_trades WHERE coin = ?", (coin,))
        db_conn.commit()

def get_trades_last_hour():
    cutoff = datetime.now(timezone.utc) - timedelta(hours=1)
    with db_lock:
        c = db_conn.cursor()
        c.execute("SELECT COUNT(*) FROM signals WHERE timestamp > ?", (cutoff,))
        return c.fetchone()[0]

# ================================
#  BINANCE API FUNCTIONS
# ================================
def get_ohlcv(symbol, interval, limit=200):
    """Fetch OHLCV data from Binance public API."""
    url = f"{BINANCE_API}/api/v3/klines"
    params = {
        "symbol": symbol.upper(),
        "interval": interval,
        "limit": limit
    }
    for attempt in range(1, 4):
        try:
            resp = requests.get(url, params=params, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                if data and len(data) > 0:
                    ohlcv = []
                    for candle in data:
                        ohlcv.append({
                            'open': float(candle[1]),
                            'high': float(candle[2]),
                            'low': float(candle[3]),
                            'close': float(candle[4]),
                            'volume': float(candle[5])
                        })
                    logging.info(f"[OHLCV SUCCESS] {symbol}")
                    return ohlcv
                else:
                    logging.warning(f"[OHLCV ERROR] {symbol}: empty data (attempt {attempt})")
            else:
                logging.warning(f"[OHLCV ERROR] {symbol}: HTTP {resp.status_code} (attempt {attempt})")
        except Exception as e:
            logging.warning(f"[OHLCV ERROR] {symbol}: {e} (attempt {attempt})")
        time.sleep(2)
    logging.error(f"[OHLCV FAILED] {symbol} after 3 attempts")
    return None

def get_current_price(symbol):
    """Fetch current price from Binance."""
    url = f"{BINANCE_API}/api/v3/ticker/price"
    params = {"symbol": symbol.upper()}
    for attempt in range(1, 4):
        try:
            resp = requests.get(url, params=params, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                return float(data['price'])
            else:
                logging.warning(f"Price fetch error {symbol}: HTTP {resp.status_code} (attempt {attempt})")
        except Exception as e:
            logging.warning(f"Price fetch error {symbol}: {e} (attempt {attempt})")
        time.sleep(2)
    logging.error(f"Price fetch failed for {symbol} after 3 attempts")
    return None

def get_btc_trend():
    """Get BTC trend using 200 EMA on 1h."""
    ohlcv = get_ohlcv(BTC_SYMBOL, "1h", limit=250)
    if not ohlcv or len(ohlcv) < 200:
        return None
    closes = [c['close'] for c in ohlcv]
    ema200_series = ema_series(closes, 200)
    current = closes[-1]
    return "bullish" if current > ema200_series[-1] else "bearish"

# ================================
#  TECHNICAL INDICATORS
# ================================
def rsi(prices, period=14):
    if len(prices) < period + 1:
        return 50.0
    gains, losses = [], []
    for i in range(1, len(prices)):
        diff = prices[i] - prices[i-1]
        if diff > 0:
            gains.append(diff)
        else:
            losses.append(abs(diff))
    avg_gain = sum(gains[-period:]) / period if gains else 0
    avg_loss = sum(losses[-period:]) / period if losses else 1e-10
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def ema(prices, period=20):
    if len(prices) < period:
        return prices[-1]
    k = 2 / (period + 1)
    ema_val = prices[0]
    for price in prices:
        ema_val = price * k + ema_val * (1 - k)
    return ema_val

def ema_series(prices, period=20):
    if len(prices) < period:
        return [prices[-1]] * len(prices)
    k = 2 / (period + 1)
    ema_vals = [prices[0]]
    for i in range(1, len(prices)):
        ema_vals.append(prices[i] * k + ema_vals[-1] * (1 - k))
    return ema_vals

def sma(values, period):
    if len(values) < period:
        return values[-1]
    return sum(values[-period:]) / period

def macd(prices, fast=12, slow=26, signal=9):
    if len(prices) < slow:
        return 0
    ema_fast = ema(prices, fast)
    ema_slow = ema(prices, slow)
    return ema_fast - ema_slow

def bollinger_bands(prices, period=20, num_std=2):
    if len(prices) < period:
        return prices[-1] - 100, prices[-1], prices[-1] + 100
    middle = sma(prices, period)
    stdev = statistics.stdev(prices[-period:])
    lower = middle - num_std * stdev
    upper = middle + num_std * stdev
    return lower, middle, upper

def true_range(high, low, prev_close):
    return max(high - low, abs(high - prev_close), abs(low - prev_close))

def atr(ohlcv, period=14):
    if len(ohlcv) < period + 1:
        return None
    tr_values = []
    for i in range(1, len(ohlcv)):
        tr = true_range(ohlcv[i]['high'], ohlcv[i]['low'], ohlcv[i-1]['close'])
        tr_values.append(tr)
    return sum(tr_values[-period:]) / period

def adx(ohlcv, period=14):
    if len(ohlcv) < period * 2:
        return 20.0
    plus_dm = []
    minus_dm = []
    tr_values = []
    for i in range(1, len(ohlcv)):
        high = ohlcv[i]['high']
        low = ohlcv[i]['low']
        prev_high = ohlcv[i-1]['high']
        prev_low = ohlcv[i-1]['low']
        prev_close = ohlcv[i-1]['close']

        up_move = high - prev_high
        down_move = prev_low - low
        if up_move > down_move and up_move > 0:
            plus_dm.append(up_move)
        else:
            plus_dm.append(0)
        if down_move > up_move and down_move > 0:
            minus_dm.append(down_move)
        else:
            minus_dm.append(0)

        tr = true_range(high, low, prev_close)
        tr_values.append(tr)

    atr_val = sum(tr_values[-period:]) / period
    plus_di = (sum(plus_dm[-period:]) / period) / atr_val * 100 if atr_val else 0
    minus_di = (sum(minus_dm[-period:]) / period) / atr_val * 100 if atr_val else 0
    dx = abs(plus_di - minus_di) / (plus_di + minus_di) * 100 if (plus_di + minus_di) > 0 else 0
    return dx

# ================================
#  MARKET STRUCTURE DETECTION
# ================================
def find_swing_highs_lows(ohlcv, lookback=5):
    highs = [c['high'] for c in ohlcv]
    lows = [c['low'] for c in ohlcv]
    swing_highs = []
    swing_lows = []
    n = len(ohlcv)
    for i in range(lookback, n - lookback):
        if all(highs[i] > highs[i-j] for j in range(1, lookback+1)) and all(highs[i] > highs[i+j] for j in range(1, lookback+1)):
            swing_highs.append((i, highs[i]))
        if all(lows[i] < lows[i-j] for j in range(1, lookback+1)) and all(lows[i] < lows[i+j] for j in range(1, lookback+1)):
            swing_lows.append((i, lows[i]))
    return swing_highs, swing_lows

def get_market_structure(ohlcv):
    if len(ohlcv) < 30:
        return "neutral"
    swing_highs, swing_lows = find_swing_highs_lows(ohlcv)
    if len(swing_highs) < 2 or len(swing_lows) < 2:
        return "neutral"
    last_two_highs = sorted(swing_highs[-2:], key=lambda x: x[0])
    last_two_lows = sorted(swing_lows[-2:], key=lambda x: x[0])
    high1, high2 = last_two_highs[-2][1], last_two_highs[-1][1] if len(last_two_highs) >= 2 else (None, None)
    low1, low2 = last_two_lows[-2][1], last_two_lows[-1][1] if len(last_two_lows) >= 2 else (None, None)

    if high2 and low2 and high2 > high1 and low2 > low1:
        return "uptrend"
    elif high2 and low2 and high2 < high1 and low2 < low1:
        return "downtrend"
    else:
        return "neutral"

# ================================
#  ADVANCED ANALYSIS FUNCTION (Stable Version)
# ================================
def analyze_coin_mtf(user_symbol):
    try:
        tf_main = get_timeframe()
        tf_confirm = "1h"

        ohlcv_main = get_ohlcv(user_symbol, tf_main, limit=200)
        ohlcv_confirm = get_ohlcv(user_symbol, tf_confirm, limit=200)

        # --- SAFE DATA HANDLING ---
        if not ohlcv_main and not ohlcv_confirm:
            return {
                "symbol": user_symbol,
                "price": 0.0,
                "signal": "HOLD",
                "confidence": 0,
                "grade": "C",
                "rsi": 50,
                "trend_main": "UNKNOWN",
                "trend_confirm": "UNKNOWN",
                "risk": "HIGH",
                "entry_comment": "No market data"
            }

        if not ohlcv_main or len(ohlcv_main) == 0:
            price = ohlcv_confirm[-1]['close'] if ohlcv_confirm else 0.0
        else:
            price = ohlcv_main[-1]['close']

        if not ohlcv_main or len(ohlcv_main) < 50 or not ohlcv_confirm or len(ohlcv_confirm) < 50:
            return {
                "symbol": user_symbol,
                "price": price,
                "signal": "HOLD",
                "confidence": 0,
                "grade": "C",
                "rsi": 50,
                "trend_main": "UNKNOWN",
                "trend_confirm": "UNKNOWN",
                "risk": "HIGH",
                "entry_comment": "Insufficient data"
            }

        closes_main = [c['close'] for c in ohlcv_main]
        volumes_main = [c['volume'] for c in ohlcv_main]

        # EMA200 series
        ema200_main_series = ema_series(closes_main, 200)
        confirm_closes = [c['close'] for c in ohlcv_confirm]
        ema200_confirm_series = ema_series(confirm_closes, 200)

        trend_main_ema200 = "bullish" if price > ema200_main_series[-1] else "bearish"
        trend_confirm_ema200 = "bullish" if confirm_closes[-1] > ema200_confirm_series[-1] else "bearish"

        # Market structure
        structure_main = get_market_structure(ohlcv_main)
        structure_confirm = get_market_structure(ohlcv_confirm)

        # Fake breakout (reduce confidence, don't skip)
        avg_candle_size = sma([c['high'] - c['low'] for c in ohlcv_main[-20:]], 20)
        current_candle_size = ohlcv_main[-1]['high'] - ohlcv_main[-1]['low']
        fake_breakout = current_candle_size > FAKE_BREAKOUT_MULTIPLIER * avg_candle_size

        # Volume spike
        avg_vol = sma(volumes_main, 20)
        volume_ratio = volumes_main[-1] / avg_vol if avg_vol > 0 else 1
        volume_spike = volume_ratio >= VOLUME_SPIKE_MULTIPLIER

        # Indicators
        r = rsi(closes_main, 14)
        ema20_main = ema(closes_main, 20)
        trend_main = "UP" if price > ema20_main else "DOWN"
        macd_line = macd(closes_main, 12, 26)
        macd_bullish = macd_line > 0
        lower, middle, upper = bollinger_bands(closes_main, 20, 2)
        near_lower = price <= lower * 1.01
        near_upper = price >= upper * 0.99
        adx_val = adx(ohlcv_main, 14)
        atr_val = atr(ohlcv_main, 14)
        if atr_val is None:
            atr_val = price * 0.01

        # ATR filter (log only)
        max_atr_percent = float(get_user_setting("max_atr_percent", str(MAX_ATR_PERCENT)))
        atr_percent = (atr_val / price) * 100
        if atr_percent > max_atr_percent:
            logging.info(f"{user_symbol} ATR% {atr_percent:.2f}% > {max_atr_percent}% – high volatility")

        # Market regime filter (relaxed)
        if adx_val < MIN_ADX_FOR_TRADE:
            logging.info(f"{user_symbol} ADX {adx_val:.1f} < {MIN_ADX_FOR_TRADE} – ranging market")

        # --- CONFIDENCE SCORE ---
        conf = 50

        if r < 30:
            conf += 15
        elif r > 70:
            conf -= 15
        else:
            conf += 5

        if trend_main == "UP":
            conf += 10
        else:
            conf -= 5

        if trend_main_ema200 == "bullish" and trend_confirm_ema200 == "bullish":
            conf += 20
        elif trend_main_ema200 == "bearish" and trend_confirm_ema200 == "bearish":
            conf += 20
        else:
            conf -= 10

        if (trend_main == "UP" and structure_main == "uptrend") or (trend_main == "DOWN" and structure_main == "downtrend"):
            conf += 15
        else:
            conf -= 10

        if volume_spike:
            conf += 10

        if adx_val > 30:
            conf += 10
        elif adx_val > 20:
            conf += 5

        if macd_bullish:
            conf += 5

        if near_lower:
            conf += 5
        elif near_upper:
            conf -= 5

        ema20_confirm = ema(confirm_closes, 20)
        trend_confirm_dir = "UP" if confirm_closes[-1] > ema20_confirm else "DOWN"
        if trend_main == trend_confirm_dir:
            conf += 10

        if fake_breakout:
            conf -= 15

        conf = max(0, min(100, conf))

        # --- DETERMINE SIGNAL ---
        buy_thr = get_param("buy_threshold")
        sell_thr = get_param("sell_threshold")
        if conf >= buy_thr:
            signal_main = "BUY"
        elif conf <= sell_thr:
            signal_main = "SELL"
        else:
            signal_main = "HOLD"

        # --- GRADE ---
        filter_mode = get_user_setting("filter_mode", "strict")
        if filter_mode == "strict":
            min_adx = STRICT_ADX
            min_vol_mult = STRICT_VOL_MULT
            min_conf = STRICT_CONFIDENCE
        else:
            min_adx = NORMAL_ADX
            min_vol_mult = NORMAL_VOL_MULT
            min_conf = NORMAL_CONFIDENCE

        grade_score = 0
        if (signal_main == "BUY" and trend_confirm_dir == "UP") or (signal_main == "SELL" and trend_confirm_dir == "DOWN"):
            grade_score += 2
        elif signal_main != "HOLD":
            grade_score += 1

        if adx_val >= min_adx:
            grade_score += 1

        if volume_ratio >= min_vol_mult:
            grade_score += 1

        if conf >= min_conf:
            grade_score += 2
        elif conf >= 70:
            grade_score += 1

        if grade_score >= 5:
            grade = "A+"
        elif grade_score >= 3:
            grade = "B"
        else:
            grade = "C"

        # --- LEVERAGE ---
        base_leverage = LEVERAGE_BASE.get(grade, 2)
        leverage = base_leverage
        if adx_val > 35 and conf > 90:
            leverage = min(5, base_leverage + 2)
        elif adx_val < 20 or conf < 50:
            leverage = max(1, base_leverage - 1)

        if atr_percent > LEVERAGE_ATR_THRESHOLD:
            leverage = min(leverage, 3)

        # --- STOP LOSS & TAKE PROFIT ---
        sl_mult = get_param("atr_multiplier_sl")
        tp_mult = get_param("atr_multiplier_tp")
        if signal_main == "BUY":
            sl = price - atr_val * sl_mult
            tp = price + atr_val * tp_mult
        elif signal_main == "SELL":
            sl = price + atr_val * sl_mult
            tp = price - atr_val * tp_mult
        else:
            sl = tp = None

        if sl is not None and (abs(price - sl) < 1e-8 or atr_percent < MIN_ATR_PERCENT):
            sl = price * 0.99 if signal_main == "BUY" else price * 1.01
            tp = price * 1.02 if signal_main == "BUY" else price * 0.98

        # --- MARKET FILTERS (convert skip to HOLD) ---
        use_btc = get_user_setting("use_btc_filter", "true") == "true"
        if use_btc and signal_main in ("BUY", "SELL"):
            btc_trend = get_btc_trend()
            if btc_trend:
                if signal_main == "BUY" and btc_trend == "bearish":
                    signal_main = "HOLD"
                if signal_main == "SELL" and btc_trend == "bullish":
                    signal_main = "HOLD"

        use_dom = get_user_setting("use_dominance_filter", "false") == "true"
        if use_dom and signal_main in ("BUY", "SELL"):
            dom = get_btc_dominance()
            if dom and dom > DOMINANCE_THRESHOLD and user_symbol != BTC_SYMBOL:
                signal_main = "HOLD"

        use_news = get_user_setting("use_news_filter", "false") == "true"
        if use_news and signal_main in ("BUY", "SELL"):
            if not get_news_sentiment():
                signal_main = "HOLD"

        # --- POSITION SIZE ---
        capital = float(get_user_setting("capital", "0"))
        risk_percent = float(get_user_setting("risk_percent", "1.0"))
        with db_lock:
            c = db_conn.cursor()
            c.execute("SELECT consecutive_losses FROM streaks WHERE coin = ?", (user_symbol,))
            row = c.fetchone()
            loss_count = row[0] if row else 0
        risk_reduction_threshold = int(get_user_setting("consecutive_losses_risk_reduction", "5"))
        if loss_count >= risk_reduction_threshold:
            risk_percent *= float(get_user_setting("risk_reduction_factor", "0.5"))

        position_size_inr = None
        quantity = None
        if capital > 0 and sl is not None:
            risk_amount = capital * (risk_percent / 100)
            risk_per_unit = abs(price - sl)
            if risk_per_unit > 0:
                base_quantity = risk_amount / risk_per_unit
                quantity = base_quantity * leverage
                position_size_inr = quantity * price
                min_pos = float(get_user_setting("min_position", str(MIN_POSITION_INR)))
                max_pos_percent = float(get_user_setting("max_position_percent", str(MAX_POSITION_PERCENT)))
                max_pos = capital * (max_pos_percent / 100)
                if position_size_inr < min_pos:
                    position_size_inr = None
                    quantity = None
                elif position_size_inr > max_pos:
                    position_size_inr = max_pos
                    quantity = position_size_inr / price

        # --- LIQUIDATION RISK ---
        if leverage <= 3:
            liq_risk = "LOW"
        elif leverage <= 5:
            liq_risk = "MEDIUM"
        else:
            liq_risk = "HIGH"

        # --- EXPLANATION ---
        explanation = []
        if trend_main_ema200 == "bullish":
            explanation.append("EMA200 bullish on both TFs")
        elif trend_main_ema200 == "bearish":
            explanation.append("EMA200 bearish on both TFs")
        else:
            explanation.append("EMA200 mixed")
        if volume_spike:
            explanation.append(f"Volume spike ({volume_ratio:.1f}x)")
        explanation.append(f"ADX {adx_val:.1f}")
        if structure_main != "neutral":
            explanation.append(f"Structure {structure_main}")
        explanation_str = ", ".join(explanation) if explanation else "No strong signal"

        return {
            "symbol": user_symbol,
            "price": price,
            "rsi": r,
            "trend_main": trend_main,
            "trend_confirm": trend_confirm_dir,
            "signal": signal_main,
            "confidence": conf,
            "grade": grade,
            "grade_score": grade_score,
            "leverage": leverage,
            "liq_risk": liq_risk,
            "sl": sl,
            "tp": tp,
            "atr_percent": atr_percent,
            "position_size": position_size_inr,
            "quantity": quantity,
            "explanation": explanation_str
        }
    except Exception as e:
        logging.error(f"[CRITICAL ERROR] in analyze_coin_mtf for {user_symbol}: {e}")
        return {
            "symbol": user_symbol,
            "price": 0.0,
            "signal": "HOLD",
            "confidence": 0,
            "grade": "C",
            "rsi": 50,
            "trend_main": "UNKNOWN",
            "trend_confirm": "UNKNOWN",
            "risk": "HIGH",
            "entry_comment": f"Error: {str(e)[:50]}"
        }

# ================================
#  HELPER FUNCTIONS FOR MESSAGING
# ================================
def format_signal(data):
    capital = float(get_user_setting("capital", "0"))
    risk_percent = float(get_user_setting("risk_percent", "1.0"))
    leverage = data['leverage']

    pos_line = ""
    if data['position_size'] is not None and capital > 0:
        pos_line = f"💵 Position Size: ₹{data['position_size']} (using ₹{capital} capital, {risk_percent}% risk, {leverage}x leverage)"
    else:
        pos_line = "💵 Position Size: Not calculated (set /capital and /risk)"

    return f"""
📊 {data['symbol']} | Grade: {data['grade']}
💰 Price: ₹{data['price']:,.2f}

🚀 Signal: {data['signal']}
📊 Confidence: {data['confidence']}%
⚡ Leverage: {data['leverage']}x
{pos_line}

🛑 Stop Loss: ₹{data['sl']:,.2f}
🎯 Take Profit: ₹{data['tp']:,.2f}

⚠️ Liquidation Risk: {data['liq_risk']}
🧠 Reasoning: {data['explanation']}
"""

def format_status(data):
    return f"{data['symbol']}: ₹{data['price']:,.2f} | {data['trend_main']} | RSI {data['rsi']:.1f} | Grade {data['grade']}"

# ================================
#  TELEGRAM HANDLERS (NEW UI)
# ================================
# --- Main Menu ---
def main_menu_keyboard():
    keyboard = [
        [InlineKeyboardButton("📊 Analyze Now", callback_data="analyze")],
        [InlineKeyboardButton("📈 Live Status", callback_data="status")],
        [InlineKeyboardButton("⚙️ Settings", callback_data="settings")],
        [InlineKeyboardButton("💰 Portfolio", callback_data="portfolio")],
        [InlineKeyboardButton("🧠 Strategy Info", callback_data="strategy")],
        [InlineKeyboardButton("🔄 Refresh", callback_data="refresh")],
        [InlineKeyboardButton("❌ Stop Bot", callback_data="stop")]
    ]
    return InlineKeyboardMarkup(keyboard)

def settings_menu_keyboard():
    keyboard = [
        [InlineKeyboardButton("💰 Set Capital", callback_data="set_capital")],
        [InlineKeyboardButton("📊 Set Risk %", callback_data="set_risk")],
        [InlineKeyboardButton("🔄 Toggle Filters", callback_data="toggle_filters")],
        [InlineKeyboardButton("⏱️ Change Timeframe", callback_data="change_timeframe")],
        [InlineKeyboardButton("🔙 Back to Main", callback_data="main_menu")]
    ]
    return InlineKeyboardMarkup(keyboard)

def toggle_filters_keyboard():
    btc_status = "ON" if get_user_setting("use_btc_filter") == "true" else "OFF"
    dom_status = "ON" if get_user_setting("use_dominance_filter") == "true" else "OFF"
    news_status = "ON" if get_user_setting("use_news_filter") == "true" else "OFF"
    keyboard = [
        [InlineKeyboardButton(f"BTC Trend Filter ({btc_status})", callback_data="toggle_btc")],
        [InlineKeyboardButton(f"Dominance Filter ({dom_status})", callback_data="toggle_dominance")],
        [InlineKeyboardButton(f"News Filter ({news_status})", callback_data="toggle_news")],
        [InlineKeyboardButton("🔙 Back to Settings", callback_data="settings")]
    ]
    return InlineKeyboardMarkup(keyboard)

def timeframe_menu_keyboard():
    current = get_timeframe()
    keyboard = [
        [InlineKeyboardButton("1m", callback_data="tf_1m"), InlineKeyboardButton("5m", callback_data="tf_5m"), InlineKeyboardButton("15m", callback_data="tf_15m")],
        [InlineKeyboardButton("30m", callback_data="tf_30m"), InlineKeyboardButton("1h", callback_data="tf_1h"), InlineKeyboardButton("4h", callback_data="tf_4h")],
        [InlineKeyboardButton("1d", callback_data="tf_1d"), InlineKeyboardButton(f"Current: {current}", callback_data="noop")],
        [InlineKeyboardButton("🔙 Back to Settings", callback_data="settings")]
    ]
    return InlineKeyboardMarkup(keyboard)

# --- Command Handlers ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🚀 Welcome to Trading Bot!\n\n"
        "I provide real-time crypto trading signals using advanced strategies.\n"
        "Use the buttons below to interact with me.",
        reply_markup=main_menu_keyboard()
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📚 Available Commands:\n"
        "/start - Show main menu\n"
        "/help - This message\n"
        "/analyze - Run analysis now\n"
        "/status - Show market status\n"
        "/settings - Open settings\n"
        "/portfolio - View portfolio\n"
        "/strategy - Strategy details\n"
        "/stop - Stop bot loops\n"
        "/add <symbol> - Add coin to watchlist\n"
        "/remove <symbol> - Remove coin\n"
        "/coins - List watchlist\n"
        "/capital <amount> - Set capital (INR)\n"
        "/risk <percent> - Set risk per trade\n"
        "/leverage <x> - Set default leverage\n"
        "/params - Show current parameters"
    )

async def analyze(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⏳ Analyzing market...")
    coins = get_watchlist()
    for coin in coins:
        result = analyze_coin_mtf(coin)
        if result and result['signal'] in ("BUY", "SELL"):
            await update.message.reply_text(format_signal(result))
            if result['signal'] in ("BUY", "SELL"):
                record_signal(coin, result['price'], result['signal'], result['confidence'],
                              result['grade'], result['grade_score'], result['leverage'],
                              result['position_size'] or 0)
                if result['position_size']:
                    add_open_trade(coin, result['price'], result['position_size'] / result['price'])
                update_last_signal(coin)
        await asyncio.sleep(1)  # avoid flooding

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    coins = get_watchlist()
    lines = ["📊 Market Status:\n"]
    for coin in coins:
        result = analyze_coin_mtf(coin)
        if result:
            lines.append(format_status(result))
        else:
            lines.append(f"{coin}: Data error")
    await update.message.reply_text("\n".join(lines))

async def settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⚙️ Settings Menu:", reply_markup=settings_menu_keyboard())

async def portfolio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    capital = get_user_setting("capital")
    risk = get_user_setting("risk_percent")
    exposure = get_total_exposure()
    cap = float(capital) if capital else 0
    msg = f"💰 Portfolio Overview\n\n"
    msg += f"💵 Capital: ₹{cap:,.2f}\n"
    msg += f"📊 Risk per trade: {risk}%\n"
    msg += f"🎯 Current exposure: ₹{exposure:,.2f}\n"
    if cap > 0:
        msg += f"📈 Exposure %: {(exposure/cap)*100:.1f}%\n"
    open_trades = get_open_trades_count()
    msg += f"🔓 Open trades: {open_trades}\n"
    await update.message.reply_text(msg)

async def strategy_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = (
        "🧠 Strategy Overview\n\n"
        "📊 Indicators used:\n"
        "- RSI (14)\n"
        "- EMA (20 & 200)\n"
        "- MACD\n"
        "- Bollinger Bands\n"
        "- ADX\n"
        "- Volume spike detection\n"
        "- Market structure (HH/HL, LH/LL)\n\n"
        "⏱️ Multi‑timeframe: 15m + 1h\n\n"
        "🎯 Signal logic:\n"
        "- BUY when confidence ≥ 70\n"
        "- SELL when confidence ≤ 30\n"
        "- HOLD otherwise\n\n"
        "⚖️ Risk management:\n"
        "- Dynamic position sizing based on capital & risk%\n"
        "- Leverage based on signal grade\n"
        "- Stop loss & take profit using ATR\n"
        "- Trailing stop after 1R\n"
        "- Partial exit at 1R"
    )
    await update.message.reply_text(msg)

async def stop_bot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global running
    running = False
    await update.message.reply_text("⛔ Bot stopped. Use /start to restart analysis.")

# --- Callback Handlers ---
async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "analyze":
        await query.edit_message_text("⏳ Analyzing market...")
        coins = get_watchlist()
        for coin in coins:
            result = analyze_coin_mtf(coin)
            if result and result['signal'] in ("BUY", "SELL"):
                await query.message.reply_text(format_signal(result))
                if result['signal'] in ("BUY", "SELL"):
                    record_signal(coin, result['price'], result['signal'], result['confidence'],
                                  result['grade'], result['grade_score'], result['leverage'],
                                  result['position_size'] or 0)
                    if result['position_size']:
                        add_open_trade(coin, result['price'], result['position_size'] / result['price'])
                    update_last_signal(coin)
            await asyncio.sleep(1)
        await query.message.reply_text("✅ Analysis complete.", reply_markup=main_menu_keyboard())

    elif data == "status":
        coins = get_watchlist()
        lines = ["📊 Market Status:\n"]
        for coin in coins:
            result = analyze_coin_mtf(coin)
            if result:
                lines.append(format_status(result))
            else:
                lines.append(f"{coin}: Data error")
        await query.edit_message_text("\n".join(lines), reply_markup=main_menu_keyboard())

    elif data == "settings":
        await query.edit_message_text("⚙️ Settings Menu:", reply_markup=settings_menu_keyboard())

    elif data == "portfolio":
        capital = get_user_setting("capital")
        risk = get_user_setting("risk_percent")
        exposure = get_total_exposure()
        cap = float(capital) if capital else 0
        msg = f"💰 Portfolio Overview\n\n"
        msg += f"💵 Capital: ₹{cap:,.2f}\n"
        msg += f"📊 Risk per trade: {risk}%\n"
        msg += f"🎯 Current exposure: ₹{exposure:,.2f}\n"
        if cap > 0:
            msg += f"📈 Exposure %: {(exposure/cap)*100:.1f}%\n"
        open_trades = get_open_trades_count()
        msg += f"🔓 Open trades: {open_trades}\n"
        await query.edit_message_text(msg, reply_markup=main_menu_keyboard())

    elif data == "strategy":
        msg = (
            "🧠 Strategy Overview\n\n"
            "📊 Indicators used:\n"
            "- RSI (14)\n"
            "- EMA (20 & 200)\n"
            "- MACD\n"
            "- Bollinger Bands\n"
            "- ADX\n"
            "- Volume spike detection\n"
            "- Market structure (HH/HL, LH/LL)\n\n"
            "⏱️ Multi‑timeframe: 15m + 1h\n\n"
            "🎯 Signal logic:\n"
            "- BUY when confidence ≥ 70\n"
            "- SELL when confidence ≤ 30\n"
            "- HOLD otherwise\n\n"
            "⚖️ Risk management:\n"
            "- Dynamic position sizing based on capital & risk%\n"
            "- Leverage based on signal grade\n"
            "- Stop loss & take profit using ATR\n"
            "- Trailing stop after 1R\n"
            "- Partial exit at 1R"
        )
        await query.edit_message_text(msg, reply_markup=main_menu_keyboard())

    elif data == "refresh":
        await query.edit_message_text("🔄 Refreshing...")
        # Re-send the same message as before? We'll just show main menu again.
        await query.message.reply_text("Main Menu:", reply_markup=main_menu_keyboard())

    elif data == "stop":
        global running
        running = False
        await query.edit_message_text("⛔ Bot stopped. Use /start to restart analysis.", reply_markup=main_menu_keyboard())

    elif data == "main_menu":
        await query.edit_message_text("Main Menu:", reply_markup=main_menu_keyboard())

    elif data == "set_capital":
        await query.edit_message_text("Please type your capital in INR (e.g., `capital 100000`).\nUse command: /capital <amount>")
        # We'll handle input via message handler

    elif data == "set_risk":
        await query.edit_message_text("Please type your risk percentage (e.g., `risk 2`).\nUse command: /risk <percent>")

    elif data == "toggle_filters":
        await query.edit_message_text("Select filter to toggle:", reply_markup=toggle_filters_keyboard())

    elif data == "change_timeframe":
        await query.edit_message_text("Select new timeframe:", reply_markup=timeframe_menu_keyboard())

    elif data.startswith("toggle_"):
        if data == "toggle_btc":
            current = get_user_setting("use_btc_filter", "true")
            new = "false" if current == "true" else "true"
            set_user_setting("use_btc_filter", new)
            await query.answer(f"BTC filter turned {new.upper()}")
            await query.edit_message_text("Filter updated.", reply_markup=toggle_filters_keyboard())
        elif data == "toggle_dominance":
            current = get_user_setting("use_dominance_filter", "false")
            new = "false" if current == "true" else "true"
            set_user_setting("use_dominance_filter", new)
            await query.answer(f"Dominance filter turned {new.upper()}")
            await query.edit_message_text("Filter updated.", reply_markup=toggle_filters_keyboard())
        elif data == "toggle_news":
            current = get_user_setting("use_news_filter", "false")
            new = "false" if current == "true" else "true"
            set_user_setting("use_news_filter", new)
            await query.answer(f"News filter turned {new.upper()}")
            await query.edit_message_text("Filter updated.", reply_markup=toggle_filters_keyboard())

    elif data.startswith("tf_"):
        tf = data.split("_")[1]
        valid = {"1m":"1m","5m":"5m","15m":"15m","30m":"30m","1h":"1h","4h":"4h","1d":"1d"}
        if tf in valid:
            set_timeframe(valid[tf])
            await query.answer(f"Timeframe set to {valid[tf]}")
            await query.edit_message_text(f"Timeframe changed to {valid[tf]}", reply_markup=timeframe_menu_keyboard())
        else:
            await query.answer("Invalid timeframe")

    elif data == "noop":
        await query.answer("No action")

# --- Message Handler for text input (for capital, risk, add/remove) ---
async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    parts = text.split()
    cmd = parts[0].lower()

    if cmd == "capital" and len(parts) == 2:
        try:
            cap = float(parts[1])
            set_user_setting("capital", str(cap))
            await update.message.reply_text(f"💰 Capital set to ₹{cap}")
        except:
            await update.message.reply_text("❌ Invalid number")
    elif cmd == "risk" and len(parts) == 2:
        try:
            risk = float(parts[1])
            if 0.1 <= risk <= 10:
                set_user_setting("risk_percent", str(risk))
                await update.message.reply_text(f"📊 Risk per trade set to {risk}%")
            else:
                await update.message.reply_text("⚠️ Risk % should be between 0.1 and 10")
        except:
            await update.message.reply_text("❌ Invalid number")
    elif cmd == "add" and len(parts) == 2:
        coin = parts[1].upper()
        add_to_watchlist(coin)
        await update.message.reply_text(f"✅ Added {coin}")
    elif cmd == "remove" and len(parts) == 2:
        coin = parts[1].upper()
        remove_from_watchlist(coin)
        await update.message.reply_text(f"❌ Removed {coin}")
    elif cmd == "coins":
        coins = get_watchlist()
        await update.message.reply_text(f"📊 Tracking: {', '.join(coins) if coins else 'None'}")
    elif cmd == "leverage" and len(parts) == 2:
        try:
            lev = int(parts[1])
            if 1 <= lev <= 20:
                set_user_setting("default_leverage", str(lev))
                await update.message.reply_text(f"⚡ Default leverage set to {lev}x")
            else:
                await update.message.reply_text("⚠️ Leverage must be between 1 and 20")
        except:
            await update.message.reply_text("❌ Invalid number")
    elif cmd == "params":
        buy = get_param("buy_threshold")
        sell = get_param("sell_threshold")
        hold = get_param("holding_period_minutes")
        profit = get_param("profit_target_percent")
        look = get_param("lookback_signals")
        target = get_param("target_win_rate")
        sl_mult = get_param("atr_multiplier_sl")
        tp_mult = get_param("atr_multiplier_tp")
        min_adx = get_param("min_adx")
        vol_mult = get_param("volume_multiplier")
        min_conf = get_param("min_confidence")
        msg = (f"📈 Current parameters:\n"
               f"BUY threshold: {buy:.1f}\n"
               f"SELL threshold: {sell:.1f}\n"
               f"Holding period: {hold} min\n"
               f"Profit target: {profit}%\n"
               f"Lookback signals: {int(look)}\n"
               f"Target win rate: {target}%\n"
               f"SL multiplier: {sl_mult:.1f}\n"
               f"TP multiplier: {tp_mult:.1f}\n"
               f"Min ADX: {min_adx}\n"
               f"Volume multiplier: {vol_mult}\n"
               f"Min confidence: {min_conf}")
        await update.message.reply_text(msg)
    else:
        # If unknown, show help
        await help_command(update, context)

# ================================
#  BACKGROUND LOOPS (unchanged)
# ================================
running = True

def evaluation_loop():
    while True:
        try:
            evaluate_pending_signals()
            adapt_parameters()
        except Exception as e:
            logging.error(f"Evaluation error: {e}")
        time.sleep(600)

def analysis_loop():
    while True:
        if running:
            trades_last_hour = get_trades_last_hour()
            max_trades = int(get_user_setting("max_trades_per_hour", "3"))
            exposure = get_total_exposure()
            capital = float(get_user_setting("capital", "0"))
            max_exposure_percent = float(get_user_setting("max_global_exposure_percent", "20"))
            max_exposure = capital * (max_exposure_percent / 100)
            max_concurrent = int(get_user_setting("max_concurrent_trades", str(MAX_CONCURRENT_TRADES)))

            if trades_last_hour >= max_trades:
                logging.info("Overtrading protection: max trades per hour reached, skipping analysis cycle")
                time.sleep(ANALYSIS_INTERVAL)
                continue

            coins = get_watchlist()
            for coin in coins:
                with db_lock:
                    c = db_conn.cursor()
                    c.execute("SELECT 1 FROM open_trades WHERE coin = ?", (coin,))
                    if c.fetchone():
                        logging.info(f"{coin} already has open trade, skipping")
                        time.sleep(5)
                        continue
                if get_open_trades_count() >= max_concurrent:
                    logging.info("Max concurrent trades reached, skipping further entries")
                    break

                result = analyze_coin_mtf(coin)
                if result and result['signal'] in ("BUY", "SELL") and result['grade_score'] >= MIN_GRADE_SCORE:
                    min_conf_trade = float(get_user_setting("min_confidence_for_trade", "75"))
                    if result['confidence'] < min_conf_trade:
                        logging.info(f"{coin} confidence {result['confidence']} < {min_conf_trade}, skipping")
                        time.sleep(5)
                        continue
                    if exposure + (result['position_size'] or 0) > max_exposure:
                        logging.info("Global exposure limit reached, skipping trade")
                        break
                    if can_send_signal(coin):
                        # Send message via Telegram (we'll need a function to send)
                        send_message_via_telegram(format_signal(result))
                        sig_id = record_signal(coin, result['price'], result['signal'], result['confidence'],
                                               result['grade'], result['grade_score'], result['leverage'],
                                               result['position_size'] or 0)
                        if result['position_size']:
                            add_open_trade(coin, result['price'], result['position_size'] / result['price'])
                            exposure += result['position_size']
                        update_last_signal(coin)
                    else:
                        logging.info(f"Cooldown/paused for {coin}, skipping.")
                time.sleep(5)
        time.sleep(ANALYSIS_INTERVAL)

def status_loop():
    while True:
        if running:
            coins = get_watchlist()
            lines = ["📊 Market Update:"]
            for coin in coins:
                result = analyze_coin_mtf(coin)
                if result:
                    lines.append(format_status(result))
                else:
                    lines.append(f"{coin}: Data error")
            send_message_via_telegram("\n".join(lines))
        time.sleep(STATUS_INTERVAL)

# We need a global reference to the bot application for sending messages from background threads
_application = None

def send_message_via_telegram(text):
    if _application:
        _application.bot.send_message(chat_id=CHAT_ID, text=text)

# ================================
#  MAIN
# ================================
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()]
    )
    logger = logging.getLogger(__name__)
    logger.info("Starting Ultimate Trading Bot with Professional UI")

    # Create application
    app = Application.builder().token(TOKEN).build()

    # Register handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("analyze", analyze))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("settings", settings))
    app.add_handler(CommandHandler("portfolio", portfolio))
    app.add_handler(CommandHandler("strategy", strategy_info))
    app.add_handler(CommandHandler("stop", stop_bot))
    app.add_handler(CallbackQueryHandler(button_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))

    # Store reference for background threads
    global _application
    _application = app

    # Start background threads
    threading.Thread(target=evaluation_loop, daemon=True).start()
    threading.Thread(target=analysis_loop, daemon=True).start()
    threading.Thread(target=status_loop, daemon=True).start()

    # Start polling
    logger.info("Bot polling started")
    app.run_polling()
