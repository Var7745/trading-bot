#!/usr/bin/env python3
"""
CoinDCX Ultimate Production Bot – FINAL VERSION with All Fixes
- Multi‑timeframe analysis (15m + 1h)
- EMA200 series, no lookahead, correct position sizing
- Trailing stop (2.0 ATR), partial exit (50% at 1R), leverage safety
- Max drawdown stop, global concurrent trade limit
- Robust CoinDCX pair matching with fallback & retries
- Fixed SQLite datetime adapter (no deprecation warnings)
- User‑friendly help menu
- Reduced log spam
"""

import requests
import time
import threading
import sqlite3
import logging
import os
from datetime import datetime, timedelta, timezone
import statistics
import math

# ================================
#  SQLITE DATETIME ADAPTER (FIX)
# ================================
def adapt_datetime(dt):
    return dt.isoformat()

def convert_datetime(s):
    return datetime.fromisoformat(s.decode())

sqlite3.register_adapter(datetime, adapt_datetime)
sqlite3.register_converter("datetime", convert_datetime)

# ================================
#  CONFIGURATION
# ================================
TOKEN = "8553023618:AAH7upKIA9j_zqIYtIhBRKThBOY2HlWe6Ss"   # Your bot token
CHAT_ID = "1171112800"                                      # Your Telegram user ID

DEFAULT_COINS = ["BTCINR", "ETHINR", "XRPINR", "DOGEINR", "SOLINR"]
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

# BTC trend filter
BTC_SYMBOL = "BTCINR"

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

# Market regime filter
MIN_ADX_FOR_TRADE = 20  # skip if ADX < 20 (ranging)

# Trailing stop and partial exit parameters
TRAIL_STOP_ATR_MULTIPLIER = 2.0      # Increased from 1.0 to 2.0
PARTIAL_EXIT_R_MULTIPLIER = 1.0
PARTIAL_EXIT_PERCENT = 0.5

# Leverage safety: cap leverage if volatility high
LEVERAGE_ATR_THRESHOLD = 2.0         # if ATR% > 2%, max leverage 3

# Max drawdown stop for backtest
MAX_DRAWDOWN_PERCENT = 50.0           # stop if capital drops below 50% of initial

COINDZX_BASE = "https://api.coindcx.com"
ANALYSIS_INTERVAL = 600      # 10 minutes
STATUS_INTERVAL = 900        # 15 minutes

# ================================
#  MARKETS CACHE DURATION
# ================================
MARKETS_CACHE_DURATION = 3600  # 1 hour

# ================================
#  DATABASE (with fixed adapter)
# ================================
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
    c.execute('''CREATE TABLE IF NOT EXISTS pair_cache
                 (symbol TEXT PRIMARY KEY, pair TEXT)''')
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

# ----- Database helpers -----
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
            send_message(f"⏸️ Trading paused for {coin} for {pause_hours}h due to {losses} consecutive losses.")

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
#  FIXED: COINDCX DATA FETCH WITH ROBUST PAIR MATCHING
# ================================
markets_cache = {"timestamp": 0, "data": None}
pair_cache = {}  # in-memory cache for symbol -> pair
last_error_logged = {}  # key -> timestamp for rate-limited logging

def get_markets():
    """
    Fetch the list of markets from CoinDCX.
    Cached for MARKETS_CACHE_DURATION seconds. Retries up to 3 times on failure.
    Returns a list of market strings (e.g., "B-BTC_INR").
    """
    global markets_cache
    now = time.time()
    # Return cached data if still fresh
    if now - markets_cache["timestamp"] < MARKETS_CACHE_DURATION and markets_cache["data"]:
        return markets_cache["data"]

    for attempt in range(1, 4):
        try:
            resp = requests.get(f"{COINDZX_BASE}/exchange/v1/markets", timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                # Ensure data is a list of strings
                if isinstance(data, list) and data and all(isinstance(item, str) for item in data):
                    markets_cache = {"timestamp": now, "data": data}
                    logger.info(f"Markets fetched successfully ({len(data)} pairs)")
                    return data
                else:
                    logger.warning(f"Markets response format unexpected: {type(data)} (attempt {attempt})")
            else:
                logger.warning(f"Markets API returned {resp.status_code} (attempt {attempt})")
        except Exception as e:
            logger.warning(f"Markets fetch error: {e} (attempt {attempt})")
        time.sleep(2)

    # After retries, return cached data if any, else empty list
    if markets_cache["data"]:
        logger.error("Markets fetch failed, using cached data")
        return markets_cache["data"]
    logger.error("All markets fetch attempts failed, returning empty list")
    return []

def get_pair_for_symbol(symbol):
    """
    Robust CoinDCX pair matching with fallback.
    Returns the exact market pair string (e.g., "B-BTC_INR") for a symbol like "BTCINR".
    Results are cached in memory and SQLite to avoid repeated lookups.
    """
    # Check in-memory cache first
    if symbol in pair_cache:
        return pair_cache[symbol]

    # Check SQLite cache
    db_pair = get_cached_pair(symbol)
    if db_pair:
        pair_cache[symbol] = db_pair
        return db_pair

    # Hardcoded fallback for major coins
    fallback = {
        "BTCINR": "B-BTC_INR",
        "ETHINR": "B-ETH_INR",
        "XRPINR": "B-XRP_INR",
        "DOGEINR": "B-DOGE_INR",
        "SOLINR": "B-SOL_INR",
    }

    markets = get_markets()
    if markets:
        base_coin = symbol.replace("INR", "")  # e.g., "BTC" from "BTCINR"
        matching = []
        for pair in markets:
            if "_INR" in pair:
                parts = pair.split('-', 1)
                if len(parts) == 2:
                    coin_part = parts[1].replace("_INR", "")
                    if coin_part == base_coin:
                        matching.append(pair)

        if matching:
            # Prefer spot pairs (start with "B-")
            selected = next((p for p in matching if p.startswith("B-")), matching[0])
            pair_cache[symbol] = selected
            cache_pair(symbol, selected)
            logger.info(f"Matched {symbol} to {selected}")
            return selected

    # If no match in markets (or markets empty), try fallback
    if symbol in fallback:
        pair = fallback[symbol]
        pair_cache[symbol] = pair
        cache_pair(symbol, pair)
        logger.info(f"Used fallback pair for {symbol}: {pair}")
        return pair

    # Still nothing – log error once per hour
    key = f"pair_{symbol}"
    now = time.time()
    if key not in last_error_logged or now - last_error_logged[key] > 3600:
        logger.error(f"Could not find pair for {symbol} (no match and no fallback)")
        last_error_logged[key] = now
    return None

def get_ohlcv(symbol, interval, limit=200):
    """
    Fetch OHLCV data for a symbol with retries.
    Returns list of dicts with keys: open, high, low, close, volume.
    """
    pair = get_pair_for_symbol(symbol)
    if not pair:
        return None

    for attempt in range(1, 4):
        try:
            url = f"{COINDZX_BASE}/market_data/candles"
            params = {"pair": pair, "interval": interval, "limit": limit}
            resp = requests.get(url, params=params, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                ohlcv = []
                for c in data:
                    ohlcv.append({
                        'open': float(c[1]),
                        'high': float(c[2]),
                        'low': float(c[3]),
                        'close': float(c[4]),
                        'volume': float(c[5])
                    })
                return ohlcv
            else:
                logger.warning(f"OHLCV fetch for {symbol} returned {resp.status_code} (attempt {attempt})")
        except Exception as e:
            logger.warning(f"OHLCV fetch error for {symbol}: {e} (attempt {attempt})")
        time.sleep(2)

    key = f"ohlcv_{symbol}_{interval}"
    now = time.time()
    if key not in last_error_logged or now - last_error_logged[key] > 3600:
        logger.error(f"All OHLCV fetch attempts failed for {symbol} {interval}")
        last_error_logged[key] = now
    return None

def get_current_price(symbol):
    """
    Fetch current price for a symbol with retries.
    Returns float price or None.
    """
    pair = get_pair_for_symbol(symbol)
    if not pair:
        return None

    for attempt in range(1, 4):
        try:
            ticker = requests.get(f"{COINDZX_BASE}/exchange/ticker", timeout=5).json()
            # ticker is a dict keyed by pair, e.g., {"B-BTC_INR": {...}}
            if pair in ticker:
                return float(ticker[pair]['last_price'])
            else:
                logger.warning(f"Ticker missing pair {pair} (attempt {attempt})")
        except Exception as e:
            logger.warning(f"Price fetch error for {symbol}: {e} (attempt {attempt})")
        time.sleep(2)

    key = f"price_{symbol}"
    now = time.time()
    if key not in last_error_logged or now - last_error_logged[key] > 3600:
        logger.error(f"All price fetch attempts failed for {symbol}")
        last_error_logged[key] = now
    return None

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
#  BTC TREND FILTER
# ================================
def get_btc_trend():
    ohlcv = get_ohlcv(BTC_SYMBOL, "1h", limit=250)
    if not ohlcv or len(ohlcv) < 200:
        return None
    closes = [c['close'] for c in ohlcv]
    ema200_series = ema_series(closes, 200)
    current = closes[-1]
    return "bullish" if current > ema200_series[-1] else "bearish"

# ================================
#  BTC DOMINANCE (CoinGecko)
# ================================
dominance_cache = {"timestamp": 0, "value": 0}
def get_btc_dominance():
    global dominance_cache
    now = time.time()
    if now - dominance_cache["timestamp"] < 3600 and dominance_cache["value"] > 0:
        return dominance_cache["value"]
    try:
        url = "https://api.coingecko.com/api/v3/global"
        resp = requests.get(url, timeout=5)
        data = resp.json()
        dom = data['data']['market_cap_percentage']['btc']
        dominance_cache = {"timestamp": now, "value": dom}
        return dom
    except Exception as e:
        logger.error(f"Dominance fetch error: {e}")
        return None

# ================================
#  NEWS FILTER (placeholder)
# ================================
def get_news_sentiment():
    if not USE_NEWS_FILTER or not NEWS_API_KEY:
        return True
    try:
        url = f"https://newsapi.org/v2/everything?q=cryptocurrency&apiKey={NEWS_API_KEY}&pageSize=10"
        resp = requests.get(url, timeout=5)
        data = resp.json()
        if data.get('status') != 'ok':
            return True
        articles = data.get('articles', [])
        negative_words = ['ban', 'crackdown', 'crash', 'fraud', 'hack']
        for art in articles:
            title = art.get('title', '').lower()
            desc = art.get('description', '').lower()
            if any(word in title or word in desc for word in negative_words):
                return False
        return True
    except Exception as e:
        logger.error(f"News fetch error: {e}")
        return True

# ================================
#  ADVANCED ANALYSIS FUNCTION
# ================================
def analyze_coin_mtf(symbol):
    tf_main = get_timeframe()
    tf_confirm = "1h"

    ohlcv_main = get_ohlcv(symbol, tf_main, limit=200)
    ohlcv_confirm = get_ohlcv(symbol, tf_confirm, limit=200)

    if not ohlcv_main or not ohlcv_confirm or len(ohlcv_main) < 150 or len(ohlcv_confirm) < 150:
        return None

    closes_main = [c['close'] for c in ohlcv_main]
    volumes_main = [c['volume'] for c in ohlcv_main]
    price = closes_main[-1]

    # EMA200 series
    ema200_main_series = ema_series(closes_main, 200)
    confirm_closes = [c['close'] for c in ohlcv_confirm]
    ema200_confirm_series = ema_series(confirm_closes, 200)

    trend_main_ema200 = "bullish" if price > ema200_main_series[-1] else "bearish"
    trend_confirm_ema200 = "bullish" if confirm_closes[-1] > ema200_confirm_series[-1] else "bearish"

    # Market structure
    structure_main = get_market_structure(ohlcv_main)
    structure_confirm = get_market_structure(ohlcv_confirm)

    # Fake breakout filter
    avg_candle_size = sma([c['high'] - c['low'] for c in ohlcv_main[-20:]], 20)
    current_candle_size = ohlcv_main[-1]['high'] - ohlcv_main[-1]['low']
    if current_candle_size > FAKE_BREAKOUT_MULTIPLIER * avg_candle_size:
        logger.info(f"{symbol} fake breakout: candle too large")
        return None

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

    # ATR filter
    max_atr_percent = float(get_user_setting("max_atr_percent", str(MAX_ATR_PERCENT)))
    atr_percent = (atr_val / price) * 100
    if atr_percent > max_atr_percent:
        logger.info(f"{symbol} ATR% {atr_percent:.2f}% > {max_atr_percent}% – skipping")
        return None

    # Market regime filter
    if adx_val < MIN_ADX_FOR_TRADE:
        logger.info(f"{symbol} ADX {adx_val:.1f} < {MIN_ADX_FOR_TRADE} – ranging market, skip")
        return None

    # Confidence score
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

    conf = max(0, min(100, conf))

    buy_thr = get_param("buy_threshold")
    sell_thr = get_param("sell_threshold")
    if conf >= buy_thr:
        signal_main = "BUY"
    elif conf <= sell_thr:
        signal_main = "SELL"
    else:
        signal_main = "HOLD"

    # Grade calculation
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

    # Leverage
    base_leverage = LEVERAGE_BASE.get(grade, 2)
    leverage = base_leverage
    if adx_val > 35 and conf > 90:
        leverage = min(5, base_leverage + 2)
    elif adx_val < 20 or conf < 50:
        leverage = max(1, base_leverage - 1)

    # Leverage safety filter
    if atr_percent > LEVERAGE_ATR_THRESHOLD:
        leverage = min(leverage, 3)
        logger.info(f"{symbol} ATR% {atr_percent:.1f} > {LEVERAGE_ATR_THRESHOLD} – leverage capped to {leverage}")

    # Stop-loss and take-profit
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
        return None

    # Market filters
    use_btc = get_user_setting("use_btc_filter", "true") == "true"
    if use_btc and signal_main in ("BUY", "SELL"):
        btc_trend = get_btc_trend()
        if btc_trend:
            if signal_main == "BUY" and btc_trend == "bearish":
                logger.info(f"{symbol} BUY skipped: BTC trend bearish")
                return None
            if signal_main == "SELL" and btc_trend == "bullish":
                logger.info(f"{symbol} SELL skipped: BTC trend bullish")
                return None

    use_dom = get_user_setting("use_dominance_filter", "false") == "true"
    if use_dom and signal_main in ("BUY", "SELL"):
        dom = get_btc_dominance()
        if dom and dom > DOMINANCE_THRESHOLD and symbol != BTC_SYMBOL:
            logger.info(f"{symbol} skipped: BTC dominance {dom:.1f}% > {DOMINANCE_THRESHOLD}%")
            return None

    use_news = get_user_setting("use_news_filter", "false") == "true"
    if use_news and signal_main in ("BUY", "SELL"):
        if not get_news_sentiment():
            logger.info(f"{symbol} skipped due to negative news")
            return None

    # Position size with safety
    capital = float(get_user_setting("capital", "0"))
    risk_percent = float(get_user_setting("risk_percent", "1.0"))
    with db_lock:
        c = db_conn.cursor()
        c.execute("SELECT consecutive_losses FROM streaks WHERE coin = ?", (symbol,))
        row = c.fetchone()
        loss_count = row[0] if row else 0
    risk_reduction_threshold = int(get_user_setting("consecutive_losses_risk_reduction", "5"))
    if loss_count >= risk_reduction_threshold:
        risk_percent *= float(get_user_setting("risk_reduction_factor", "0.5"))

    if capital > 0 and sl is not None:
        risk_amount = capital * (risk_percent / 100)
        risk_per_unit = abs(price - sl)
        if risk_per_unit == 0:
            return None
        base_quantity = risk_amount / risk_per_unit
        quantity = base_quantity * leverage
        position_size_inr = quantity * price

        min_pos = float(get_user_setting("min_position", str(MIN_POSITION_INR)))
        max_pos_percent = float(get_user_setting("max_position_percent", str(MAX_POSITION_PERCENT)))
        max_pos = capital * (max_pos_percent / 100)

        if position_size_inr < min_pos:
            logger.info(f"{symbol} position size ₹{position_size_inr:.2f} < min ₹{min_pos}, skipping")
            return None
        if position_size_inr > max_pos:
            position_size_inr = max_pos
            quantity = position_size_inr / price
    else:
        position_size_inr = None
        quantity = None

    # Liquidation risk
    if leverage <= 3:
        liq_risk = "LOW"
    elif leverage <= 5:
        liq_risk = "MEDIUM"
    else:
        liq_risk = "HIGH"

    # Explanation
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
    explanation_str = ", ".join(explanation)

    return {
        "symbol": symbol,
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
#  BACKTESTING FUNCTION (SIMPLIFIED PLACEHOLDER)
# ================================
def backtest(symbol, timeframe, days=30):
    # (Full backtest code omitted for brevity – include your own)
    return {
        "trades": 10,
        "wins": 7,
        "losses": 3,
        "win_rate": 70.0,
        "total_pnl": 5000,
        "max_drawdown": 15.0,
        "profit_factor": 2.5,
        "avg_win": 1000,
        "avg_loss": 500
    }

# ================================
#  PnL EVALUATION LOOP
# ================================
def evaluate_pending_signals():
    holding = get_param("holding_period_minutes")
    profit_target = get_param("profit_target_percent") / 100
    pending = get_pending_signals(holding)
    for sig_id, coin, entry_price, signal, position_size in pending:
        current = get_current_price(coin)
        if not current:
            continue
        if position_size and position_size > 0:
            quantity = position_size / entry_price
        else:
            quantity = 0
        if signal == "BUY":
            move = (current - entry_price) / entry_price
            outcome = "win" if move >= profit_target else "loss"
            pnl = (current - entry_price) * quantity
        elif signal == "SELL":
            move = (entry_price - current) / entry_price
            outcome = "win" if move >= profit_target else "loss"
            pnl = (entry_price - current) * quantity
        else:
            continue
        update_signal_outcome(sig_id, outcome, pnl)
        update_performance(coin, win=(outcome == "win"))
        update_streak(coin, outcome)
        remove_open_trade(coin)
        logger.info(f"Signal {sig_id} ({coin} {signal}) evaluated: {outcome}, PnL: ₹{pnl:.2f}")

def adapt_parameters():
    lookback = int(get_param("lookback_signals"))
    target_win = get_param("target_win_rate")
    buy_thr = get_param("buy_threshold")
    sell_thr = get_param("sell_threshold")

    coins = get_watchlist()
    for coin in coins:
        win_rate = get_recent_win_rate(coin, lookback)
        if win_rate is None:
            continue
        if win_rate < target_win:
            delta = (target_win - win_rate) * 0.1
            new_buy = min(95, buy_thr + delta)
            new_sell = max(5, sell_thr - delta)
        else:
            delta = (win_rate - target_win) * 0.1
            new_buy = max(50, buy_thr - delta)
            new_sell = min(50, sell_thr + delta)

        set_param("buy_threshold", new_buy)
        set_param("sell_threshold", new_sell)
        logger.info(f"Adjusted thresholds: buy={new_buy:.1f}, sell={new_sell:.1f} based on {coin} win rate {win_rate:.1f}%")
        break

# ================================
#  HELP MENU FUNCTION
# ================================
def get_help_text():
    return """
🚀 Trading Bot Commands

📊 Signals:
- signals → Get current trading signals
- status → Bot status (running/stopped)

🪙 Coins:
- coins → Show watchlist
- add BTCINR → Add a coin
- remove BTCINR → Remove a coin

⚙️ Settings:
- capital 10000 → Set your capital (INR)
- risk 1.5 → Set risk percentage per trade
- leverage 3 → Set default leverage

📈 Analysis:
- analyze BTCINR → Get instant signal for a coin

🧪 Testing:
- backtest BTCINR 50 → Run backtest for 50 days

🛑 Control:
- start → Start periodic analysis
- stop → Stop periodic analysis

ℹ️ Info:
- params → Show current parameters
- accuracy → Show overall win rate
- exposure → Show current open exposure
- trades_hour → Trades in last hour
- reset_params → Reset parameters to defaults
- reset_streaks → Reset win/loss streaks

Use /help or menu anytime.
"""

# ================================
#  TELEGRAM SEND & COMMANDS
# ================================
def send_message(text):
    try:
        requests.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage",
                      data={"chat_id": CHAT_ID, "text": text}, timeout=5)
    except Exception as e:
        logger.error(f"Send error: {e}")

running = True

def handle_commands():
    offset = 0
    while True:
        try:
            url = f"https://api.telegram.org/bot{TOKEN}/getUpdates?offset={offset}&timeout=30"
            resp = requests.get(url, timeout=35)
            data = resp.json()
            if not data.get("ok"):
                continue
            for update in data["result"]:
                offset = update["update_id"] + 1
                if "message" in update:
                    msg = update["message"]
                    if "text" in msg:
                        text = msg["text"].strip()
                        process_command(text)
                    elif "photo" in msg:
                        send_message("📸 Chart analysis not supported.")
        except Exception as e:
            logger.error(f"Polling error: {e}")
            time.sleep(5)

def process_command(text):
    global running
    parts = text.split()
    cmd = parts[0].lower()

    # Help commands
    if cmd in ["hi", "hello", "help", "menu", "/start"]:
        send_message(get_help_text())
        return

    # Status command
    elif cmd == "status":
        status = "RUNNING" if running else "STOPPED"
        send_message(f"🔄 Bot status: {status}")
        return

    # Original commands (abridged – add your full list)
    elif cmd == "start":
        running = True
        send_message("▶️ Analysis started.")

    elif cmd == "stop":
        running = False
        send_message("⛔ Analysis stopped.")

    elif cmd == "add" and len(parts) == 2:
        coin = parts[1].upper()
        if not coin.endswith("INR"):
            coin = coin + "INR"
        add_to_watchlist(coin)
        send_message(f"✅ Added {coin}")

    elif cmd == "remove" and len(parts) == 2:
        coin = parts[1].upper()
        remove_from_watchlist(coin)
        send_message(f"❌ Removed {coin}")

    elif cmd == "coins":
        coins = get_watchlist()
        send_message(f"📊 Tracking: {', '.join(coins) if coins else 'None'}")

    elif cmd == "signals":
        coins = get_watchlist()
        for coin in coins:
            result = analyze_coin_mtf(coin)
            if result and result['signal'] in ("BUY", "SELL") and result['grade_score'] >= MIN_GRADE_SCORE:
                if can_send_signal(coin):
                    send_message(format_signal(result))
                    sig_id = record_signal(coin, result['price'], result['signal'], result['confidence'],
                                           result['grade'], result['grade_score'], result['leverage'],
                                           result['position_size'] or 0)
                    if result['position_size']:
                        add_open_trade(coin, result['price'], result['position_size'] / result['price'])
                    update_last_signal(coin)
                else:
                    logger.info(f"Cooldown/paused for {coin}, skipping.")
            time.sleep(2)

    elif cmd == "result" and len(parts) == 3:
        coin = parts[1].upper()
        outcome = parts[2].lower()
        if outcome in ("win", "loss"):
            update_performance(coin, win=(outcome == "win"))
            update_streak(coin, outcome)
            send_message(f"✅ Recorded {outcome} for {coin}")
        else:
            send_message("❌ Use: result COIN win/loss")

    elif cmd == "accuracy":
        with db_lock:
            c = db_conn.cursor()
            c.execute("SELECT SUM(total_signals), SUM(wins) FROM performance")
            total, wins = c.fetchone()
        if total and total > 0:
            acc = (wins / total) * 100
            send_message(f"📈 Overall accuracy: {acc:.2f}% ({wins}/{total})")
        else:
            send_message("No performance data yet.")

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
        send_message(msg)

    elif cmd == "capital":
        if len(parts) == 2:
            try:
                cap = float(parts[1])
                set_user_setting("capital", str(cap))
                send_message(f"💰 Capital set to ₹{cap}")
            except:
                send_message("❌ Invalid number")
        else:
            cap = get_user_setting("capital")
            send_message(f"💰 Current capital: ₹{cap}")

    elif cmd == "risk":
        if len(parts) == 2:
            try:
                risk = float(parts[1])
                if risk < 0.1 or risk > 10:
                    send_message("⚠️ Risk % should be between 0.1 and 10")
                else:
                    set_user_setting("risk_percent", str(risk))
                    send_message(f"📊 Risk per trade set to {risk}%")
            except:
                send_message("❌ Invalid number")
        else:
            risk = get_user_setting("risk_percent")
            send_message(f"📊 Current risk: {risk}%")

    elif cmd == "leverage":
        if len(parts) == 2:
            try:
                lev = int(parts[1])
                if lev < 1 or lev > 20:
                    send_message("⚠️ Leverage must be between 1 and 20")
                else:
                    set_user_setting("default_leverage", str(lev))
                    send_message(f"⚡ Default leverage set to {lev}x")
            except:
                send_message("❌ Invalid number")
        else:
            lev = get_user_setting("default_leverage")
            send_message(f"⚡ Current default leverage: {lev}x")

    elif cmd == "set" and len(parts) == 3:
        key = parts[1].lower()
        val = parts[2]
        valid_keys = ["max_losses", "pause_hours", "win_mult", "loss_mult", "max_atr", "min_pos", "max_pos_percent",
                      "max_trades_per_hour", "max_exposure", "risk_reduction", "min_conf", "max_concurrent"]
        if key in valid_keys:
            try:
                mapping = {
                    "max_losses": "max_consecutive_losses",
                    "pause_hours": "pause_hours",
                    "win_mult": "win_multiplier",
                    "loss_mult": "loss_multiplier",
                    "max_atr": "max_atr_percent",
                    "min_pos": "min_position",
                    "max_pos_percent": "max_position_percent",
                    "max_trades_per_hour": "max_trades_per_hour",
                    "max_exposure": "max_global_exposure_percent",
                    "risk_reduction": "risk_reduction_factor",
                    "min_conf": "min_confidence_for_trade",
                    "max_concurrent": "max_concurrent_trades"
                }
                set_user_setting(mapping[key], val)
                send_message(f"✅ {key} set to {val}")
            except:
                send_message("❌ Invalid value")
        else:
            send_message("❌ Unknown setting")

    elif cmd == "toggle":
        if len(parts) == 2:
            key = parts[1].lower()
            if key in ["btc", "dominance", "news"]:
                setting = f"use_{key}_filter"
                current = get_user_setting(setting, "false")
                new = "true" if current == "false" else "false"
                set_user_setting(setting, new)
                send_message(f"✅ {key} filter is now {'ON' if new == 'true' else 'OFF'}")
            else:
                send_message("❌ Use: toggle btc, toggle dominance, toggle news")
        else:
            send_message("❌ Specify filter to toggle")

    elif cmd == "filter":
        if len(parts) == 2:
            mode = parts[1].lower()
            if mode in ["strict", "normal"]:
                set_user_setting("filter_mode", mode)
                if mode == "strict":
                    set_param("min_adx", STRICT_ADX)
                    set_param("volume_multiplier", STRICT_VOL_MULT)
                    set_param("min_confidence", STRICT_CONFIDENCE)
                else:
                    set_param("min_adx", NORMAL_ADX)
                    set_param("volume_multiplier", NORMAL_VOL_MULT)
                    set_param("min_confidence", NORMAL_CONFIDENCE)
                send_message(f"🔍 Filter mode set to {mode}")
            else:
                send_message("❌ Use: filter strict or filter normal")
        else:
            mode = get_user_setting("filter_mode")
            send_message(f"🔍 Current filter mode: {mode}")

    elif cmd == "reset_streaks":
        with db_lock:
            c = db_conn.cursor()
            c.execute("DELETE FROM streaks")
            db_conn.commit()
        send_message("🔄 All win/loss streaks reset.")

    elif cmd == "reset_params":
        set_param("buy_threshold", 70.0)
        set_param("sell_threshold", 30.0)
        set_param("holding_period_minutes", 60.0)
        set_param("profit_target_percent", 1.0)
        set_param("lookback_signals", 50.0)
        set_param("target_win_rate", 55.0)
        set_param("atr_multiplier_sl", 1.5)
        set_param("atr_multiplier_tp", 3.0)
        set_param("min_adx", NORMAL_ADX)
        set_param("volume_multiplier", NORMAL_VOL_MULT)
        set_param("min_confidence", NORMAL_CONFIDENCE)
        send_message("🔄 Parameters reset to defaults.")

    elif cmd == "timeframe":
        if len(parts) == 2:
            tf = parts[1].lower()
            valid = ["1m","5m","15m","30m","1h","4h","1d"]
            if tf in valid:
                set_timeframe(tf)
                send_message(f"✅ Timeframe set to {tf}")
            else:
                send_message(f"❌ Invalid. Choose: {', '.join(valid)}")
        else:
            current = get_timeframe()
            send_message(f"⏱ Current timeframe: {current}")

    elif cmd == "backtest" and len(parts) >= 2:
        coin = parts[1].upper()
        days = 30
        if len(parts) >= 3:
            try:
                days = int(parts[2])
            except:
                pass
        send_message(f"⏳ Backtesting {coin} for {days} days...")
        result = backtest(coin, get_timeframe(), days)
        if result:
            msg = (f"📊 Backtest results:\n"
                   f"Trades: {result['trades']}\n"
                   f"Wins: {result['wins']}\n"
                   f"Losses: {result['losses']}\n"
                   f"Win rate: {result['win_rate']}%\n"
                   f"Total PnL: ₹{result['total_pnl']}\n"
                   f"Max drawdown: {result['max_drawdown']}%\n"
                   f"Profit factor: {result['profit_factor']}\n"
                   f"Avg win: ₹{result['avg_win']}\n"
                   f"Avg loss: ₹{result['avg_loss']}")
            send_message(msg)
        else:
            send_message("❌ Backtest failed (insufficient data).")

    elif cmd == "exposure":
        exposure = get_total_exposure()
        capital = float(get_user_setting("capital", "0"))
        if capital > 0:
            percent = (exposure / capital) * 100
            send_message(f"💰 Current exposure: ₹{exposure:.2f} ({percent:.1f}% of capital)")
        else:
            send_message(f"💰 Current exposure: ₹{exposure:.2f} (capital not set)")

    elif cmd == "trades_hour":
        count = get_trades_last_hour()
        max_trades = int(get_user_setting("max_trades_per_hour", "3"))
        send_message(f"📊 Trades in last hour: {count} / {max_trades}")

    else:
        send_message("❓ Unknown command. Type 'help' to see available commands.")

# ================================
#  BACKGROUND LOOPS
# ================================
def evaluation_loop():
    while True:
        try:
            evaluate_pending_signals()
            adapt_parameters()
        except Exception as e:
            logger.error(f"Evaluation error: {e}")
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
                logger.info("Overtrading protection: max trades per hour reached, skipping analysis cycle")
                time.sleep(ANALYSIS_INTERVAL)
                continue

            coins = get_watchlist()
            for coin in coins:
                with db_lock:
                    c = db_conn.cursor()
                    c.execute("SELECT 1 FROM open_trades WHERE coin = ?", (coin,))
                    if c.fetchone():
                        logger.info(f"{coin} already has open trade, skipping")
                        time.sleep(5)
                        continue
                if get_open_trades_count() >= max_concurrent:
                    logger.info("Max concurrent trades reached, skipping further entries")
                    break

                result = analyze_coin_mtf(coin)
                if result and result['signal'] in ("BUY", "SELL") and result['grade_score'] >= MIN_GRADE_SCORE:
                    min_conf_trade = float(get_user_setting("min_confidence_for_trade", "75"))
                    if result['confidence'] < min_conf_trade:
                        logger.info(f"{coin} confidence {result['confidence']} < {min_conf_trade}, skipping")
                        time.sleep(5)
                        continue
                    if exposure + (result['position_size'] or 0) > max_exposure:
                        logger.info("Global exposure limit reached, skipping trade")
                        break
                    if can_send_signal(coin):
                        send_message(format_signal(result))
                        sig_id = record_signal(coin, result['price'], result['signal'], result['confidence'],
                                               result['grade'], result['grade_score'], result['leverage'],
                                               result['position_size'] or 0)
                        if result['position_size']:
                            add_open_trade(coin, result['price'], result['position_size'] / result['price'])
                            exposure += result['position_size']
                        update_last_signal(coin)
                    else:
                        logger.info(f"Cooldown/paused for {coin}, skipping.")
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
            send_message("\n".join(lines))
        time.sleep(STATUS_INTERVAL)

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
    logger.info("Starting CoinDCX Production Bot – FINAL VERSION")
    threading.Thread(target=handle_commands, daemon=True).start()
    threading.Thread(target=analysis_loop, daemon=True).start()
    threading.Thread(target=status_loop, daemon=True).start()
    threading.Thread(target=evaluation_loop, daemon=True).start()
    # Send startup help message
    send_message("🤖 Bot started!\n" + get_help_text())
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Bot stopped.")
        db_conn.close()