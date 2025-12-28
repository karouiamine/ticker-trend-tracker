### this file is just to plot a visulaization for our strategy you can find the visualization in the Readme
import pandas as pd
import matplotlib.pyplot as plt
import mplfinance as mpf
from sqlalchemy import create_engine
from strategy.engine import BreakoutStrategy

# 1. Setup Connection and Load Data
engine = create_engine("postgresql://trader:laminenba@localhost:5432/marketdata")
df = pd.read_sql("SELECT ts, open, high, low, close, volume FROM spy_ohlc_1s ORDER BY ts ASC", engine)
df['ts'] = pd.to_datetime(df['ts'])
df.set_index('ts', inplace=True)

# 2. Resample to 5-Minute Candles
df_5m = df.resample('5min').agg({
    'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'
}).dropna()

# 3. Calculate Indicators
strat = BreakoutStrategy(ma_period=50, pvt_len=3)
df_plot = strat.generate_signals(df_5m.copy())

# 4. Prepare Add-on Plots for mplfinance
# Trend Filter MA
apds = [
    mpf.make_addplot(df_plot['ma_filter'], color='blue', width=1.5),
    # Pivot Levels (Buy/Stop Levels)
    mpf.make_addplot(df_plot['buy_level'], color='green', linestyle='--', width=0.8),
    mpf.make_addplot(df_plot['stop_level'], color='red', linestyle='--', width=0.8)
]

# 5. Highlight Trades (Triangles)
df_plot['entry_marker'] = df_plot['close'].where(df_plot['buy_signal'], None)
# Simple exit marker logic for visualization
df_plot['exit_marker'] = df_plot['close'].where(df_plot['sell_signal'], None)

if not df_plot['entry_marker'].isnull().all():
    apds.append(mpf.make_addplot(df_plot['entry_marker'], type='scatter', marker='^', markersize=100, color='green'))
if not df_plot['exit_marker'].isnull().all():
    apds.append(mpf.make_addplot(df_plot['exit_marker'], type='scatter', marker='v', markersize=100, color='red'))

# 6. Generate Plot
print("Generating Chart...")
mpf.plot(df_plot, type='candle', style='charles',
         title='SPY 5m - Breakout Trend Follower',
         ylabel='Price ($)',
         addplot=apds,
         volume=True,
         figsize=(12, 8),
         savefig='strategy_chart.png')