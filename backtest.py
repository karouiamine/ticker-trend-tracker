import pandas as pd
import psycopg2
from strategy.engine import BreakoutStrategy

# Database Connection
DB_CONFIG = {
    "host": "localhost", # Change to 'postgres' if running inside Docker
    "dbname": "marketdata",
    "user": "trader",
    "password": "laminenba"
}

def load_and_resample(timeframe='5min'):
    conn = psycopg2.connect(**DB_CONFIG)
    print(f"Loading 1s data and resampling to {timeframe}...")
    
    # 1. Load the raw 1-second data
    query = "SELECT ts, open, high, low, close, volume FROM spy_ohlc_1s ORDER BY ts ASC;"
    df = pd.read_sql(query, conn)
    
    # Ensure 'ts' is the index for resampling
    df['ts'] = pd.to_datetime(df['ts'])
    df.set_index('ts', inplace=True)
    
    # 2. Resample to OHLCV candles
    # '5min' or '5T' groups the data into 5-minute blocks
    resampled_df = df.resample(timeframe).agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    })
    
    # Remove any empty candles (e.g., if there was no trading in a 5m block)
    resampled_df.dropna(subset=['close'], inplace=True)
    print(f"Resampled {len(df)} 1s-rows into {len(resampled_df)} {timeframe}-candles.")
    # Reset index to make 'ts' a column again for the strategy engine
    return resampled_df.reset_index()

def run_backtest():
    # Load data at 5-minute resolution
    df_5m = load_and_resample('5min')
    # Use the BreakoutStrategy Class
    # Note: On 5m candles, you might want to adjust pvt_len or ma_period
    strat = BreakoutStrategy(ma_period=50, pvt_len=2, stop_buffer_pct=0.0005)
    df_signals = strat.generate_signals(df_5m)

    in_position = False
    entry_price = 0
    trades = []

    for i in range(len(df_signals)):
        row = df_signals.iloc[i]
        
        if not in_position and row['buy_signal']:
            in_position = True
            entry_price = row['close']
            entry_time = row['ts']
        
        elif in_position and row['sell_signal']:
            in_position = False
            pnl = row['close'] - entry_price
            trades.append({
                'entry_time': entry_time,
                'exit_time': row['ts'],
                'pnl': pnl,
                'return': (pnl / entry_price) * 100
            })

    results = pd.DataFrame(trades)
    
    # Export for inspection
    results.to_csv('backtest_results_5m.csv', index=False)
    return results

# Execute
trade_results = run_backtest()
print(trade_results)