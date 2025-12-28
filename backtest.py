import pandas as pd
from sqlalchemy import create_engine
from strategy.engine import BreakoutStrategy

# Connection
engine = create_engine("postgresql://trader:laminenba@localhost:5432/marketdata")

def run_5m_backtest():
    # 1. Load 1s data
    print("Fetching data...")
    df = pd.read_sql("SELECT ts, open, high, low, close, volume FROM spy_ohlc_1s ORDER BY ts ASC", engine)
    if df.empty: 
        print("No data found!")
        return

    df['ts'] = pd.to_datetime(df['ts'])
    df.set_index('ts', inplace=True)

    # 2. Resample to 5 Minutes
    print("Resampling to 5m candles...")
    df_5m = df.resample('5min').agg({
        'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'
    }).dropna().reset_index()

    # 3. Apply Strategy
    strat = BreakoutStrategy(ma_period=50, pvt_len=3, stop_buffer_pct=0.0005)
    df_results = strat.generate_signals(df_5m)

    # 4. Trade Simulation
    trades = []
    in_pos = False
    entry_data = {}

    for i, row in df_results.iterrows():
        if not in_pos and row['buy_signal']:
            in_pos = True
            entry_data = {'price': row['close'], 'time': row['ts']}
        elif in_pos and row['sell_signal']:
            in_pos = False
            pnl = row['close'] - entry_data['price']
            trades.append({
                'entry_time': entry_data['time'],
                'exit_time': row['ts'],
                'entry_price': entry_data['price'],
                'exit_price': row['close'],
                'return_pct': (pnl / entry_data['price']) * 100
            })

    # 5. Output
    report = pd.DataFrame(trades)
    if not report.empty:
        print(f"\n--- Backtest Results (5m) ---")
        print(f"Total Trades: {len(report)}")
        print(f"Win Rate: {(report['return_pct'] > 0).mean():.2%}")
        print(f"Total Return: {report['return_pct'].sum():.2%}")
        print(report.tail())
    else:
        print("No trades executed with current parameters.")

    
    # Export for inspection
    report.to_csv('backtest_results_5m.csv', index=False)
    
if __name__ == "__main__":
    run_5m_backtest()