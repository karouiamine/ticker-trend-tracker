import time
from datetime import datetime, timezone
from ib_insync import *
from strategy.engine import BreakoutStrategy
import pandas as pd

# 1. Connection & Setup
ib = IB()
ib.connect('127.0.0.1', 7497, clientId=10) # 7497 is standard for Paper Trading

contract = Stock('SPY', 'ARCA', 'USD')
ib.qualifyContracts(contract)

# Strategy Instance
strat = BreakoutStrategy(ma_period=50, pvt_len=3, stop_buffer_pct=0.0005)

def get_historical_and_live_data():
    """Initial fetch to populate indicators."""
    print("Populating initial history...")
    # Fetch 2 days of 5-minute bars
    bars = ib.reqHistoricalData(
        contract, endDateTime='', durationStr='2 D',
        barSizeSetting='5 mins', whatToShow='TRADES', useRTH=True
    )
    df = util.df(bars)
    df['ts'] = pd.to_datetime(df['date'])
    return df

def on_bar_update(bars, has_new_bar):
    """Event handler triggered every 5 minutes when a candle closes."""
    if not has_new_bar:
        return

    # Convert bars to DataFrame
    df = util.df(bars)
    df['ts'] = pd.to_datetime(df['date'])
    
    # Generate Signals
    df_signals = strat.generate_signals(df)
    last_row = df_signals.iloc[-1]
    
    print(f"[{datetime.now()}] Price: {last_row['close']} | Buy Level: {last_row['buy_level']:.2f} | Stop: {last_row['stop_level']:.2f}")

    # Check Position
    positions = ib.positions()
    current_pos = next((p for p in positions if p.contract.symbol == 'SPY'), None)
    in_position = current_pos is not None and current_pos.size != 0

    # 2. Logic Execution
    if not in_position and last_row['buy_signal']:
        print("!!! BUY SIGNAL DETECTED !!!")
        order = MarketOrder('BUY', 10) # Buy 10 shares
        ib.placeOrder(contract, order)
    
    elif in_position and last_row['sell_signal']:
        print("!!! SELL SIGNAL DETECTED (STOP HIT) !!!")
        order = MarketOrder('SELL', abs(current_pos.size))
        ib.placeOrder(contract, order)

# 3. Main Loop
# Populate history first
bars = ib.reqRealTimeBars(contract, 5, 'TRADES', useRTH=True)
# Subscribe to the update event
bars.updateEvent += on_bar_update

print("Live Paper Trader started. Waiting for 5-minute bar closures...")
ib.run()