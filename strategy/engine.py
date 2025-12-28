import pandas as pd
import numpy as np

class BreakoutStrategy:
    def __init__(self, ma_period=50, pvt_len=3, stop_buffer_pct=0.0):
        self.ma_period = ma_period
        self.pvt_len = pvt_len
        self.stop_buffer_pct = stop_buffer_pct

    def generate_signals(self, df):
        # 1. Trend Filter
        df['ma_filter'] = df['close'].rolling(window=self.ma_period).mean()

        # 2. Pivot Identification (Mirroring Pine ta.pivothigh/low)
        # We need a window of pvt_len on both sides
        window = self.pvt_len * 2 + 1
        df['is_pvt_high'] = df['high'] == df['high'].rolling(window=window, center=True).max()
        df['is_pvt_low'] = df['low'] == df['low'].rolling(window=window, center=True).min()

        # 3. Dynamic Levels (Shifted to prevent look-ahead bias)
        # We only know a pivot happened 'pvt_len' bars after it peaked
        df['buy_level'] = df['high'].where(df['is_pvt_high']).ffill().shift(self.pvt_len)
        df['raw_stop'] = df['low'].where(df['is_pvt_low']).ffill().shift(self.pvt_len)
        df['stop_level'] = df['raw_stop'] * (1 - self.stop_buffer_pct)

        # 4. Signal Logic
        df['buy_signal'] = (df['close'] > df['buy_level']) & (df['close'] > df['ma_filter'])
        df['sell_signal'] = (df['close'] < df['stop_level'])

        return df