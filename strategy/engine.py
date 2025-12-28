import pandas as pd

class BreakoutStrategy:
    def __init__(self, ma_period=50, pvt_len=3, stop_buffer_pct=0.001):
        self.ma_period = ma_period
        self.pvt_len = pvt_len
        self.stop_buffer_pct = stop_buffer_pct

    def calculate_indicators(self, df):
        df['ma_filter'] = df['close'].rolling(window=self.ma_period).mean()

        # Pivot Highs/Lows
        window = self.pvt_len * 2 + 1
        df['is_pvt_high'] = df['high'] == df['high'].rolling(window=window, center=True).max()
        df['is_pvt_low'] = df['low'] == df['low'].rolling(window=window, center=True).min()

        # SHIFT: We can't know a pivot happened until 'pvt_len' bars later
        df['buy_level'] = df['high'].where(df['is_pvt_high']).ffill().shift(self.pvt_len)
        df['stop_level'] = df['low'].where(df['is_pvt_low']).ffill().shift(self.pvt_len)
        return df

    def generate_signals(self, df):
        df = self.calculate_indicators(df)
        # buy_level needs to be non-null and price must cross it
        df['buy_signal'] = (df['close'] > df['buy_level']) & (df['close'] > df['ma_filter'])
        df['sell_signal'] = (df['close'] < df['stop_level'])
        return df