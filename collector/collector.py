# # -----------------------------------------
# # Live Data Collector
# # -----------------------------------------
# from ib_insync import *
# import psycopg2
# from psycopg2.extras import execute_values
# from datetime import datetime, timedelta, timezone
# import time
# import os

# # -------------------------
# # CONFIG FROM ENV
# # -------------------------
# IB_HOST = os.getenv("IB_HOST", "host.docker.internal")
# IB_PORT = int(os.getenv("IB_PORT", "7496"))
# CLIENT_ID = int(os.getenv("IB_CLIENT_ID", "21"))

# DB_HOST = os.getenv("DB_HOST", "postgres")
# DB_PORT = int(os.getenv("DB_PORT", "5432"))
# DB_NAME = os.getenv("DB_NAME", "marketdata")
# DB_USER = os.getenv("DB_USER", "trader")
# DB_PASS = os.getenv("DB_PASS", "laminenba")

# SYMBOL = "SPY"

# # -------------------------
# # CONNECT DB
# # -------------------------
# def wait_for_postgres():
#     while True:
#         try:
#             return psycopg2.connect(
#                 host=DB_HOST,
#                 port=DB_PORT,
#                 dbname=DB_NAME,
#                 user=DB_USER,
#                 password=DB_PASS
#             )
#         except psycopg2.OperationalError:
#             print("Waiting for Postgres...")
#             time.sleep(2)

# conn = wait_for_postgres()
# conn.autocommit = True
# cur = conn.cursor()

# INSERT_SQL = """
# INSERT INTO spy_ohlc_1s (ts, symbol, open, high, low, close, volume)
# VALUES %s
# ON CONFLICT (ts) DO NOTHING;
# """

# # -------------------------
# # CONNECT IBKR
# # -------------------------
# ib = IB()
# ib.connect(IB_HOST, IB_PORT, clientId=CLIENT_ID, timeout=10)

# contract = Stock("SPY", "SMART", "USD", primaryExchange="ARCA")
# ib.qualifyContracts(contract)

# print("Collector started")

# # -------------------------
# # MAIN LOOP
# # -------------------------
# while True:
#     try:
#         # lag by 10 seconds (VERY important)
#         end_time = datetime.now(timezone.utc) - timedelta(seconds=10)

#         bars = ib.reqHistoricalData(
#             contract,
#             endDateTime=end_time.strftime("%Y%m%d %H:%M:%S"),
#             durationStr="30 S",
#             barSizeSetting="1 secs",
#             whatToShow="TRADES",
#             useRTH=False,
#             formatDate=1
#         )

#         if bars:
#             rows = [
#                 (
#                     bar.date.astimezone(timezone.utc),
#                     SYMBOL,
#                     bar.open,
#                     bar.high,
#                     bar.low,
#                     bar.close,
#                     int(bar.volume)
#                 )
#                 for bar in bars
#             ]

#             execute_values(cur, INSERT_SQL, rows)
#             print(f"Inserted {len(rows)} bars up to {rows[-1][0]}")

#         time.sleep(5)  # pacing protection

#     except Exception as e:
#         print("Error:", e)
#         time.sleep(10)

# # ---------------------------------------
# # Collect yesterday data
# # ---------------------------------------
from ib_insync import *
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta, timezone
import time
import os

# -------------------------
# CONFIG FROM ENV
# -------------------------
IB_HOST = os.getenv("IB_HOST", "host.docker.internal")
IB_PORT = int(os.getenv("IB_PORT", "7496"))
CLIENT_ID = int(os.getenv("IB_CLIENT_ID", "21"))

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "marketdata")
DB_USER = os.getenv("DB_USER", "trader")
DB_PASS = os.getenv("DB_PASS", "laminenba")

SYMBOL = "SPY"

# -------------------------
# CONNECT DB
# -------------------------
def wait_for_postgres():
    while True:
        try:
            return psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASS
            )
        except psycopg2.OperationalError:
            print("Waiting for Postgres...")
            time.sleep(2)

conn = wait_for_postgres()
conn.autocommit = True
cur = conn.cursor()

INSERT_SQL = """
INSERT INTO spy_ohlc_1s (ts, symbol, open, high, low, close, volume)
VALUES %s
ON CONFLICT (ts) DO NOTHING;
"""

# -------------------------
# BACKFILL LOGIC
# -------------------------
def backfill_in_chunks(ib, contract, total_days=1):
    """
    Break backfill into 30-minute chunks to avoid 'invalid step' errors.
    """
    # 1-second bars are limited in duration per request
    chunk_size = timedelta(minutes=30) 
    
    # Set the end point to the last Friday close
    end_point = datetime(2025, 12, 26, 20, 0, 0, tzinfo=timezone.utc)
    start_point = end_point - timedelta(days=total_days)
    
    current_end = end_point
    
    print(f"Starting chunked backfill from {end_point} backwards...")

    while current_end > start_point:
        formatted_end = current_end.strftime('%Y%m%d %H:%M:%S')
        print(f"Requesting 30m chunk ending at: {formatted_end}")
        
        try:
            # Small chunks are less likely to trigger 'invalid step'
            bars = ib.reqHistoricalData(
                contract,
                endDateTime=formatted_end,
                durationStr='1800 S', # 30 minutes in seconds
                barSizeSetting='1 secs',
                whatToShow='TRADES',
                useRTH=False,
                formatDate=1
            )
            
            if bars:
                save_bars(bars)
                print(f"  Successfully saved {len(bars)} bars.")
            else:
                print("  No data returned for this chunk. (Likely subscription required)")

            # Move back in time
            current_end -= chunk_size
            
            # CRITICAL: Sleep between chunks to avoid pacing (Limit: 60 req / 10 min)
            # 12 seconds = 5 requests per minute = 50 per 10 mins (Safe zone)
            time.sleep(12) 

        except Exception as e:
            if "162" in str(e):
                print("  Pacing violation detected. Cooling down for 60s...")
                time.sleep(60)
            else:
                print(f"  Error in chunk: {e}")
                current_end -= chunk_size # Skip failed chunk

def backfill_entire_week(ib, contract, weeks_back=1):
    # Set target to Friday Close (9 PM UTC)
    end_date = datetime(2025, 12, 26, 21, 0, 0, tzinfo=timezone.utc)
    start_date = end_date - timedelta(days=7 * weeks_back)
    
    current_cursor = end_date
    chunk_size = timedelta(minutes=30)

    print(f"Starting Smart Backfill from {end_date} backwards...")

    while current_cursor > start_date:
        # 1. Skip Weekends (Saturday=5, Sunday=6)
        if current_cursor.weekday() >= 5:
            print(f"Weekend detected. Jumping to Friday close.")
            current_cursor = current_cursor.replace(hour=21, minute=0, second=0) - timedelta(days=current_cursor.weekday() - 4)
            continue

        # 2. Skip Overnight Hours (If cursor is before 14:30 UTC, jump to previous day 21:00 UTC)
        # UTC 14:30 is 9:30 AM EST
        if current_cursor.hour < 14 or (current_cursor.hour == 14 and current_cursor.minute < 30):
            print(f"Market closed ({current_cursor.time()} UTC). Jumping to previous day close.")
            current_cursor = (current_cursor - timedelta(days=1)).replace(hour=21, minute=0, second=0)
            continue

        formatted_end = current_cursor.strftime('%Y%m%d %H:%M:%S')
        print(f"Requesting RTH chunk ending at: {formatted_end} UTC")

        try:
            bars = ib.reqHistoricalData(
                contract,
                endDateTime=formatted_end,
                durationStr='1800 S', 
                barSizeSetting='1 secs',
                whatToShow='TRADES',
                useRTH=True, # API will still double-check RTH for us
                formatDate=1
            )

            if bars:
                save_bars(bars)
                # Success: Move cursor back by the exact amount we just downloaded
                current_cursor -= chunk_size
                print(f"  Saved {len(bars)} bars. Cursor now at {current_cursor}")
            else:
                # If no data, the market was likely closed (Holiday like Dec 25)
                # Jump back one day to keep the loop moving
                print(f"  No data for {formatted_end}. Jumping back 1 day.")
                current_cursor = (current_cursor - timedelta(days=1)).replace(hour=21, minute=0, second=0)

            # Pacing Protection
            time.sleep(12) 

        except Exception as e:
            if "162" in str(e):
                print("Pacing violation! Sleeping 5 mins...")
                time.sleep(300)
            else:
                print(f"Error: {e}")
                current_cursor -= chunk_size

    print("Weekly backfill complete!")

def save_bars(bars):
    """Helper to format and insert bars into Postgres."""
    rows = [
    (
    bar.date.astimezone(timezone.utc),
    SYMBOL,
    bar.open,
    bar.high,
    bar.low,
    bar.close,
    int(bar.volume)
    )
    for bar in bars
    ]
    print(rows)
    execute_values(cur, INSERT_SQL, rows)



# -------------------------
# MAIN EXECUTION
# -------------------------
ib = IB()
print("Connecting to IBKR...")
try:
    ib.connect(IB_HOST, IB_PORT, clientId=CLIENT_ID)
    print("Connected to IBKR")
    
    contract = Stock("SPY", "SMART", "USD", primaryExchange="ARCA")
    ib.qualifyContracts(contract)
    print("Contract qualified")
    # Run Chunked Backfill
    #backfill_in_chunks(ib, contract, total_days=1)
    backfill_entire_week(ib, contract, weeks_back=1)
    
    print("Backfill process finished. Entering live loop...")
    
    # LIVE LOOP (Same as your previous logic, but ensure it's inside this try block)
    while True:
        # (Your live loop code here)
        time.sleep(15)

except Exception as e:
    print(f"Fatal Error: {e}")
# DO NOT CALL ib.disconnect() HERE!

print("Collector entering real-time loop...")

# -------------------------
# MAIN LOOP
# -------------------------
# while True:
#     try:
#         # Use a 20s lag for better data stability on weekends/holidays
#         end_time = datetime.now(timezone.utc) - timedelta(seconds=20)

#         bars = ib.reqHistoricalData(
#             contract,
#             endDateTime=end_time.strftime("%Y%m%d %H:%M:%S"),
#             durationStr="60 S", 
#             barSizeSetting="1 secs",
#             whatToShow="TRADES",
#             useRTH=False,
#             formatDate=1
#         )

#         if bars:
#             save_bars(bars)
#             print(f"Inserted {len(bars)} bars up to {bars[-1].date}")

#         # Increased to 15s to be 100% safe from the 60-req/10-min limit
#         time.sleep(15) 

#     except Exception as e:
#         if "162" in str(e):
#             print("Pacing violation (162) - entering 2-minute cooldown...")
#             time.sleep(120) # Stay in the 'penalty box' until the window clears
#         else:
#             print("Error:", e)
#             time.sleep(20)