from luno_python.client import Client
import pandas as pd
import datetime
import csv
import schedule
import time
import pytz


c = Client(api_key_id='wx9h7rv46mmf', api_key_secret='ZbB8SfrtcotdQ47nSu-RBvN2JK8odVVxwh51X4DVOI0')

def calculate_rsi(data, window=14):
    """Calculate the RSI for the given DataFrame."""
    delta = data['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def fetch_and_process_data():
    res = c.get_candles(pair='ETHZAR', since=1721080800000, duration=300)
    
    # Convert the candles to a DataFrame
    candles_df = pd.DataFrame(res['candles'])

    # Convert the timestamp to standard datetime format (UTC)
    candles_df['timestamp'] = pd.to_datetime(candles_df['timestamp'], unit='ms', utc=True)

    # Convert to South African time (SAST)
    candles_df['timestamp'] = candles_df['timestamp'].dt.tz_convert('Africa/Johannesburg')

    # Remove the timezone info (make it naive)
    candles_df['timestamp'] = candles_df['timestamp'].dt.tz_localize(None)

    # Select only the required columns and convert them to numeric
    candles_df['open'] = pd.to_numeric(candles_df['open'])
    candles_df['close'] = pd.to_numeric(candles_df['close'])
    candles_df['high'] = pd.to_numeric(candles_df['high'])
    candles_df['low'] = pd.to_numeric(candles_df['low'])
    candles_df['volume'] = pd.to_numeric(candles_df['volume'])

    candles_df = candles_df[['timestamp', 'open', 'close', 'high', 'low', 'volume']]

    # Calculate the RSI and add it to the DataFrame
    candles_df['RSI'] = calculate_rsi(candles_df)

    # Display the DataFrame
    print(candles_df)

# Schedule the task every 5 minutes
schedule.every(5).minutes.do(fetch_and_process_data)

while True:
    schedule.run_pending()
    time.sleep(1)
