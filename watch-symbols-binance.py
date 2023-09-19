import atexit
import os
import ccxt
import time
import csv
import concurrent.futures


# Define the lock file name
lock_file_name = "crypto_data_lock.txt"
# Check if the lock file exists
if os.path.exists(lock_file_name):
    print("Another instance of the script is already running. Exiting.")
    exit()
# Create the lock file
with open(lock_file_name, "w") as lock_file:
    pass


# Function to remove the lock file on exit
def remove_lock_file():
    if os.path.exists(lock_file_name):
        os.remove(lock_file_name)


# Register the function to remove the lock file on exit
atexit.register(remove_lock_file)

# Initialize the Binance exchange object
exchange = ccxt.binance()

# Define the list of cryptocurrency pairs you want to track
pairs = [
    "BTC/USDT",
    "ETH/USDT",
    "XRP/USDT",
    "ADA/USDT",
    "DOT/USDT",
    "BNB/USDT",
    "LINK/USDT",
    "LTC/USDT",
    "BCH/USDT",
    "XLM/USDT",
    "UNI/USDT",
    "DOGE/USDT",
    "EOS/USDT",
    "TRX/USDT",
    "XMR/USDT",
    "XTZ/USDT",
    "ATOM/USDT",
    "VET/USDT",
    "FIL/USDT",
    "SOL/USDT",
    "AAVE/USDT",
    "EGLD/USDT",
    "MKR/USDT",
    "COMP/USDT",
    "ICP/USDT",
    "LUNA/USDT",
    "NEO/USDT",
    "ALGO/USDT",
    "AVAX/USDT",
]

# Define the time interval for OHLCV data (1 minute)
timeframe = "1m"

# Define the CSV file name
csv_file_name = "crypto_data-100.csv"

# Define the maximum number of concurrent tasks
max_workers = min(10, len(pairs))  # Limit the number of concurrent tasks


# Function to fetch data for a single pair
def fetch_data(pair):
    try:
        order_book = exchange.fetch_order_book(pair, limit=100)
        ohlcv = exchange.fetch_ohlcv(pair, timeframe)

        if ohlcv:
            # Extract the most recent OHLCV data
            ohlcv_data = ohlcv[-1]
            timestamp = ohlcv_data[0]
            open_price = ohlcv_data[1]
            high_price = ohlcv_data[2]
            low_price = ohlcv_data[3]
            close_price = ohlcv_data[4]
            volume = ohlcv_data[5]

            # Extract the top 100 asks and bids from the order book
            asks = order_book["asks"]  # [:5]
            bids = order_book["bids"]  # [:5]

            # Flatten the ask and bid data into separate lists
            ask_volumes = [ask[1] for ask in asks]
            ask_prices = [ask[0] for ask in asks]
            bid_volumes = [bid[1] for bid in bids]
            bid_prices = [bid[0] for bid in bids]

            # Combine OHLCV, asks, and bids data
            row_data = (
                [
                    timestamp,
                    pair,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    volume,
                ]
                + ask_volumes
                + ask_prices
                + bid_volumes
                + bid_prices
            )

            # Print the data for debugging (optional)
            print(
                f"Timestamp: {timestamp}, Pair: {pair}, Open: {open_price},"
                f" High: {high_price}, Low: {low_price}, Close: {close_price},"
                f" Volume: {volume}"
            )
            print(f"Asks: {asks}")
            print(f"Bids: {bids}")

            return row_data
        else:
            return None
    except Exception as e:
        print(f"An error occurred for {pair}: {e}")
        return None


# Check if the CSV file exists and has a size of zero bytes
if not os.path.exists(csv_file_name) or os.path.getsize(csv_file_name) == 0:
    write_header = True
else:
    write_header = False

# Open the CSV file in write mode
with open(csv_file_name, mode="a", newline="") as csv_file:
    csv_writer = csv.writer(csv_file)

    if write_header:
        # Define the columns for asks and bids
        ask_columns = [f"AV{i+1}" for i in range(100)]  # Ask volume columns
        ask_columns += [f"AP{i+1}" for i in range(100)]  # Ask price columns
        bid_columns = [f"BV{i+1}" for i in range(100)]  # Bid volume columns
        bid_columns += [f"BP{i+1}" for i in range(100)]  # Bid price columns

        # Write the CSV header
        csv_header = (
            ["Timestamp", "Pair", "Open", "High", "Low", "Close", "Volume"]
            + ask_columns
            + bid_columns
        )
        csv_writer.writerow(csv_header)

    # Create a ThreadPoolExecutor with the specified number of workers
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=max_workers
    ) as executor:
        # Continuous data fetching loop
        while True:
            try:
                # Fetch data concurrently for each pair
                future_to_pair = {
                    executor.submit(fetch_data, pair): pair for pair in pairs
                }
                for future in concurrent.futures.as_completed(future_to_pair):
                    pair = future_to_pair[future]
                    data = future.result()
                    if data:
                        # Write the data to the CSV file
                        csv_writer.writerow(data)
            except Exception as e:
                print(f"An error occurred: {e}")

            # Wait for 1 minute before fetching data again
            time.sleep(60)
