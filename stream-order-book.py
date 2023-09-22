import atexit
import csv
import os
import asyncio
import websockets
from math import floor
from time import time
import json


async def on_message(ws, message, symbol: str, csv_writer):
    data = json.loads(message)

    ts = int(floor(time() * 1000))

    asks = data["asks"]
    bids = data["bids"]

    # Flatten the ask and bid data into separate lists
    ask_volumes = [ask[1] for ask in asks]
    ask_prices = [ask[0] for ask in asks]
    bid_volumes = [bid[1] for bid in bids]
    bid_prices = [bid[0] for bid in bids]

    row_data = (
        [
            ts,
            symbol,
        ]
        + ask_volumes
        + ask_prices
        + bid_volumes
        + bid_prices
    )

    # Process and handle the data as needed
    csv_writer.writerow(row_data)
    print(
        ts, symbol, f"A=[{asks[0]}...{asks[-1]}]", f"B=[{bids[0]}...{bids[-1]}]"
    )


async def binance_partial_depth_stream(symbol, csv_writer):
    url = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@depth20"
    async with websockets.connect(url) as ws:
        while True:
            message = await ws.recv()
            await on_message(ws, message, symbol, csv_writer)


async def _write_headers_csv(csv_file_name: str):
    if os.path.exists(csv_file_name):
        return

    ask_columns = [f"AV{i+1}" for i in range(20)]  # Ask volume columns
    ask_columns += [f"AP{i+1}" for i in range(20)]  # Ask price columns
    bid_columns = [f"BV{i+1}" for i in range(20)]  # Bid volume columns
    bid_columns += [f"BP{i+1}" for i in range(20)]  # Bid price columns
    headers = ["Timestamp", "Symbol"] + ask_columns + bid_columns

    with open(csv_file_name, "w", newline="") as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(headers)


def _check_unique_instance(lock_file_name: str):
    if os.path.exists(lock_file_name):
        print(
            "Another instance of the script is already running. Exiting. (If"
            " you are sure that no other instance is running, delete the lock"
            f" file: {lock_file_name}.))"
        )
        exit(-1)
    # Create the lock file
    with open(lock_file_name, "w") as lock_file:
        pass

    # Function to remove the lock file on exit
    def remove_lock_file():
        if os.path.exists(lock_file_name):
            os.remove(lock_file_name)

    # Register the function to remove the lock file on exit
    atexit.register(remove_lock_file)


async def main():
    _check_unique_instance(".crypto-stream-order-book.lock")
    symbols = [
        "BTCUSDT",
        "ETHUSDT",
        "XRPUSDT",
        "ADAUSDT",
        "DOTUSDT",
        "BNBUSDT",
        "LINKUSDT",
        "LTCUSDT",
        "BCHUSDT",
        "XLMUSDT",
        "UNIUSDT",
        "DOGEUSDT",
        "EOSUSDT",
        "TRXUSDT",
        "XMRUSDT",
        "XTZUSDT",
        "ATOMUSDT",
        "VETUSDT",
        "FILUSDT",
        "SOLUSDT",
        "AAVEUSDT",
        "EGLDUSDT",
        "MKRUSDT",
        "COMPUSDT",
        "ICPUSDT",
        "LUNAUSDT",
        "NEOUSDT",
        "ALGOUSDT",
        "AVAXUSDT",
    ]
    csv_filename = "binance_order_book_data.csv"
    await _write_headers_csv(csv_filename)

    with open(csv_filename, "a", newline="") as csvfile:
        csv_writer = csv.writer(csvfile)
        tasks = [
            binance_partial_depth_stream(symbol, csv_writer)
            for symbol in symbols
        ]
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
