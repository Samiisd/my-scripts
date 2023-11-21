import os
import asyncio
from loguru import logger
import csv
import gzip
from binance import AsyncClient, BinanceSocketManager
from datetime import datetime, timedelta


class Status:
    def __init__(self, interval):
        self.interval = interval
        self.start_time = datetime.utcnow()
        self.last_report_time = None
        self.error_count = 0
        self.ticker_count = 0
        self.total_ticker_count = 0
        self.total_error_count = 0
        self.count_per_symbol = {}

    def increment_error(self):
        self.error_count += 1

    def increment_24hrTicker(self):
        self.ticker_count += 1

    def seen_symbol(self, symbol):
        self.count_per_symbol[symbol] = self.count_per_symbol.get(symbol, 0) + 1

    def tick_report(self):
        now = datetime.utcnow()
        if (
            self.last_report_time is None
            or now - self.last_report_time >= self.interval
        ):
            self.total_ticker_count += self.ticker_count
            self.total_error_count += self.error_count
            # total_symbols = sum(self.count_per_symbol.values())
            # mean_count_symbols = total_symbols / len(self.count_per_symbol)
            logger.info(
                f"Status Report - Tickers: {self.ticker_count} (total={self.total_ticker_count}), Errors: {self.error_count} (total={self.total_error_count}), Total Symbols: {len(self.count_per_symbol)}, Uptime: {now - self.start_time}"
            )
            self.last_report_time = now
            self.error_count = 0
            self.ticker_count = 0


column_names = [
    "EventType",
    "EventTime",
    "Symbol",
    "PriceChange",
    "PriceChangePercent",
    "WeightedAvgPrice",
    "PrevClosePrice",
    "LastPrice",
    "LastQty",
    "BidPrice",
    "BidQty",
    "AskPrice",
    "AskQty",
    "OpenPrice",
    "HighPrice",
    "LowPrice",
    "TotalTradedVolume",
    "TotalTradedQuoteAssetVolume",
    "OpenTime",
    "CloseTime",
    "FirstTradeId",
    "LastTradeId",
    "TotalTrades",
    "TakerBuyBaseAssetVolume",
    "TakerBuyQuoteAssetVolume",
]


async def main():
    client = await AsyncClient.create()
    bm = BinanceSocketManager(client)

    filename = "binance_ticker_data.csv.gz"
    while True:
        file_exists = os.path.exists(filename)
        try:
            with gzip.open(filename, "at", newline="", encoding="utf-8") as file:
                write = csv.writer(file)

                if not file_exists:
                    write.writerow(column_names[1:])

                status = Status(timedelta(seconds=30))

                async with bm.ticker_socket() as ts:
                    while True:
                        messages = await ts.recv()
                        for message in messages:
                            match message["e"]:
                                case "error":
                                    logger.error(message)
                                    status.increment_error()
                                case "24hrTicker":
                                    write.writerow(list(message.values())[1:])
                                    status.seen_symbol(message["s"])
                                    status.increment_24hrTicker()
                                case _:
                                    logger.warning(
                                        "Unknown message type: {}", message["e"]
                                    )
                        status.tick_report()
        except Exception as e:
            if file is not None:
                file.close()
            logger.error(f"An error occurred: {str(e)}")
            await asyncio.sleep(5)

    await client.close_connection()


if __name__ == "__main__":
    asyncio.run(main())
