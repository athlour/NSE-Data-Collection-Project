import os
import csv
import asyncio
from datetime import datetime
from fyers_apiv3.FyersWebsocket import data_ws


class AsyncDataRecorder:
    def __init__(self, filename="data.csv"):
        self.filename = filename
        self.ensure_file_exists()
        print(f"Initialized AsyncDataRecorder with file: {self.filename}")

    def ensure_file_exists(self):
        """Ensure that the file exists and has headers."""
        path = os.getcwd()
        full_path = path+"\\"+self.filename
        with open(full_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["symbol", "ltp", "vol_traded_today", "last_traded_time", "exch_feed_time",
                             "bid_size", "ask_size", "bid_price", "ask_price", "last_traded_qty",
                             "tot_buy_qty", "tot_sell_qty", "avg_trade_price", "low_price",
                             "high_price", "lower_ckt", "upper_ckt", "open_price", "prev_close_price",
                             "ch", "chp"])
        print(f"Created file with headers: {full_path}")



    async def save_to_file(self, message):
        async with asyncio.Lock():
            await asyncio.sleep(0)
            data_row = [
                message.get("symbol"), message.get("ltp"), message.get("vol_traded_today"),
                message.get("last_traded_time"), message.get("exch_feed_time"), message.get("bid_size"),
                message.get("ask_size"), message.get("bid_price"), message.get("ask_price"),
                message.get("last_traded_qty"), message.get("tot_buy_qty"), message.get("tot_sell_qty"),
                message.get("avg_trade_price"), message.get("low_price"), message.get("high_price"),
                message.get("lower_ckt"), message.get("upper_ckt"), message.get("open_price"),
                message.get("prev_close_price"), message.get("ch"), message.get("chp")
            ]
            print(f"Saving data to {self.filename}: {data_row}")
            with open(self.filename, mode='a', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(data_row)
                file.flush()
                os.fsync(file.fileno())  # Force the OS to flush the file to disk
            print(f"Data successfully saved to {self.filename}")


class FyersWebSocketClient:
    def __init__(self, access_token):
        self.access_token = access_token
        self.fyers = None
        self.symbols = ['NSE:SPIC-EQ', 'NSE:YESBANK-EQ']
        self.data_type = "SymbolUpdate"
        self.data_recorders = {}

    async def onmessage(self, message):
        print("Received message:", message)
        symbol = message.get("symbol")
        if symbol:
            if symbol not in self.data_recorders:
                # Get the current date
                date = datetime.now().strftime("%Y-%m-%d")
                # Create a new AsyncDataRecorder for each symbol with a unique filename
                filename = symbol[4:]  # Removing the "NSE:" prefix
                self.data_recorders[symbol] = AsyncDataRecorder(filename=f"{filename}-data-{date}.csv")
            await self.data_recorders[symbol].save_to_file(message)


    def onmessage_sync(self, message):
        asyncio.run(self.onmessage(message))

    def onerror(self, message):
        print("Fyers WebSocket Error:", message)

    def onclose(self, message):
        print("Connection closed:", message)

    def onopen(self):
        self.subscribe_initial_symbols()

    def subscribe_initial_symbols(self):
        uppercase_symbols = [symbol.upper() for symbol in self.symbols]
        self.fyers.subscribe(symbols=uppercase_symbols, data_type=self.data_type)
        self.fyers.keep_running()

    def connect(self):
        self.fyers = data_ws.FyersDataSocket(
            access_token=self.access_token,
            log_path="",
            litemode=False,
            write_to_file=False,
            reconnect=True,
            on_connect=self.onopen,
            on_close=self.onclose,
            on_error=self.onerror,
            on_message=self.onmessage_sync,
            reconnect_retry=10
        )
        self.fyers.connect()

    def add_symbols(self, symbols):
        uppercase_symbols = [symbol.upper() for symbol in symbols]
        self.symbols.extend(uppercase_symbols)
        if self.fyers:
            self.fyers.subscribe(symbols=uppercase_symbols, data_type=self.data_type)
        return f"Subscribed to new symbols: {symbols}, List of Subscribed symbols {self.symbols}"

    def remove_symbols(self, symbols):
        uppercase_symbols = [symbol.upper() for symbol in symbols]
        self.symbols = [s for s in self.symbols if s not in uppercase_symbols]
        if self.fyers:
            self.fyers.unsubscribe(symbols=uppercase_symbols, data_type=self.data_type)
        return f"Unsubscribed from symbols: {symbols} List of Subscribed symbols {self.symbols}"

def main():
    access_token = ""
    client = FyersWebSocketClient(access_token)
    client.connect()

if __name__ == "__main__":
    main()

