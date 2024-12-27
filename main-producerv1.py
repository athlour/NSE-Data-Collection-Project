import os
import csv
import signal
import asyncio
import threading
import time
from fyers_apiv3.FyersWebsocket import data_ws

class AsyncDataRecorder:
    def __init__(self, filename):
        self.filename = filename
        self.lock = threading.Lock()
        self.ensure_file_exists()  # Ensure the file is created with headers

    def ensure_file_exists(self):
        """Ensure that the file exists and has headers."""
        path = os.getcwd()  # Get the current working directory
        full_path = os.path.join(path, self.filename)
        if not os.path.exists(full_path):  # Only create file if it doesn't exist
            with open(full_path, mode='w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(["symbol", "ltp", "vol_traded_today", "last_traded_time", "exch_feed_time",
                                 "bid_size", "ask_size", "bid_price", "ask_price", "last_traded_qty",
                                 "tot_buy_qty", "tot_sell_qty", "avg_trade_price", "low_price",
                                 "high_price", "lower_ckt", "upper_ckt", "open_price", "prev_close_price",
                                 "ch", "chp"])
            print(f"Created file with headers: {full_path}")
        else:
            print(f"File already exists: {full_path}")

    async def save_to_file(self, data):
        """Save data asynchronously to the file."""
        try:
            with self.lock:
                with open(self.filename, 'a', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow([
                        data.get("symbol"), data.get("ltp"), data.get("vol_traded_today"),
                        data.get("last_traded_time"), data.get("exch_feed_time"),
                        data.get("bid_size"), data.get("ask_size"), data.get("bid_price"),
                        data.get("ask_price"), data.get("last_traded_qty"),
                        data.get("tot_buy_qty"), data.get("tot_sell_qty"), data.get("avg_trade_price"),
                        data.get("low_price"), data.get("high_price"), data.get("lower_ckt"),
                        data.get("upper_ckt"), data.get("open_price"), data.get("prev_close_price"),
                        data.get("ch"), data.get("chp")
                    ])
                print(f"Data saved for {data['symbol']} to {self.filename}")
        except IOError as e:
            print(f"Error saving data for {data.get('symbol')}: {e}")


class AsyncDataRecorderManager:
    def __init__(self):
        self.data_recorders = {}
        self.lock = threading.Lock()

    def get_recorder(self, symbol):
        """Get or create a recorder for the given symbol."""
        with self.lock:
            if symbol not in self.data_recorders:
                filename = f"{symbol[4:]}_data_{time.strftime('%Y-%m-%d')}.csv"
                self.data_recorders[symbol] = AsyncDataRecorder(filename)
            return self.data_recorders[symbol]


class FyersWebSocketClient:
    def __init__(self, access_token):
        self.access_token = access_token
        self.fyers = None
        self.symbols = ['NSE:SPIC-EQ', 'NSE:YESBANK-EQ']
        self.data_type = "SymbolUpdate"
        self.data_manager = AsyncDataRecorderManager()
        self.is_shutting_down = False

    async def onmessage(self, message):
        symbol = message.get("symbol")
        if symbol and not self.is_shutting_down:
            recorder = self.data_manager.get_recorder(symbol)
            await recorder.save_to_file(message)

    def onmessage_sync(self, message):
        """Synchronously handle WebSocket messages."""
        if not self.is_shutting_down:
            asyncio.run(self.onmessage(message))

    def onerror(self, message):
        """Handle WebSocket errors."""
        print("Fyers WebSocket Error:", message)

    def onclose(self, message):
        """Handle WebSocket connection closure."""
        print("Connection closed:", message)

    def onopen(self):
        """Handle WebSocket connection open."""
        self.subscribe_initial_symbols()

    def subscribe_initial_symbols(self):
        """Subscribe to the initial set of symbols."""
        uppercase_symbols = [symbol.upper() for symbol in self.symbols]
        self.fyers.subscribe(symbols=uppercase_symbols, data_type=self.data_type)
        self.fyers.keep_running()

    def connect(self):
        """Establish WebSocket connection."""
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
        """Add new symbols to subscribe to."""
        uppercase_symbols = [symbol.upper() for symbol in symbols]
        self.symbols.extend(uppercase_symbols)
        if self.fyers:
            self.fyers.subscribe(symbols=uppercase_symbols, data_type=self.data_type)
        return f"Subscribed to new symbols: {symbols}, List of Subscribed symbols {self.symbols}"

    def remove_symbols(self, symbols):
        """Remove symbols from subscription."""
        uppercase_symbols = [symbol.upper() for symbol in symbols]
        self.symbols = [s for s in self.symbols if s not in uppercase_symbols]
        if self.fyers:
            self.fyers.unsubscribe(symbols=uppercase_symbols, data_type=self.data_type)
        return f"Unsubscribed from symbols: {symbols}, List of Subscribed symbols {self.symbols}"

    def shutdown(self):
        """Gracefully shut down the WebSocket connection."""
        self.is_shutting_down = True
        print("Shutting down gracefully...")
        # Unsubscribe from all symbols
        if self.fyers:
            self.fyers.unsubscribe(symbols=self.symbols, data_type=self.data_type)
        # Close the WebSocket connection
        if self.fyers:
            self.fyers.keep_running = False

def main():
    access_token = ""
    client = FyersWebSocketClient(access_token)

    # Register signal handlers for graceful shutdown
    def signal_handler(sig, frame):
        print(f"Signal {sig} received. Shutting down...")
        client.shutdown()

    signal.signal(signal.SIGINT, signal_handler)  # Handle Ctrl+C (SIGINT)
    signal.signal(signal.SIGTERM, signal_handler)  # Handle termination signal

    client.connect()

if __name__ == "__main__":
    main()
