import os
import csv
import signal
import asyncio
import threading
import time
from fyers_apiv3.FyersWebsocket import data_ws


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

import os
import csv
import asyncio
import threading
import time
from fyers_apiv3.FyersWebsocket import data_ws

class AsyncDataRecorder:
    def __init__(self, filename):
        self.filename = filename
        self.lock = threading.Lock()
        self.queue = asyncio.Queue()
        self.loop = asyncio.get_event_loop()
        self.processing_task = self.loop.create_task(self.process_queue())
        self.ensure_file_exists()

    def ensure_file_exists(self):
        path = os.getcwd()
        full_path = os.path.join(path, self.filename)
        if not os.path.exists(full_path):
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
        await self.queue.put(data)
        print(f"Data queued for {data.get('symbol')}")

    async def process_queue(self):
        while True:
            data = await self.queue.get()
            if data is None:
                break
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
                print(f"Data saved for {data.get('symbol')} to {self.filename}")
            except IOError as e:
                print(f"Error saving data for {data.get('symbol')}: {e}")
            finally:
                self.queue.task_done()

    def stop_processing(self):
        self.loop.call_soon_threadsafe(self.queue.put_nowait, None)
        self.loop.run_until_complete(self.processing_task)

class FyersWebSocketClient:
    def __init__(self, access_token):
        self.access_token = access_token
        self.symbols = ['NSE:SPIC-EQ', 'NSE:YESBANK-EQ']
        self.data_type = "SymbolUpdate"
        self.data_manager = AsyncDataRecorderManager()
        self.is_shutting_down = False
        self.loop = asyncio.new_event_loop()
        threading.Thread(target=self.start_event_loop, daemon=True).start()

    def start_event_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    async def onmessage(self, message):
        symbol = message.get("symbol")
        if symbol and not self.is_shutting_down:
            print(f"Received message for {symbol}: {message}")
            recorder = self.data_manager.get_recorder(symbol)
            await recorder.save_to_file(message)

    def onmessage_sync(self, message):
        asyncio.run_coroutine_threadsafe(self.onmessage(message), self.loop)

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
        return f"Unsubscribed from symbols: {symbols}, List of Subscribed symbols {self.symbols}"

    def shutdown(self):
        self.is_shutting_down = True
        print("Shutting down gracefully...")
        for recorder in self.data_manager.data_recorders.values():
            recorder.stop_processing()
        if self.fyers:
            self.fyers.unsubscribe(symbols=self.symbols, data_type=self.data_type)
        if self.fyers:
            self.fyers.keep_running = False
        self.loop.call_soon_threadsafe(self.loop.stop)

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
