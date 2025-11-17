import time
import json
import uuid
import random
from datetime import datetime
from kafka import KafkaProducer
import numpy as np


class StockMarketSimulator:
    """Simulates realistic stock price movements using geometric Brownian motion"""

    def __init__(self):
        self.stocks = {
            "AAPL": {"price": 175.0, "volatility": 0.02, "drift": 0.0001},
            "GOOGL": {"price": 140.0, "volatility": 0.025, "drift": 0.0002},
            "MSFT": {"price": 380.0, "volatility": 0.018, "drift": 0.00015},
            "AMZN": {"price": 145.0, "volatility": 0.03, "drift": 0.0003},
            "TSLA": {"price": 240.0, "volatility": 0.04, "drift": -0.0001},
            "NVDA": {"price": 495.0, "volatility": 0.035, "drift": 0.0004},
            "META": {"price": 350.0, "volatility": 0.028, "drift": 0.00025},
            "JPM": {"price": 155.0, "volatility": 0.015, "drift": 0.0001},
            "V": {"price": 265.0, "volatility": 0.02, "drift": 0.00012},
            "WMT": {"price": 165.0, "volatility": 0.012, "drift": 0.00008},
        }

        self.sectors = {
            "AAPL": "Technology",
            "GOOGL": "Technology",
            "MSFT": "Technology",
            "AMZN": "Consumer",
            "TSLA": "Automotive",
            "NVDA": "Technology",
            "META": "Technology",
            "JPM": "Financial",
            "V": "Financial",
            "WMT": "Retail",
        }

        self.exchanges = ["NYSE", "NASDAQ"]
        self.trade_types = ["BUY", "SELL"]

    def update_price(self, symbol):
        """Update stock price using geometric Brownian motion"""
        stock = self.stocks[symbol]
        dt = 1.0 / (252 * 78)  # Fraction of trading day (assuming 6.5 hour trading day)

        drift = stock["drift"] * dt
        shock = stock["volatility"] * np.random.normal(0, 1) * np.sqrt(dt)

        price_change = stock["price"] * (drift + shock)
        stock["price"] = max(
            stock["price"] + price_change, 1.0
        )  # Ensure positive price

        return round(stock["price"], 2)

    def generate_trade(self):
        """Generate a synthetic stock trade"""
        symbol = random.choice(list(self.stocks.keys()))
        price = self.update_price(symbol)

        # Volume follows log-normal distribution
        volume = int(np.random.lognormal(mean=5, sigma=1.5) * 10)
        volume = max(100, min(volume, 1000000))  # Reasonable bounds

        trade_type = random.choice(self.trade_types)

        # Add slight bid-ask spread
        spread = price * 0.0001
        if trade_type == "BUY":
            price += spread
        else:
            price -= spread

        price = round(price, 2)
        trade_value = round(price * volume, 2)

        # Simulate different exchanges with different fees
        exchange = random.choice(self.exchanges)
        fee_rate = 0.0001 if exchange == "NYSE" else 0.00015
        fee = round(trade_value * fee_rate, 2)

        return {
            "trade_id": str(uuid.uuid4()),
            "timestamp": datetime.now().isoformat(),
            "symbol": symbol,
            "sector": self.sectors[symbol],
            "price": price,
            "volume": volume,
            "trade_value": trade_value,
            "trade_type": trade_type,
            "exchange": exchange,
            "fee": fee,
        }


def run_producer():
    """Kafka producer that sends stock trades"""
    simulator = StockMarketSimulator()

    try:
        print("[Producer] Connecting to Kafka at localhost:9092...")
        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=30000,
            max_block_ms=60000,
            retries=5,
        )
        print("[Producer] ✓ Connected to Kafka successfully!")

        count = 0
        while True:
            trade = simulator.generate_trade()

            print(
                f"[Producer] Trade #{count}: {trade['symbol']} @ ${trade['price']} "
                f"x {trade['volume']} = ${trade['trade_value']:,.2f}"
            )

            future = producer.send("stock_trades", value=trade)
            record_metadata = future.get(timeout=10)

            print(
                f"[Producer] ✓ Sent to partition {record_metadata.partition} "
                f"at offset {record_metadata.offset}"
            )

            producer.flush()
            count += 1

            # Generate trades every 0.5-1.5 seconds for realistic flow
            sleep_time = random.uniform(0.5, 1.5)
            time.sleep(sleep_time)

    except Exception as e:
        print(f"[Producer ERROR] {e}")
        import traceback

        traceback.print_exc()
        raise


if __name__ == "__main__":
    run_producer()
