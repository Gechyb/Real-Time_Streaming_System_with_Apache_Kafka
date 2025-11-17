"""
Simplified Real-time Aggregator (Alternative to Flink)
This provides similar windowed aggregation functionality without PyFlink complexity.
"""

import json
import time
from datetime import datetime
from collections import defaultdict
from kafka import KafkaConsumer, KafkaProducer


class WindowedAggregator:
    """Performs 1-minute tumbling window aggregations"""

    def __init__(self, window_seconds=60):
        self.window_seconds = window_seconds
        self.current_window = defaultdict(
            lambda: {"prices": [], "volumes": [], "values": [], "count": 0}
        )
        self.last_flush = time.time()

    def add_trade(self, trade):
        """Add trade to current window"""
        symbol = trade["symbol"]
        window = self.current_window[symbol]

        window["prices"].append(trade["price"])
        window["volumes"].append(trade["volume"])
        window["values"].append(trade["trade_value"])
        window["count"] += 1

    def should_flush(self):
        """Check if window should be flushed"""
        return (time.time() - self.last_flush) >= self.window_seconds

    def flush_window(self):
        """Calculate aggregations and return results"""
        results = []

        for symbol, data in self.current_window.items():
            if data["count"] > 0:
                # Calculate VWAP (Volume Weighted Average Price)
                total_value = sum(data["values"])
                total_volume = sum(data["volumes"])
                vwap = total_value / total_volume if total_volume > 0 else 0

                result = {
                    "window_type": "1min_aggregation",
                    "symbol": symbol,
                    "vwap": round(vwap, 2),
                    "total_volume": total_volume,
                    "total_value": round(total_value, 2),
                    "trade_count": data["count"],
                    "min_price": round(min(data["prices"]), 2),
                    "max_price": round(max(data["prices"]), 2),
                    "avg_price": round(sum(data["prices"]) / len(data["prices"]), 2),
                    "timestamp": datetime.now().isoformat(),
                }
                results.append(result)

        # Clear window
        self.current_window.clear()
        self.last_flush = time.time()

        return results


def run_aggregator():
    """Main aggregation loop"""
    print("[Aggregator] Starting real-time aggregation service...")
    print("[Aggregator] Window size: 1 minute")
    print("[Aggregator] Computing VWAP, volumes, and price statistics")

    try:
        # Setup Kafka consumer
        print("[Aggregator] Connecting to Kafka consumer...")
        consumer = KafkaConsumer(
            "stock_trades",
            bootstrap_servers="localhost:9092",
            auto_offset_reset="latest",  # Only process new messages
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id="aggregator-group",
            consumer_timeout_ms=1000,  # Check for flush every second
        )
        print("[Aggregator] ✓ Consumer connected!")

        # Setup Kafka producer for aggregated results
        print("[Aggregator] Connecting to Kafka producer...")
        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        print("[Aggregator] ✓ Producer connected!")

        aggregator = WindowedAggregator(window_seconds=60)

        print("[Aggregator] 🎧 Listening for trades and computing aggregations...")
        print("[Aggregator] Aggregations will be published every 60 seconds\n")

        window_count = 0

        while True:
            # Consume messages
            messages = consumer.poll(timeout_ms=1000)

            for topic_partition, records in messages.items():
                for record in records:
                    trade = record.value
                    aggregator.add_trade(trade)

            # Check if window should be flushed
            if aggregator.should_flush():
                results = aggregator.flush_window()

                if results:
                    window_count += 1
                    print(f"\n[Aggregator] 📊 Window #{window_count} Complete!")
                    print(
                        f"[Aggregator] Computed aggregations for {len(results)} symbols"
                    )

                    # Publish each aggregation
                    for result in results:
                        producer.send("trade_aggregations", value=result)
                        print(
                            f"[Aggregator]   {result['symbol']}: "
                            f"VWAP=${result['vwap']:.2f}, "
                            f"Vol={result['total_volume']:,}, "
                            f"Trades={result['trade_count']}, "
                            f"Range=${result['min_price']:.2f}-${result['max_price']:.2f}"
                        )

                    producer.flush()
                    print(
                        f"[Aggregator] ✓ Published {len(results)} aggregations to Kafka\n"
                    )

    except KeyboardInterrupt:
        print("\n[Aggregator] Shutting down gracefully...")
    except Exception as e:
        print(f"[Aggregator ERROR] {e}")
        import traceback

        traceback.print_exc()
        raise


if __name__ == "__main__":
    run_aggregator()
