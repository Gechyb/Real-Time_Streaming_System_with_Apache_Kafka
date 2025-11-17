import json
import psycopg2
from kafka import KafkaConsumer
from collections import deque
import numpy as np


class AnomalyDetector:
    """Simple statistical anomaly detection for trade patterns"""

    def __init__(self, window_size=100, threshold=3.0):
        self.window_size = window_size
        self.threshold = threshold
        self.price_windows = {}  # symbol -> deque of recent prices

    def detect_anomaly(self, symbol, price):
        """Detect if price is anomalous using z-score method"""
        if symbol not in self.price_windows:
            self.price_windows[symbol] = deque(maxlen=self.window_size)

        window = self.price_windows[symbol]
        window.append(price)

        if len(window) < 30:  # Need minimum data points
            return False, 0.0

        mean = np.mean(window)
        std = np.std(window)

        if std == 0:
            return False, 0.0

        z_score = abs((price - mean) / std)
        is_anomaly = z_score > self.threshold

        return is_anomaly, round(z_score, 2)


def run_consumer():
    """Consumes trades from Kafka and stores in PostgreSQL with anomaly detection"""

    # Initialize anomaly detector
    detector = AnomalyDetector()

    try:
        print("[Consumer] Connecting to Kafka at localhost:9092...")
        consumer = KafkaConsumer(
            "stock_trades",
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id="stock-consumer-group",
        )
        print("[Consumer] ✓ Connected to Kafka successfully!")

        print("[Consumer] Connecting to PostgreSQL...")
        conn = psycopg2.connect(
            dbname="stock_db",
            user="stock_user",
            password="stock_password",
            host="localhost",
            port="5432",
        )
        conn.autocommit = True
        cur = conn.cursor()
        print("[Consumer] ✓ Connected to PostgreSQL successfully!")

        # Create main trades table
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS trades (
                trade_id VARCHAR(50) PRIMARY KEY,
                timestamp TIMESTAMP,
                symbol VARCHAR(10),
                sector VARCHAR(50),
                price NUMERIC(10, 2),
                volume INTEGER,
                trade_value NUMERIC(15, 2),
                trade_type VARCHAR(10),
                exchange VARCHAR(20),
                fee NUMERIC(10, 2),
                is_anomaly BOOLEAN DEFAULT FALSE,
                anomaly_score NUMERIC(5, 2) DEFAULT 0.0
            );
        """
        )

        # Create aggregations table (for Flink results)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS trade_aggregations (
                id SERIAL PRIMARY KEY,
                window_type VARCHAR(50),
                symbol VARCHAR(10),
                sector VARCHAR(50),
                vwap NUMERIC(10, 2),
                total_volume BIGINT,
                total_value NUMERIC(15, 2),
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
        )

        print("[Consumer] ✓ Tables ready.")
        print("[Consumer] 🎧 Listening for messages...\n")

        message_count = 0
        anomaly_count = 0

        for message in consumer:
            try:
                trade_data = message.value

                # Detect anomalies
                is_anomaly, anomaly_score = detector.detect_anomaly(
                    trade_data["symbol"], trade_data["price"]
                )

                if is_anomaly:
                    anomaly_count += 1
                    print(
                        f"[Consumer] 🚨 ANOMALY DETECTED! {trade_data['symbol']} "
                        f"@ ${trade_data['price']} (z-score: {anomaly_score})"
                    )

                # Insert trade
                insert_query = """
                    INSERT INTO trades (
                        trade_id, timestamp, symbol, sector, price, volume,
                        trade_value, trade_type, exchange, fee,
                                                                      is_anomaly, anomaly_score
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (trade_id) DO NOTHING;
                """

                cur.execute(
                    insert_query,
                    (
                        trade_data["trade_id"],
                        trade_data["timestamp"],
                        trade_data["symbol"],
                        trade_data["sector"],
                        trade_data["price"],
                        trade_data["volume"],
                        trade_data["trade_value"],
                        trade_data["trade_type"],
                        trade_data["exchange"],
                        trade_data["fee"],
                        is_anomaly,
                        anomaly_score,
                    ),
                )

                message_count += 1

                if message_count % 10 == 0:
                    print(
                        f"[Consumer] ✓ Processed {message_count} trades "
                        f"({anomaly_count} anomalies detected)"
                    )

            except Exception as e:
                print(f"[Consumer ERROR] Failed to process message: {e}")
                continue

    except Exception as e:
        print(f"[Consumer ERROR] {e}")
        import traceback

        traceback.print_exc()
        raise


if __name__ == "__main__":
    run_consumer()
