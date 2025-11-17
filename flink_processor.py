"""
Apache Flink Real-time Stream Processor
Performs windowed aggregations and sends results to Kafka
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
    KafkaSink,
    KafkaRecordSerializationSchema,
)
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction, ReduceFunction
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common.time import Time
import json


class TradeParser(MapFunction):
    """Parse JSON trade messages"""

    def map(self, value):
        trade = json.loads(value)
        return (
            trade["symbol"],
            trade["sector"],
            float(trade["price"]),
            int(trade["volume"]),
            float(trade["trade_value"]),
            trade["trade_type"],
        )


class TradeAggregator(ReduceFunction):
    """Aggregate trades within time window"""

    def reduce(self, trade1, trade2):
        symbol = trade1[0]
        sector = trade1[1]
        total_volume = trade1[3] + trade2[3]
        total_value = trade1[4] + trade2[4]

        # Calculate VWAP (Volume Weighted Average Price)
        vwap = total_value / total_volume if total_volume > 0 else 0

        return (symbol, sector, vwap, total_volume, total_value, trade1[5])


def create_flink_job():
    """Create Flink streaming job for real-time aggregations"""

    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)

    # Configure Kafka source
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers("localhost:9092")
        .set_topics("stock_trades")
        .set_group_id("flink-aggregator")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # Read from Kafka
    trade_stream = env.from_source(
        kafka_source, watermark_strategy=None, source_name="Stock Trades Source"
    )

    # Parse trades
    parsed_stream = trade_stream.map(
        TradeParser(),
        output_type=Types.TUPLE(
            [
                Types.STRING(),  # symbol
                Types.STRING(),  # sector
                Types.FLOAT(),  # price
                Types.INT(),  # volume
                Types.FLOAT(),  # trade_value
                Types.STRING(),  # trade_type
            ]
        ),
    )

    # 1-minute tumbling window aggregations by symbol
    windowed_aggregations = (
        parsed_stream.key_by(lambda x: x[0])
        .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
        .reduce(TradeAggregator())
    )

    # Format aggregated results
    def format_aggregation(trade_tuple):
        symbol, sector, vwap, volume, value, _ = trade_tuple
        result = {
            "window_type": "1min_aggregation",
            "symbol": symbol,
            "sector": sector,
            "vwap": round(vwap, 2),
            "total_volume": volume,
            "total_value": round(value, 2),
            "timestamp": str(Time.now()),
        }
        return json.dumps(result)

    formatted_stream = windowed_aggregations.map(
        format_aggregation, output_type=Types.STRING()
    )

    # Configure Kafka sink for aggregated results
    kafka_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers("localhost:9092")
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("trade_aggregations")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    # Send aggregated results to Kafka
    formatted_stream.sink_to(kafka_sink)

    # Print to console for monitoring
    formatted_stream.print()

    # Execute the Flink job
    env.execute("Stock Trade Aggregator")


if __name__ == "__main__":
    print("[Flink] Starting real-time aggregation job...")
    print("[Flink] Will compute 1-minute windowed VWAP and volumes per symbol")
    create_flink_job()
