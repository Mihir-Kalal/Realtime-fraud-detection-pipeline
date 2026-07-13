"""
PyFlink Streaming Architecture (Architecture Demo)

This file demonstrates how the Python-based `feature_engine` consumer can be 
upgraded to Apache Flink for true distributed, stateful stream processing.

Key Flink Concepts demonstrated:
1. Event-Time Processing: Using the transaction's actual timestamp, not ingestion time.
2. Watermarking: Handling late or out-of-order events gracefully.
3. Stateful Windowing: Using TumblingEventTimeWindows to compute 1h and 24h velocity.
"""

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.time import Time
from pyflink.common.watermark_strategy import Duration
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.datastream.window import TumblingEventTimeWindows

def create_feature_pipeline():
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Enable exactly-once checkpointing
    env.enable_checkpointing(60000)
    
    # 1. Kafka Source Definition
    deserialization_schema = JsonRowDeserializationSchema.builder() \
        .type_info(Types.ROW_NAMED(
            ["txn_id", "user_id", "amount", "merchant_id", "timestamp"],
            [Types.STRING(), Types.STRING(), Types.FLOAT(), Types.STRING(), Types.STRING()]
        )).build()
        
    kafka_consumer = FlinkKafkaConsumer(
        topics='transactions-raw',
        deserialization_schema=deserialization_schema,
        properties={'bootstrap.servers': 'kafka:29092', 'group.id': 'flink-feature-engine'}
    )
    
    # 2. Ingest Stream
    stream = env.add_source(kafka_consumer)
    
    # 3. Watermarking (Handle out-of-order events up to 30 seconds late)
    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(30)) \
        .with_timestamp_assigner(lambda event, _: int(event.timestamp)) # Simplified parsing
        
    stream_with_time = stream.assign_timestamps_and_watermarks(watermark_strategy)
    
    # 4. Keyed Stream by user_id
    keyed_stream = stream_with_time.key_by(lambda event: event.user_id)
    
    # 5. Windowing (e.g., 24-hour tumbling window)
    windowed_stream = keyed_stream.window(TumblingEventTimeWindows.of(Time.hours(24)))
    
    # 6. Apply Aggregation (Velocity, Mean Amount, etc.)
    # In a real Flink job, this uses an AggregateFunction to maintain running totals
    # aggregated_stream = windowed_stream.aggregate(UserFeatureAggregator())
    
    # 7. Sink to Redis (Feature Store) and Postgres (Offline Store)
    # aggregated_stream.add_sink(RedisSink(...))
    # aggregated_stream.add_sink(PostgresSink(...))

    env.execute("Fraud Detection Feature Engine")

if __name__ == '__main__':
    # This is a demonstration script. Running it requires a Flink cluster setup.
    print("PyFlink Feature Engine pipeline defined.")
