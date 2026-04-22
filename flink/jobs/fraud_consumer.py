from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy, Duration, Types
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream.window import TumblingEventTimeWindows, Time
import json
from datetime import datetime


# ── Timestamp Assigner (still required) ──
class TransactionTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        record = json.loads(value)
        return int(datetime.fromisoformat(record["timestamp"]).timestamp() * 1000)


# ── Environment ──
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)


# ── Kafka Source ──
source = KafkaSource.builder() \
    .set_bootstrap_servers("kafka:29092") \
    .set_topics("transactions") \
    .set_group_id("flink_consumer") \
    .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .build()


# ── Step 1: Raw stream ──
stream = env.from_source(
    source,
    WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_timestamp_assigner(TransactionTimestampAssigner()),
    "Kafka Source"
)


# ── Step 2: Parse JSON ──
parsed_stream = stream.map(
    lambda x: json.loads(x)
)


#AMOUNT FRAUD DETECTION
amount_stream = parsed_stream.map(
    lambda x: (x["user_id"], float(x["amount"])),
    output_type=Types.TUPLE([Types.STRING(), Types.DOUBLE()])
)


keyed_amount_stream = amount_stream.key_by(lambda x: x[0])



wind_amount_stream = keyed_amount_stream.window(
    TumblingEventTimeWindows.of(Time.seconds(30))
).reduce(
    lambda a, b: (a[0], round(a[1] + b[1],2))
)


fraud_amount_alerts = wind_amount_stream \
    .filter(lambda x: x[1] > 10000) \
    .map(
        lambda x: {
            'user_id': x[0],
            'value': str(x[1]),
            'alert_type': 'HIGH_AMOUNT'
        },
        output_type=Types.MAP(Types.STRING(), Types.STRING())
    )


#TRANSACTION COUNT FRAUD DETECTION


count_stream = parsed_stream.map(
    lambda x: (x["user_id"], 1),
    output_type=Types.TUPLE([Types.STRING(), Types.INT()])
)

keyed_count_stream=count_stream.key_by(lambda x :x[0])

wind_count_stream = keyed_count_stream.window(
    TumblingEventTimeWindows.of(Time.seconds(30))
).reduce(
    lambda a, b: (a[0], round(a[1] + b[1],2))
)

fraud_count_alerts = wind_count_stream \
    .filter(lambda x: x[1] > 10) \
    .map(
        lambda x: {
            'user_id': x[0],
            'value': str(x[1]),
            'alert_type': 'HIGH_FREQUENCY'
        },
        output_type=Types.MAP(Types.STRING(), Types.STRING())
    )

all_alerts = fraud_amount_alerts.union(fraud_count_alerts)

all_alerts.print()

env.execute("Step-by-Step Fraud Detection")