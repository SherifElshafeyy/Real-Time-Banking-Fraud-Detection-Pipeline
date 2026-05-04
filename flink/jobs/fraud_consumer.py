from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.expressions import col, lit
from pyflink.table.window import Tumble

# ── Environment ───────────────────────────────────────────────
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
env.enable_checkpointing(10000)

t_env = StreamTableEnvironment.create(env)

# ── Kafka Source Table ────────────────────────────────────────
t_env.execute_sql("""
    CREATE TABLE transactions (
        transaction_id   STRING,
        user_id          STRING,
        `timestamp`      STRING,
        transaction_type STRING,
        amount           DOUBLE,
        country          STRING,
        currency         STRING,
        merchant         STRING,
        ip_address       STRING,
        status           STRING,
        event_time       AS TO_TIMESTAMP(LEFT(`timestamp`, 23), 'yyyy-MM-dd''T''HH:mm:ss.SSS'),
        WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
    ) WITH (
        'connector'                     = 'kafka',
        'topic'                         = 'transactions',
        'properties.bootstrap.servers'  = 'kafka:29092',
        'properties.group.id'           = 'flink_consumer',
        'scan.startup.mode'             = 'latest-offset',
        'format'                        = 'json'
    )
""")

table = t_env.from_path("transactions")

# -- Kafka Table where Flink will sink alerts in 
t_env.execute_sql("""
    CREATE TABLE fraud_alerts (
        user_id STRING,
        alert_value DOUBLE,
        window_start TIMESTAMP(3),
        window_end TIMESTAMP(3),
        alert_type STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'fraud_alerts',
        'properties.bootstrap.servers' = 'kafka:29092',
        'format' = 'json',
        'json.timestamp-format.standard' = 'ISO-8601'
    )
""")

# ============================================================
# 🚨 1. AMOUNT FRAUD
# ============================================================
amount_fraud = (
    table
    .window(Tumble.over(lit(30).seconds).on(col("event_time")).alias("w"))
    .group_by(col("user_id"), col("w"))
    .select(
        col("user_id"),
        col("amount").sum.alias("alert_value"),
        col("w").start.alias("window_start"),
        col("w").end.alias("window_end"),
        lit("HIGH_AMOUNT").alias("alert_type")
    )
    .filter(col("alert_value") > 10000)
)

# ============================================================
# 🚨 2. COUNT FRAUD
# ============================================================
count_fraud = (
    table
    .window(Tumble.over(lit(30).seconds).on(col("event_time")).alias("w"))
    .group_by(col("user_id"), col("w"))
    .select(
        col("user_id"),
        col("user_id").count.cast(DataTypes.DOUBLE()).alias("alert_value"),
        col("w").start.alias("window_start"),
        col("w").end.alias("window_end"),
        lit("HIGH_FREQUENCY").alias("alert_type")
    )
    .filter(col("alert_value") > 10.0)
)

# ============================================================
# 🚨 3. LOCATION FRAUD — SQL used because count_distinct
#    is not supported in fluent Table API for STRING columns
# ============================================================
t_env.execute_sql("""
    CREATE VIEW location_fraud AS
    SELECT
        user_id,
        CAST(COUNT(DISTINCT country) AS DOUBLE)  AS alert_value,
        window_start              AS window_start,
        window_end              AS window_end,
        'MULTI_COUNTRY'                          AS alert_type
    FROM TABLE(
        TUMBLE(TABLE transactions, DESCRIPTOR(event_time), INTERVAL '30' SECONDS)
    )
    GROUP BY user_id, window_start, window_end
    HAVING COUNT(DISTINCT country) > 1.0
""")

location_fraud = t_env.from_path("location_fraud")

# ============================================================
# 🔥 Combine and print
# ============================================================
all_alerts=amount_fraud.union_all(count_fraud).union_all(location_fraud)

all_alerts.execute_insert("fraud_alerts").wait()
