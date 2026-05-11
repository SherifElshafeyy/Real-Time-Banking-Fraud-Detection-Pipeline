from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.expressions import col, lit
from pyflink.table.window import Tumble

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
env.enable_checkpointing(60000)  

t_env = StreamTableEnvironment.create(env)

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
        WATERMARK FOR event_time AS event_time - INTERVAL '20' SECOND
    ) WITH (
        'connector'                     = 'kafka',
        'topic'                         = 'transactions',
        'properties.bootstrap.servers'  = 'kafka:29092',
        'properties.group.id'           = 'flink_consumer',
        'scan.startup.mode'             = 'latest-offset',
        'format'                        = 'json'
    )
""")

t_env.execute_sql("""
    CREATE TABLE fraud_alerts (
        user_id      STRING,
        alert_value  DOUBLE,
        window_start TIMESTAMP(3),
        window_end   TIMESTAMP(3),
        alert_type   STRING
    ) WITH (
        'connector'                      = 'kafka',
        'topic'                          = 'fraud_alerts',
        'properties.bootstrap.servers'   = 'kafka:29092',
        'format'                         = 'json',
        'json.timestamp-format.standard' = 'ISO-8601'
    )
""")

table = t_env.from_path("transactions")

# 1. AMOUNT FRAUD 
amount_fraud = (
    table
    .window(Tumble.over(lit(120).seconds).on(col("event_time")).alias("w"))
    .group_by(col("user_id"), col("w"))
    .select(
        col("user_id"),
        col("amount").sum.alias("alert_value"),
        col("w").start.alias("window_start"),
        col("w").end.alias("window_end"),
        lit("HIGH_AMOUNT").alias("alert_type")
    )
    .filter(col("alert_value") > 20000)
)

# 2. FREQUENCY FRAUD 
frequency_fraud = (
    table
    .window(Tumble.over(lit(120).seconds).on(col("event_time")).alias("w"))
    .group_by(col("user_id"), col("w"))
    .select(
        col("user_id"),
        col("user_id").count.cast(DataTypes.DOUBLE()).alias("alert_value"),
        col("w").start.alias("window_start"),
        col("w").end.alias("window_end"),
        lit("HIGH_FREQUENCY").alias("alert_type")
    )
    .filter(col("alert_value") >= 3)
)

# 3. FAILED FRAUD
failed_fraud = (
    table
    .filter(col("status") == "FAILED")
    .window(Tumble.over(lit(120).seconds).on(col("event_time")).alias("w"))
    .group_by(col("user_id"), col("w"))
    .select(
        col("user_id"),
        col("user_id").count.cast(DataTypes.DOUBLE()).alias("alert_value"),
        col("w").start.alias("window_start"),
        col("w").end.alias("window_end"),
        lit("FAILED_TRANSACTIONS").alias("alert_type")
    )
    .filter(col("alert_value") >= 2)
)

# 4. LOCATION FRAUD 
t_env.execute_sql("""
    CREATE VIEW location_fraud AS
    SELECT
        user_id,
        CAST(COUNT(DISTINCT country) AS DOUBLE) AS alert_value,
        window_start                            AS window_start,
        window_end                              AS window_end,
        'MULTI_COUNTRY'                         AS alert_type
    FROM TABLE(
        TUMBLE(TABLE transactions, DESCRIPTOR(event_time), INTERVAL '2' MINUTES)
    )
    GROUP BY user_id, window_start, window_end
    HAVING COUNT(DISTINCT country) > 1.0
""")

location_fraud = t_env.from_path("location_fraud")

# Combine and sink 
all_alerts = (
    amount_fraud
    .union_all(frequency_fraud)
    .union_all(location_fraud)
    .union_all(failed_fraud)
)

all_alerts.execute_insert("fraud_alerts").wait()