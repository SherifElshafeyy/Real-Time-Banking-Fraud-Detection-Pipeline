from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(env)

t_env.execute_sql("""
    CREATE TABLE transactions (
        user_id     STRING,
        amount      DOUBLE,
        `timestamp` STRING,
        country     STRING,
        event_time  AS TO_TIMESTAMP(LEFT(`timestamp`, 23), 'yyyy-MM-dd''T''HH:mm:ss.SSS'),
        WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
    ) WITH (
        'connector'                    = 'kafka',
        'topic'                        = 'transactions',
        'properties.bootstrap.servers' = 'kafka:29092',
        'properties.group.id'          = 'flink_consumer',
        'scan.startup.mode'            = 'latest-offset',
        'format'                       = 'json'
    )
""")

t_env.execute_sql("SELECT * FROM transactions").print()