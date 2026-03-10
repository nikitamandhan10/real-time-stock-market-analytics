"""
Anomaly detection with MinIO persistence
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.udf import udf
from pyflink.table import DataTypes
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@udf(result_type=DataTypes.BOOLEAN())
def detect_price_spike(current_price, avg_price, threshold=0.05):
    """Detect if price spike exceeds threshold (5%)"""
    if avg_price is None or avg_price == 0:
        return False
    
    change_pct = abs(current_price - avg_price) / avg_price
    return change_pct > threshold

@udf(result_type=DataTypes.BOOLEAN())
def detect_volume_surge(current_volume, avg_volume, threshold=2.0):
    """Detect if volume surge exceeds threshold (2x average)"""
    if avg_volume is None or avg_volume == 0:
        return False
    
    return current_volume > (avg_volume * threshold)


def create_anomaly_detector():
    """Create Flink job for anomaly detection with MinIO sink"""
    
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)
    env.enable_checkpointing(60000)
    
    settings = EnvironmentSettings.new_instance() \
        .in_streaming_mode() \
        .build()
    
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    
    # Configure S3/MinIO
    t_env.get_config().get_configuration().set_string(
        "s3.endpoint", "http://minio:9000"
    )
    t_env.get_config().get_configuration().set_string(
        "s3.access-key", "minioadmin"
    )
    t_env.get_config().get_configuration().set_string(
        "s3.secret-key", "minioadmin"
    )
    t_env.get_config().get_configuration().set_string(
        "s3.path.style.access", "true"
    )
    
    # Register UDFs
    t_env.create_temporary_function("detect_price_spike", detect_price_spike)
    t_env.create_temporary_function("detect_volume_surge", detect_volume_surge)
    
    # Source
    t_env.execute_sql("""
        CREATE TABLE market_quotes (
            ticker STRING,
            ts TIMESTAMP(3),
            close DOUBLE,
            volume BIGINT,
            WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'market.quotes',
            'properties.bootstrap.servers' = 'kafka:29092',
            'properties.group.id' = 'flink-anomaly-consumer',
            'format' = 'json',
            'json.timestamp-format.standard' = 'ISO-8601',
            'scan.startup.mode' = 'latest-offset'
        )
    """)
    
    # Sink 1: Kafka (for alerts)
    t_env.execute_sql("""
        CREATE TABLE market_anomalies_kafka (
            ticker STRING,
            ts TIMESTAMP(3),
            anomaly_type STRING,
            current_value DOUBLE,
            avg_value DOUBLE,
            severity STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'market.anomalies',
            'properties.bootstrap.servers' = 'kafka:29092',
            'format' = 'json'
        )
    """)
    
    # Sink 2: MinIO (for audit trail)
    t_env.execute_sql("""
        CREATE TABLE market_anomalies_minio (
            ticker STRING,
            ts TIMESTAMP(3),
            anomaly_type STRING,
            current_value DOUBLE,
            avg_value DOUBLE,
            severity STRING,
            dt STRING,
            hr STRING
        ) PARTITIONED BY (dt, hr) WITH (
            'connector' = 'filesystem',
            'path' = 's3a://bronze/streaming/anomalies',
            'format' = 'parquet',
            'sink.partition-commit.delay' = '1 min',
            'sink.partition-commit.policy.kind' = 'success-file'
        )
    """)
    
    # Create view with anomaly detection logic
    t_env.execute_sql("""
        CREATE VIEW detected_anomalies AS
        SELECT
            ticker,
            ts,
            CASE
                WHEN detect_price_spike(close, avg_close) THEN 'PRICE_SPIKE'
                WHEN detect_volume_surge(volume, avg_volume) THEN 'VOLUME_SURGE'
            END as anomaly_type,
            CASE
                WHEN detect_price_spike(close, avg_close) THEN close
                ELSE CAST(volume AS DOUBLE)
            END as current_value,
            CASE
                WHEN detect_price_spike(close, avg_close) THEN avg_close
                ELSE avg_volume
            END as avg_value,
            'HIGH' as severity,
            DATE_FORMAT(ts, 'yyyy-MM-dd') as dt,
            DATE_FORMAT(ts, 'HH') as hr
        FROM (
            SELECT
                ticker,
                ts,
                close,
                volume,
                AVG(close) OVER (
                    PARTITION BY ticker
                    ORDER BY ts
                    RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
                ) as avg_close,
                AVG(CAST(volume AS DOUBLE)) OVER (
                    PARTITION BY ticker
                    ORDER BY ts
                    RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
                ) as avg_volume
            FROM market_quotes
        )
        WHERE
            detect_price_spike(close, avg_close) OR
            detect_volume_surge(volume, avg_volume)
    """)
    
    # Insert into Kafka
    t_env.execute_sql("""
        INSERT INTO market_anomalies_kafka
        SELECT
            ticker,
            ts,
            anomaly_type,
            current_value,
            avg_value,
            severity
        FROM detected_anomalies
    """)
    
    # Insert into MinIO
    t_env.execute_sql("""
        INSERT INTO market_anomalies_minio
        SELECT *
        FROM detected_anomalies
    """)
    
    logger.info("Anomaly Detector job submitted with dual sinks (Kafka + MinIO)")


if __name__ == "__main__":
    create_anomaly_detector()
