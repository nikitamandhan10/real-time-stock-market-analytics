"""
Apache Flink job to aggregate tick data into OHLCV candles
FIXED VERSION - Non-blocking with proper error handling
"""

from pyflink.table import TableEnvironment, EnvironmentSettings
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)


def create_ohlcv_aggregator():
    """Create and submit Flink job for OHLCV aggregation"""
    
    try:
        logger.info("="*60)
        logger.info("Starting OHLCV Aggregator Job")
        logger.info("="*60)
        
        # Create table environment for remote execution
        logger.info("Creating Flink Table Environment...")
        settings = EnvironmentSettings.new_instance() \
            .in_streaming_mode() \
            .build()
        t_env = TableEnvironment.create(settings)
        
        # Set configuration for remote cluster
        config = t_env.get_config()
        config.set("pipeline.jars", "file:///opt/flink/lib/flink-sql-connector-kafka-3.0.2-1.18.jar")
        config.set("execution.target", "remote")
        config.set("execution.remote.host", "flink-jobmanager")
        config.set("execution.remote.port", "8081")
        
        # Set parallelism
        t_env.get_config().set("parallelism.default", "2")
        
        # Set checkpoint interval (required for streaming)
        t_env.get_config().set(
            "execution.checkpointing.interval",
            "60000"  # 60 seconds
        )
        
        logger.info("✓ Flink environment initialized")
        
        # ---------------------------------------------------
        # Create Kafka SOURCE table
        # ---------------------------------------------------
        logger.info("Creating Kafka source table...")
        
        t_env.execute_sql("""
            CREATE TABLE market_quotes (
                ticker STRING,
                `timestamp` STRING,
                `open` DOUBLE,
                high DOUBLE,
                low DOUBLE,
                `close` DOUBLE,
                volume BIGINT,
                ts AS TO_TIMESTAMP(`timestamp`),
                WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'market.quotes',
                'properties.bootstrap.servers' = 'kafka:29092',
                'properties.group.id' = 'flink-ohlcv-consumer',
                'scan.startup.mode' = 'latest-offset',
                'format' = 'json',
                'json.fail-on-missing-field' = 'false',
                'json.ignore-parse-errors' = 'true'
            )
        """)
        
        logger.info("✓ Source table created")
        
        # ---------------------------------------------------
        # Create Kafka SINK table
        # ---------------------------------------------------
        logger.info("Creating Kafka sink table...")
        
        t_env.execute_sql("""
            CREATE TABLE ohlcv_1min (
                ticker STRING,
                window_start TIMESTAMP(3),
                window_end TIMESTAMP(3),
                `open` DOUBLE,
                high DOUBLE,
                low DOUBLE,
                `close` DOUBLE,
                volume BIGINT
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'market.ohlcv.1min',
                'properties.bootstrap.servers' = 'kafka:29092',
                'format' = 'json'
            )
        """)
        
        logger.info("✓ Sink table created")
        
        # ---------------------------------------------------
        # Create and execute aggregation query
        # ---------------------------------------------------
        logger.info("Creating aggregation query...")
        
        # Use statement set for better control
        stmt_set = t_env.create_statement_set()
        
        stmt_set.add_insert_sql("""
            INSERT INTO ohlcv_1min
            SELECT
                ticker,
                TUMBLE_START(ts, INTERVAL '1' MINUTE) AS window_start,
                TUMBLE_END(ts, INTERVAL '1' MINUTE) AS window_end,
                FIRST_VALUE(`open`) AS `open`,
                MAX(high) AS high,
                MIN(low) AS low,
                LAST_VALUE(`close`) AS `close`,
                SUM(volume) AS volume
            FROM market_quotes
            GROUP BY
                ticker,
                TUMBLE(ts, INTERVAL '1' MINUTE)
        """)
        
        logger.info("✓ Aggregation query created")
        
        # ---------------------------------------------------
        # Execute the job
        # ---------------------------------------------------
        logger.info("Submitting job to Flink cluster...")
        
        # Execute - this will run continuously
        stmt_set.execute()
        
        logger.info("="*60)
        logger.info("✓ OHLCV Aggregator Job Running!")
        logger.info("="*60)
        
    except Exception as e:
        logger.error("="*60)
        logger.error("ERROR: Job submission failed!")
        logger.error(f"Error: {str(e)}")
        logger.error("="*60)
        import traceback
        logger.error(traceback.format_exc())
        raise


if __name__ == "__main__":
    create_ohlcv_aggregator()