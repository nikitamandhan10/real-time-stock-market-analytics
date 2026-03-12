"""
Real-time market data producer - fetches stock quotes and sends to Kafka
"""
import time
import json
import yaml
import yfinance as yf
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MarketDataProducer:
    """Produces real-time stock market data to Kafka"""
    
    def __init__(self, config_path='config/config.yaml'):
        #Initialize producer with configuration
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.tickers = self.config['tickers']
        self.kafka_config = self.config['kafka']
        self.fetch_interval = self.config['streaming']['fetch_interval_seconds']
        
        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_config['bootstrap_servers'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            compression_type='gzip',
            acks='all',
            retries=3
        )
        
        logger.info(f"MarketDataProducer initialized with {len(self.tickers)} tickers")
    
    def fetch_quote(self, ticker):
        """Fetch current quote for a ticker"""
        try:
            stock = yf.Ticker(ticker)
            
            # Get latest quote (today's data)
            hist = stock.history(period='1d', interval='1m')
            
            if hist.empty:
                logger.warning(f"No data available for {ticker}")
                return None
            
            latest = hist.iloc[-1]
            
            quote = {
                'ticker': ticker,
                'timestamp': datetime.now().isoformat(),
                'open': float(latest['Open']),
                'high': float(latest['High']),
                'low': float(latest['Low']),
                'close': float(latest['Close']),
                'volume': int(latest['Volume'])
            }
            
            return quote
            
        except Exception as e:
            logger.error(f"Error fetching {ticker}: {str(e)}")
            return None
    
    def send_to_kafka(self, topic, key, value):
        """Send message to Kafka topic"""
        try:
            future = self.producer.send(
                topic,
                key=key.encode('utf-8'),
                value=value
            )
            
            # Wait for confirmation
            record_metadata = future.get(timeout=10)
            
            logger.debug(
                f"Sent to {record_metadata.topic} "
                f"partition {record_metadata.partition} "
                f"offset {record_metadata.offset}"
            )
            
            return True
            
        except KafkaError as e:
            logger.error(f"Failed to send to Kafka: {str(e)}")
            return False
    
    def run(self):
        """Main producer loop"""
        logger.info("Starting market data producer...")
        logger.info(f"Fetch interval: {self.fetch_interval} seconds")
        
        iteration = 0
        
        try:
            while True:
                iteration += 1
                logger.info(f"\n{'='*60}")
                logger.info(f"Iteration {iteration} - {datetime.now()}")
                logger.info(f"{'='*60}")
                
                quotes_sent = 0
                
                for ticker in self.tickers:
                    quote = self.fetch_quote(ticker)
                    
                    if quote:
                        success = self.send_to_kafka(
                            topic=self.kafka_config['topics']['quotes'],
                            key=ticker,
                            value=quote
                        )
                        
                        if success:
                            quotes_sent += 1
                            logger.info(f"✓ {ticker}: ${quote['close']:.2f}")
                
                logger.info(f"\nSent {quotes_sent}/{len(self.tickers)} quotes to Kafka")
                
                # Wait before next iteration
                logger.info(f"Sleeping for {self.fetch_interval} seconds...\n")
                time.sleep(self.fetch_interval)
                
        except KeyboardInterrupt:
            logger.info("\nShutting down producer...")
        finally:
            self.producer.close()
            logger.info("Producer closed")


if __name__ == "__main__":
    producer = MarketDataProducer()
    producer.run()
