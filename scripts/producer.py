from kafka import KafkaProducer
import yfinance as yf
import json
from datetime import datetime
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_producer():
    """Create and return a Kafka producer instance"""
    return KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def fetch_and_format_stock_data(symbol):
    """Fetch stock data and format it according to consumer expectations"""
    # Download stock data
    data = yf.download(symbol, period='1d', interval='1m')
    
    # Format the data into the structure expected by the consumer
    formatted_records = []
    for timestamp, row in data.iterrows():
        record = {
            "timestamp": timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            "symbol": symbol,
            "price": float(row['Close']),
            "volume": float(row['Volume'])
        }
        formatted_records.append(record)
    
    return formatted_records

def send_stock_data(symbol, producer):
    """Send formatted stock data to Kafka topic"""
    try:
        # Fetch and format the data
        records = fetch_and_format_stock_data(symbol)
        
        # Send each record to Kafka
        for record in records:
            producer.send('stock_data', value=record)
            logger.info(f"Sent record: {record}")
        
        # Ensure all messages are sent
        producer.flush()
        logger.info(f"Successfully sent {len(records)} records for {symbol}")
        
    except Exception as e:
        logger.error(f"Error sending stock data: {str(e)}")
        raise

def run_continuous_producer(symbols=['AAPL'], interval_seconds=300):
    """Run the producer continuously with specified interval"""
    producer = create_producer()
    
    try:
        while True:
            for symbol in symbols:
                send_stock_data(symbol, producer)
            logger.info(f"Waiting {interval_seconds} seconds before next update...")
            time.sleep(interval_seconds)
            
    except KeyboardInterrupt:
        logger.info("Stopping producer...")
    
    finally:
        producer.close()
        logger.info("Producer closed")

if __name__ == "__main__":
    # List of stock symbols to track
    SYMBOLS = ['AAPL', 'GOOGL', 'MSFT']
    
    # Run the producer with 1-minute updates
    run_continuous_producer(symbols=SYMBOLS, interval_seconds=300)