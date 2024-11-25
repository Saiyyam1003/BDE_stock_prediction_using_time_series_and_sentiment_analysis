from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
import json
from datetime import datetime
from stock_news_scraper import StockNewsScraper  # Import our previous scraper

class NewsKafkaProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.admin_client = None
        self.topic_name = 'stock_news'

    def initialize(self):
        """Initialize Kafka producer and admin client"""
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.admin_client = KafkaAdminClient(
            bootstrap_servers=self.bootstrap_servers
        )

    def create_topic(self, num_partitions=3, replication_factor=1):
        """Create Kafka topic if it doesn't exist"""
        try:
            topic = NewTopic(
                name=self.topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor
            )
            self.admin_client.create_topics([topic])
            print(f"Topic '{self.topic_name}' created successfully")
        except TopicAlreadyExistsError:
            print(f"Topic '{self.topic_name}' already exists")
        except Exception as e:
            print(f"Error creating topic: {str(e)}")

    def send_news_to_kafka(self, news_data):
        """Send news data to Kafka topic"""
        try:
            for company, articles in news_data.items():
                for article in articles:
                    # Add metadata
                    message = {
                        **article,
                        "kafka_timestamp": datetime.utcnow().isoformat(),
                        "company_symbol": company
                    }
                    
                    # Use company as key for partitioning
                    future = self.producer.send(
                        self.topic_name,
                        key=company.encode('utf-8'),
                        value=message
                    )
                    # Wait for message to be sent
                    future.get(timeout=60)
                    
            print(f"Successfully sent news data to Kafka topic '{self.topic_name}'")
            
        except Exception as e:
            print(f"Error sending data to Kafka: {str(e)}")
        
    def close(self):
        """Close Kafka connections"""
        if self.producer:
            self.producer.close()
        if self.admin_client:
            self.admin_client.close()

def main():
    # Initialize the news scraper
    api_key = "b5866a249cb0499c9aa3a7eb82e8f7f9"
    scraper = StockNewsScraper(api_key)
    
    # List of companies to track
    companies = ["AAPL", "TSLA", "MSFT", "GOOGL"]
    
    try:
        # Initialize Kafka producer
        kafka_producer = NewsKafkaProducer()
        kafka_producer.initialize()
        
        # Create Kafka topic
        kafka_producer.create_topic()
        
        # Fetch news data
        news_data = scraper.fetch_stock_news(
            companies=companies,
            days_back=15,  # Get today's news
            page_size=5   # 5 articles per company
        )
        
        # Send news data to Kafka
        kafka_producer.send_news_to_kafka(news_data)
        
    except Exception as e:
        print(f"Error in main process: {str(e)}")
    
    finally:
        # Clean up
        kafka_producer.close()

if __name__ == "__main__":
    main()