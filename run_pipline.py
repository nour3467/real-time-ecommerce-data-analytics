import logging
from concurrent.futures import ThreadPoolExecutor
from pipeline.consumers.base_consumer import BaseConsumer
from pipeline.processors.event_processor import EventProcessor
from pipeline.db.db_writer import DatabaseWriter
from config import DB_CONFIG, KAFKA_CONFIG

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_consumer(topic: str, db_writer: DatabaseWriter):
    """Run consumer for a specific topic"""
    class TopicConsumer(BaseConsumer):
        def process_message(self, message):
            # Process the message
            processed_data = EventProcessor.process_event(topic, message)
            # Write to database
            db_writer.write_event(topic, processed_data)

    consumer = TopicConsumer(topic, KAFKA_CONFIG, f"ecommerce-{topic}-group")
    consumer.start_consuming()

def main():
    # Initialize database writer
    db_writer = DatabaseWriter(DB_CONFIG)

    # Define topics to consume
    topics = [
        'users',
        'user_demographics',
        'sessions',
        'products',
        'orders'
    ]

    # Start consumers in separate threads
    with ThreadPoolExecutor(max_workers=len(topics)) as executor:
        for topic in topics:
            executor.submit(run_consumer, topic, db_writer)

if __name__ == "__main__":
    main()