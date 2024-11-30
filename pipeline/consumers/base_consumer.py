from kafka import KafkaConsumer
import json
import logging
from abc import ABC, abstractmethod
from typing import Dict, Any

logger = logging.getLogger(__name__)

class BaseConsumer(ABC):
    def __init__(self, topic: str, kafka_config: Dict[str, Any], group_id: str):
        self.topic = topic
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=kafka_config['bootstrap_servers'],
            group_id=group_id,
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            enable_auto_commit=True
        )

    @abstractmethod
    def process_message(self, message: Dict[str, Any]):
        """Process a single message"""
        pass

    def start_consuming(self):
        """Start consuming messages"""
        logger.info(f"Started consuming from topic: {self.topic}")
        try:
            for message in self.consumer:
                try:
                    data = message.value
                    logger.debug(f"Received message: {data}")
                    self.process_message(data)
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    continue
        except Exception as e:
            logger.error(f"Consumer error: {e}")
        finally:
            self.consumer.close()