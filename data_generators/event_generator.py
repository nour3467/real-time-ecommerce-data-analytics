from datetime import datetime
import random
import logging
from typing import Dict, Any, Optional, List
import psycopg2
from psycopg2.extras import RealDictCursor
from kafka import KafkaProducer
import json
from abc import ABC, abstractmethod

class BaseGenerator(ABC):
    def __init__(self, db_config: Dict[str, Any], kafka_config: Dict[str, Any]):
        self.db_config = db_config
        self.kafka_config = kafka_config
        self.setup_connections()
        self.logger = logging.getLogger(self.__class__.__name__)

    def setup_connections(self):
        """Setup database and Kafka connections with proper error handling"""
        try:
            # Setup PostgreSQL connection with RealDictCursor for easier data handling
            self.db_conn = psycopg2.connect(
                **self.db_config,
                cursor_factory=RealDictCursor
            )
            self.db_conn.autocommit = True

            # Setup Kafka producer
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_config['bootstrap_servers'],
                value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8'),
                retries=5,
                acks='all'
            )
        except Exception as e:
            self.logger.error(f"Failed to setup connections: {str(e)}")
            raise

    def execute_query(self, query: str, params: tuple = None) -> List[Dict]:
        """Execute database query with error handling"""
        try:
            with self.db_conn.cursor() as cur:
                cur.execute(query, params)
                if cur.description:  # If it's a SELECT query
                    return cur.fetchall()
                return []
        except Exception as e:
            self.logger.error(f"Database query failed: {str(e)}")
            raise

    def insert_record(self, table: str, data: Dict[str, Any]) -> Optional[str]:
        """Insert a record and return its ID"""
        try:
            columns = list(data.keys())
            values = list(data.values())
            placeholders = ["%s"] * len(columns)

            query = f"""
                INSERT INTO {table} ({', '.join(columns)})
                VALUES ({', '.join(placeholders)})
                RETURNING *
            """

            with self.db_conn.cursor() as cur:
                cur.execute(query, values)
                result = cur.fetchone()
                return result
        except Exception as e:
            self.logger.error(f"Failed to insert into {table}: {str(e)}")
            raise

    def update_record(self, table: str, record_id: str, data: Dict[str, Any], id_column: str = 'id') -> bool:
        """Update an existing record"""
        try:
            set_clause = ", ".join([f"{k} = %s" for k in data.keys()])
            values = list(data.values()) + [record_id]

            query = f"""
                UPDATE {table}
                SET {set_clause}
                WHERE {id_column} = %s
            """

            with self.db_conn.cursor() as cur:
                cur.execute(query, values)
                return cur.rowcount > 0
        except Exception as e:
            self.logger.error(f"Failed to update {table}: {str(e)}")
            raise

    def simulate_network_issues(self) -> bool:
        """Simulate realistic network connectivity issues"""
        return random.random() < 0.02

    def introduce_data_quality_issues(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Introduce realistic data quality issues while maintaining schema validity"""
        if random.random() < 0.01:  # 1% chance of data issues
            nullable_fields = self.get_nullable_fields(data)
            if nullable_fields and random.random() < 0.5:
                # Introduce missing data only for nullable fields
                field_to_nullify = random.choice(nullable_fields)
                data[field_to_nullify] = None
            else:
                # Introduce invalid but type-consistent data
                field_to_modify = random.choice(list(data.keys()))
                data[field_to_modify] = self.generate_invalid_but_typed_data(
                    field_to_modify,
                    data[field_to_modify]
                )
        return data

    def get_nullable_fields(self, data: Dict[str, Any]) -> List[str]:
        """Get fields that can be NULL based on schema"""
        # This should be overridden by specific generators
        return [
            field for field, value in data.items()
            if field not in ['created_at', 'user_id']  # Example of non-nullable fields
        ]

    def generate_invalid_but_typed_data(self, field: str, current_value: Any) -> Any:
        """Generate invalid but type-consistent data"""
        if isinstance(current_value, str):
            return "INVALID_" + current_value
        elif isinstance(current_value, int):
            return -1
        elif isinstance(current_value, float):
            return -1.0
        elif isinstance(current_value, bool):
            return not current_value
        return current_value

    def apply_time_based_patterns(self, base_probability: float) -> float:
        """Adjust probabilities based on time patterns"""
        current_hour = datetime.now().hour
        current_day = datetime.now().weekday()

        multiplier = 1.0

        # Business hours pattern
        if 9 <= current_hour <= 17:
            multiplier *= 1.5
        elif 1 <= current_hour <= 6:
            multiplier *= 0.2
        elif 18 <= current_hour <= 22:
            multiplier *= 1.3

        # Weekend pattern
        if current_day in [5, 6]:
            multiplier *= 1.3

        return base_probability * multiplier

    @abstractmethod
    def generate_event(self) -> Dict[str, Any]:
        """Generate a single event - to be implemented by specific generators"""
        pass

    def send_event(self, topic: str, event: Dict[str, Any], store_in_db: bool = True):
        """Send event to Kafka and optionally store in database"""
        try:
            if self.simulate_network_issues():
                raise Exception("Simulated network failure")

            event = self.introduce_data_quality_issues(event)

            if store_in_db:
                self.insert_record(topic, event)

            future = self.producer.send(topic, event)
            future.get(timeout=10)

        except Exception as e:
            self.logger.error(f"Failed to process event: {str(e)}")
            self.store_failed_event(topic, event)

    def store_failed_event(self, topic: str, event: Dict[str, Any]):
        """Store failed events for retry"""
        try:
            self.insert_record('failed_events', {
                'topic': topic,
                'event_data': json.dumps(event),
                'created_at': datetime.now(),
                'retry_count': 0,
                'last_retry_at': None,
                'error_message': str(e)
            })
        except Exception as e:
            self.logger.error(f"Failed to store failed event: {str(e)}")

    def cleanup(self):
        """Clean up connections"""
        try:
            if self.producer:
                self.producer.close()
            if self.db_conn:
                self.db_conn.close()
        except Exception as e:
            self.logger.error(f"Cleanup failed: {str(e)}")