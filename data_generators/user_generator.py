import uuid
from faker import Faker
from typing import Dict, Any, Tuple
import random
from datetime import datetime, timedelta
from .event_generator import BaseGenerator
from .config import USER_PATTERNS
import time
import hashlib

class UserGenerator(BaseGenerator):
    def __init__(self, db_config: Dict[str, Any], kafka_config: Dict[str, Any]):
        super().__init__(db_config, kafka_config)
        self.fake = Faker()
        self.load_existing_users()

    def load_existing_users(self):
        """Load existing users from database"""
        with self.db_conn.cursor() as cur:
            cur.execute("SELECT user_id, email FROM users WHERE is_active = true")
            self.existing_users = dict(cur.fetchall())

    def generate_user(self) -> Tuple[Dict[str, Any], str]:
        """Generate base user data"""
        user_id = str(uuid.uuid4())
        first_name = self.fake.first_name()
        last_name = self.fake.last_name()
        email = f"{first_name.lower()}.{last_name.lower()}{random.randint(1,999)}@{self.fake.free_email_domain()}"

        # Generate a proper password hash
        password = self.fake.password()
        password_hash = hashlib.sha256(password.encode()).hexdigest()

        user_data = {
            'user_id': user_id,
            'email': email,
            'password_hash': password_hash,
            'first_name': first_name,
            'last_name': last_name,
            'registration_date': datetime.now().isoformat(),
            'last_login': None,
            'is_active': True,
            'preferences': {
                'language': random.choice(['en', 'es', 'fr']),
                'currency': random.choice(['USD', 'EUR', 'GBP']),
                'notifications': random.choice([True, False])
            },
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat()
        }

        return user_data, user_id

    def generate_user_demographics(self, user_id: str) -> Dict[str, Any]:
        """Generate user demographics data"""
        return {
            'demographic_id': str(uuid.uuid4()),
            'user_id': user_id,
            'age_range': random.choice(USER_PATTERNS['demographics']['age_ranges']),
            'gender': random.choice(USER_PATTERNS['demographics']['genders']),
            'income_bracket': random.choice(USER_PATTERNS['demographics']['income_brackets']),
            'occupation': self.fake.job(),
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat()
        }

    def generate_user_address(self, user_id: str) -> Dict[str, Any]:
        """Generate user address data"""
        return {
            'address_id': str(uuid.uuid4()),
            'user_id': user_id,
            'address_type': random.choice(USER_PATTERNS['address']['types']),
            'street_address': self.fake.street_address(),
            'city': self.fake.city(),
            'state': self.fake.state(),
            'country': self.fake.country(),
            'postal_code': self.fake.postcode(),
            'is_default': True,  # First address is default
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat()
        }

    def generate_event(self) -> Dict[str, Any]:
        """Generate user-related events"""
        if not self.existing_users or random.random() < 0.3:  # 30% chance for new user
            return self.generate_new_user_event()
        else:
            return self.generate_update_event()

    def generate_new_user_event(self) -> Dict[str, Any]:
        """Generate a new user with demographics and address"""
        user_data, user_id = self.generate_user()
        demographics_data = self.generate_user_demographics(user_id)
        address_data = self.generate_user_address(user_id)

        return {
            'event_type': 'new_user',
            'user': user_data,
            'demographics': demographics_data,
            'address': address_data
        }

    def generate_update_event(self) -> Dict[str, Any]:
        """Generate update event for existing user"""
        user_id = random.choice(list(self.existing_users.keys()))
        update_type = random.choice([
            'user_update',
            'demographics_update',
            'address_update',
            'address_add'
        ])

        if update_type == 'user_update':
            return {
                'event_type': 'user_update',
                'user_id': user_id,
                'data': {
                    'is_active': random.choice([True, False]),
                    'last_login': datetime.now().isoformat(),
                    'updated_at': datetime.now().isoformat()
                }
            }
        elif update_type == 'demographics_update':
            return {
                'event_type': 'demographics_update',
                'user_id': user_id,
                'data': self.generate_user_demographics(user_id)
            }
        else:  # address updates
            return {
                'event_type': update_type,
                'user_id': user_id,
                'data': self.generate_user_address(user_id)
            }

    def send_events(self, event: Dict[str, Any]):
        """Send events to appropriate Kafka topics"""
        if event['event_type'] == 'new_user':
            self.send_event('users', event['user'])
            self.send_event('user_demographics', event['demographics'])
            self.send_event('user_addresses', event['address'])
        else:
            topic = {
                'user_update': 'users',
                'demographics_update': 'user_demographics',
                'address_update': 'user_addresses',
                'address_add': 'user_addresses'
            }[event['event_type']]
            self.send_event(topic, event['data'])

    def run(self):
        """Run the generator"""
        while True:
            try:
                event = self.generate_event()
                self.send_events(event)

                # Simulate realistic timing between events
                delay = random.expovariate(1/10)  # Average 10 seconds between events
                time.sleep(delay)

            except Exception as e:
                self.logger.error(f"Error in user generator: {str(e)}")
                time.sleep(5)  # Back off on error