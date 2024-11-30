from datetime import datetime, timedelta
import random
from faker import Faker
import time
from .event_generator import BaseGenerator
from .config import SESSION_PATTERNS
import uuid

class SessionGenerator(BaseGenerator):
    def __init__(self, db_config, kafka_config):
        super().__init__(db_config, kafka_config)
        self.fake = Faker()
        self.active_sessions = {}
        self.load_active_users()

    def load_active_users(self):
        """Load active users from database"""
        with self.db_conn.cursor() as cur:
            cur.execute("SELECT user_id FROM users WHERE is_active = true")
            self.active_users = [row[0] for row in cur.fetchall()]

    def generate_device_info(self):
        """Generate realistic device and browser information"""
        device_choices = list(SESSION_PATTERNS['device_types'].items())
        device_type, _ = random.choices(
            device_choices,
            weights=[w for _, w in device_choices]
        )[0]

        os_mapping = {
            'mobile': ['iOS 15', 'iOS 16', 'Android 12', 'Android 13'],
            'desktop': ['Windows 11', 'macOS 12', 'Ubuntu 22.04'],
            'tablet': ['iPadOS 16', 'Android 12']
        }

        return {
            'device_type': device_type,
            'os_info': random.choice(os_mapping[device_type]),
            'browser_info': random.choice([
                k for k, v in SESSION_PATTERNS['browser_distribution'].items()
            ])
        }

    def generate_utm_data(self):
        """Generate UTM tracking data"""
        source_types = {
            'organic_search': ('google', 'organic', None),
            'paid_search': ('google', 'cpc', f'spring_sale_{datetime.now().year}'),
            'social': (random.choice(['facebook', 'instagram', 'twitter']), 'social', None),
            'email': ('email', 'email', 'newsletter'),
            'direct': (None, None, None)
        }

        source_type = random.choice(list(source_types.keys()))
        utm_source, utm_medium, utm_campaign = source_types[source_type]

        return {
            'referral_source': source_type,
            'utm_source': utm_source,
            'utm_medium': utm_medium,
            'utm_campaign': utm_campaign
        }

    def generate_session(self) -> dict:
        """Generate a new session record matching DB schema"""
        user_id = random.choice(self.active_users)
        device_info = self.generate_device_info()
        utm_data = self.generate_utm_data()
        now = datetime.now()

        return {
            'session_id': str(uuid.uuid4()),
            'user_id': user_id,
            'timestamp_start': now.isoformat(),
            'timestamp_end': None,
            'device_type': device_info['device_type'],
            'os_info': device_info['os_info'],
            'browser_info': device_info['browser_info'],
            'ip_address': self.fake.ipv4(),
            'referral_source': utm_data['referral_source'],
            'utm_source': utm_data['utm_source'],
            'utm_medium': utm_data['utm_medium'],
            'utm_campaign': utm_data['utm_campaign'],
            'created_at': now.isoformat()
        }

    def update_session(self, session_id: str, session_data: dict) -> dict:
        """Update an existing session"""
        now = datetime.now()
        session_start = datetime.fromisoformat(session_data['timestamp_start'])

        # Calculate realistic session duration based on patterns
        min_minutes = SESSION_PATTERNS['duration']['min_minutes']
        max_minutes = SESSION_PATTERNS['duration']['max_minutes']
        typical_duration = SESSION_PATTERNS['duration']['typical_duration']

        # Use exponential distribution for realistic session duration
        duration_minutes = random.expovariate(1/typical_duration)
        duration_minutes = max(min_minutes, min(max_minutes, duration_minutes))

        session_data['timestamp_end'] = (session_start + timedelta(minutes=duration_minutes)).isoformat()
        return session_data

    def generate_event(self):
        """Generate a session event"""
        if self.active_sessions and random.random() < 0.7:
            # Update existing session
            session_id = random.choice(list(self.active_sessions.keys()))
            session_data = self.active_sessions[session_id]

            if random.random() < 0.2:  # 20% chance to end session
                session_data = self.update_session(session_id, session_data)
                del self.active_sessions[session_id]

            return session_data
        else:
            # Start new session
            session_data = self.generate_session()
            self.active_sessions[session_data['session_id']] = session_data
            return session_data

    def run(self):
        """Run the session generator"""
        while True:
            try:
                event = self.generate_event()
                self.send_event('sessions', event)

                # Realistic timing between session events
                time.sleep(random.uniform(0.1, 2.0))

            except Exception as e:
                self.logger.error(f"Error generating session: {str(e)}")
                time.sleep(5)  # Back off on error