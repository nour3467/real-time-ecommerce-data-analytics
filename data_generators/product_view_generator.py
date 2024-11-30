from datetime import datetime
import uuid
import random
from typing import Dict, Any
from .event_generator import BaseGenerator
from .config import SESSION_PATTERNS

class ProductViewGenerator(BaseGenerator):
    def __init__(self, db_config: Dict[str, Any], kafka_config: Dict[str, Any]):
        super().__init__(db_config, kafka_config)
        self.load_active_data()

    def load_active_data(self):
        """Load active sessions and products"""
        self.active_sessions = self.execute_query("""
            SELECT session_id
            FROM sessions
            WHERE timestamp_end IS NULL
        """)

        self.active_products = self.execute_query("""
            SELECT product_id
            FROM products
            WHERE is_active = true
        """)

    def generate_view_duration(self) -> str:
        """Generate realistic view duration"""
        # Define view patterns
        patterns = [
            ('bounce', 0.15, '5 seconds'),
            ('quick_view', 0.45, '30 seconds'),
            ('detailed_view', 0.30, '2 minutes'),
            ('thorough_review', 0.10, '5 minutes')
        ]

        view_type, probability, base_duration = random.choices(
            patterns,
            weights=[p[1] for p in patterns]
        )[0]

        # Add some randomness to duration
        base_seconds = int(base_duration.split()[0]) * {
            'seconds': 1,
            'minutes': 60
        }[base_duration.split()[1]]

        variation = random.uniform(0.8, 1.2)
        duration_seconds = int(base_seconds * variation)

        return f"{duration_seconds} seconds"

    def generate_source_page(self) -> str:
        """Generate realistic source page"""
        sources = [
            ('search_results', 0.4),
            ('category_page', 0.3),
            ('recommended_products', 0.15),
            ('homepage_featured', 0.1),
            ('email_link', 0.05)
        ]
        return random.choices(
            [s[0] for s in sources],
            weights=[s[1] for s in sources]
        )[0]

    def generate_view(self) -> Dict[str, Any]:
        """Generate product view event"""
        if not self.active_sessions or not self.active_products:
            return None

        now = datetime.now()
        return {
            'view_id': str(uuid.uuid4()),
            'session_id': random.choice(self.active_sessions)['session_id'],
            'product_id': random.choice(self.active_products)['product_id'],
            'view_timestamp': now.isoformat(),
            'view_duration': self.generate_view_duration(),
            'source_page': self.generate_source_page()
        }

    def generate_event(self) -> Dict[str, Any]:
        """Generate view events"""
        view = self.generate_view()
        if view:
            return {
                'type': 'product_view',
                'data': view
            }

    def run(self):
        """Run the view generator"""
        while True:
            try:
                event = self.generate_event()
                if event:
                    self.send_event('product_views', event)

                # Views happen frequently during active sessions
                time.sleep(random.expovariate(1/5))  # Average 5 seconds between views

                # Periodically refresh active sessions and products
                if random.random() < 0.05:  # 5% chance each iteration
                    self.load_active_data()

            except Exception as e:
                self.logger.error(f"Error in view generator: {str(e)}")
                time.sleep(5)