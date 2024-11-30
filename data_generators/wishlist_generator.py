from datetime import datetime
import uuid
import random
from typing import Dict, Any
from .event_generator import BaseGenerator

class WishlistGenerator(BaseGenerator):
    def __init__(self, db_config: Dict[str, Any], kafka_config: Dict[str, Any]):
        super().__init__(db_config, kafka_config)
        self.load_active_data()

    def load_active_data(self):
        """Load active users and products"""
        self.active_users = self.execute_query("""
            SELECT user_id
            FROM users
            WHERE is_active = true
        """)

        self.active_products = self.execute_query("""
            SELECT product_id
            FROM products
            WHERE is_active = true
        """)

    def check_existing_wishlist(self, user_id: str, product_id: str) -> bool:
        """Check if product already in user's wishlist"""
        query = """
            SELECT 1 FROM wishlists
            WHERE user_id = %s
            AND product_id = %s
            AND removed_timestamp IS NULL
        """
        return bool(self.execute_query(query, (user_id, product_id)))

    def generate_wishlist_item(self) -> Dict[str, Any]:
        """Generate new wishlist item"""
        if not self.active_users or not self.active_products:
            return None

        user_id = random.choice(self.active_users)['user_id']
        product_id = random.choice(self.active_products)['product_id']

        # Avoid duplicates in active wishlists
        if self.check_existing_wishlist(user_id, product_id):
            return None

        now = datetime.now()
        return {
            'wishlist_id': str(uuid.uuid4()),
            'user_id': user_id,
            'product_id': product_id,
            'added_timestamp': now,
            'removed_timestamp': None,
            'notes': random.choice([
                None,
                "Birthday wishlist",
                "Future purchase",
                "Price too high now",
                "Wait for sale"
            ])
        }

    def remove_from_wishlist(self) -> Dict[str, Any]:
        """Remove item from wishlist"""
        query = """
            SELECT wishlist_id, user_id, product_id
            FROM wishlists
            WHERE removed_timestamp IS NULL
        """
        active_items = self.execute_query(query)

        if not active_items:
            return None

        item = random.choice(active_items)
        now = datetime.now()

        self.update_record(
            'wishlists',
            item['wishlist_id'],
            {'removed_timestamp': now},
            'wishlist_id'
        )

        return {
            'wishlist_id': item['wishlist_id'],
            'user_id': item['user_id'],
            'product_id': item['product_id'],
            'removed_timestamp': now
        }

    def generate_event(self) -> Dict[str, Any]:
        """Generate wishlist events"""
        # 70% chance to add new item, 30% to remove existing
        if not self.active_users or random.random() < 0.7:
            wishlist_item = self.generate_wishlist_item()
            if wishlist_item:
                self.insert_record('wishlists', wishlist_item)
                return {
                    'type': 'wishlist_add',
                    'data': wishlist_item
                }
        else:
            removed_item = self.remove_from_wishlist()
            if removed_item:
                return {
                    'type': 'wishlist_remove',
                    'data': removed_item
                }

    def run(self):
        """Run the wishlist generator"""
        while True:
            try:
                event = self.generate_event()
                if event:
                    self.send_event('wishlists', event)

                # Wishlist updates happen relatively infrequently
                time.sleep(random.uniform(10, 60))

                # Periodically refresh active data
                if random.random() < 0.1:  # 10% chance each iteration
                    self.load_active_data()

            except Exception as e:
                self.logger.error(f"Error in wishlist generator: {str(e)}")
                time.sleep(5)