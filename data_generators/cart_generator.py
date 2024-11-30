from datetime import datetime
import uuid
import random
from typing import Dict, Any, List, Tuple
from .event_generator import BaseGenerator
from .config import CART_PATTERNS

class CartGenerator(BaseGenerator):
    def __init__(self, db_config: Dict[str, Any], kafka_config: Dict[str, Any]):
        super().__init__(db_config, kafka_config)
        self.active_carts = {}
        self.load_active_sessions()
        self.load_active_products()

    def load_active_sessions(self):
        """Load active sessions for cart creation"""
        query = """
            SELECT session_id, user_id
            FROM sessions
            WHERE timestamp_end IS NULL
        """
        self.active_sessions = {
            row['session_id']: row['user_id']
            for row in self.execute_query(query)
        }

    def load_active_products(self):
        """Load active products for cart items"""
        query = """
            SELECT product_id, price
            FROM products
            WHERE is_active = true
            AND stock_quantity > 0
        """
        self.active_products = {
            row['product_id']: row['price']
            for row in self.execute_query(query)
        }

    def generate_cart(self, session_id: str, user_id: str) -> Dict[str, Any]:
        """Generate new cart"""
        now = datetime.now()
        return {
            'cart_id': str(uuid.uuid4()),
            'user_id': user_id,
            'session_id': session_id,
            'status': 'active',
            'created_at': now,
            'updated_at': now
        }

    def generate_cart_item(self, cart_id: str, product_id: str, unit_price: float) -> Dict[str, Any]:
        """Generate cart item"""
        now = datetime.now()
        return {
            'cart_item_id': str(uuid.uuid4()),
            'cart_id': cart_id,
            'product_id': product_id,
            'quantity': random.randint(1, 5),
            'added_timestamp': now,
            'removed_timestamp': None,
            'unit_price': unit_price
        }

    def generate_cart_with_items(self) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """Generate a cart with its items"""
        if not self.active_sessions or not self.active_products:
            self.logger.warning("No active sessions or products available")
            return None, None

        # Select random session and create cart
        session_id = random.choice(list(self.active_sessions.keys()))
        user_id = self.active_sessions[session_id]
        cart = self.generate_cart(session_id, user_id)

        # Generate cart items
        num_items = random.randint(
            CART_PATTERNS['items']['min'],
            CART_PATTERNS['items']['max']
        )

        cart_items = []
        selected_products = random.sample(
            list(self.active_products.items()),
            min(num_items, len(self.active_products))
        )

        for product_id, price in selected_products:
            cart_item = self.generate_cart_item(cart['cart_id'], product_id, price)
            cart_items.append(cart_item)

        return cart, cart_items

    def update_cart_status(self, cart_id: str) -> str:
        """Update cart status based on patterns"""
        status_choices = list(CART_PATTERNS['status_distribution'].items())
        new_status = random.choices(
            [status for status, _ in status_choices],
            weights=[weight for _, weight in status_choices]
        )[0]

        now = datetime.now()
        self.update_record(
            'carts',
            cart_id,
            {'status': new_status, 'updated_at': now},
            'cart_id'
        )

        return new_status

    def generate_event(self) -> Dict[str, Any]:
        """Generate cart events"""
        # 70% chance to update existing cart, 30% for new cart
        if self.active_carts and random.random() < 0.7:
            cart_id = random.choice(list(self.active_carts.keys()))
            cart_data = self.active_carts[cart_id]

            # Decide between updating status or adding/removing items
            if random.random() < 0.3:  # 30% chance to update status
                new_status = self.update_cart_status(cart_id)
                if new_status in ['converted', 'abandoned']:
                    del self.active_carts[cart_id]
                return {'type': 'cart_update', 'cart_id': cart_id, 'status': new_status}
            else:
                # Add or remove items
                if random.random() < 0.7:  # 70% chance to add item
                    product_id, price = random.choice(list(self.active_products.items()))
                    cart_item = self.generate_cart_item(cart_id, product_id, price)
                    self.insert_record('cart_items', cart_item)
                    return {'type': 'item_add', 'cart_item': cart_item}
                else:
                    # Mark random item as removed
                    query = """
                        SELECT cart_item_id
                        FROM cart_items
                        WHERE cart_id = %s
                        AND removed_timestamp IS NULL
                    """
                    active_items = self.execute_query(query, (cart_id,))
                    if active_items:
                        item_id = random.choice(active_items)['cart_item_id']
                        self.update_record(
                            'cart_items',
                            item_id,
                            {'removed_timestamp': datetime.now()},
                            'cart_item_id'
                        )
                        return {'type': 'item_remove', 'cart_item_id': item_id}
        else:
            # Generate new cart with items
            cart, cart_items = self.generate_cart_with_items()
            if cart:
                self.insert_record('carts', cart)
                for item in cart_items:
                    self.insert_record('cart_items', item)
                self.active_carts[cart['cart_id']] = cart
                return {
                    'type': 'cart_create',
                    'cart': cart,
                    'items': cart_items
                }

    def run(self):
        """Run the cart generator"""
        while True:
            try:
                event = self.generate_event()
                if event:
                    self.send_event('cart_events', event)

                # Realistic timing between cart events
                delay = random.expovariate(1/30)  # Average 30 seconds between events
                time.sleep(delay)

            except Exception as e:
                self.logger.error(f"Error in cart generator: {str(e)}")
                time.sleep(5)  # Back off on error

    def cleanup(self):
        """Cleanup resources"""
        super().cleanup()