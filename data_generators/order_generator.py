from datetime import datetime
import uuid
import random
from typing import Dict, Any, List, Tuple
from .event_generator import BaseGenerator
from .config import ORDER_PATTERNS

class OrderGenerator(BaseGenerator):
    def __init__(self, db_config: Dict[str, Any], kafka_config: Dict[str, Any]):
        super().__init__(db_config, kafka_config)
        self.load_active_data()

    def load_active_data(self):
        """Load necessary data for order generation"""
        # Get converted carts ready for orders
        self.pending_carts = self.execute_query("""
            SELECT c.cart_id, c.user_id, c.session_id
            FROM carts c
            WHERE c.status = 'converted'
            AND NOT EXISTS (
                SELECT 1 FROM orders o WHERE o.cart_id = c.cart_id
            )
        """)

        # Get active orders for updates
        self.active_orders = self.execute_query("""
            SELECT order_id, status
            FROM orders
            WHERE status NOT IN ('delivered', 'cancelled')
        """)

        # Get user addresses
        self.user_addresses = self.execute_query("""
            SELECT user_id, address_id, address_type
            FROM user_addresses
            WHERE is_default = true
        """)

    def get_user_addresses(self, user_id: str) -> Tuple[str, str]:
        """Get billing and shipping addresses for user"""
        addresses = [addr for addr in self.user_addresses if addr['user_id'] == user_id]
        billing = next((addr['address_id'] for addr in addresses if addr['address_type'] == 'billing'), None)
        shipping = next((addr['address_id'] for addr in addresses if addr['address_type'] == 'shipping'), None)
        return billing, shipping

    def calculate_order_amounts(self, cart_items: List[Dict]) -> Dict[str, float]:
        """Calculate order amounts"""
        subtotal = sum(item['quantity'] * item['unit_price'] for item in cart_items)
        tax_rate = 0.08  # 8% tax rate
        shipping_base = 10.00

        tax_amount = round(subtotal * tax_rate, 2)
        shipping_amount = shipping_base if subtotal < 100 else 0  # Free shipping over $100

        # Calculate potential discount
        discount_amount = 0
        if subtotal > 200:
            discount_amount = round(subtotal * 0.1, 2)  # 10% off orders over $200

        total_amount = subtotal + tax_amount + shipping_amount - discount_amount

        return {
            'total_amount': total_amount,
            'tax_amount': tax_amount,
            'shipping_amount': shipping_amount,
            'discount_amount': discount_amount
        }

    def generate_order(self, cart_data: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """Generate new order and order items"""
        # Get cart items
        cart_items = self.execute_query("""
            SELECT product_id, quantity, unit_price
            FROM cart_items
            WHERE cart_id = %s AND removed_timestamp IS NULL
        """, (cart_data['cart_id'],))

        if not cart_items:
            return None, None

        # Get addresses
        billing_id, shipping_id = self.get_user_addresses(cart_data['user_id'])
        if not (billing_id and shipping_id):
            return None, None

        # Calculate amounts
        amounts = self.calculate_order_amounts(cart_items)

        now = datetime.now()

        # Create order
        order = {
            'order_id': str(uuid.uuid4()),
            'user_id': cart_data['user_id'],
            'cart_id': cart_data['cart_id'],
            'status': 'pending',
            'total_amount': amounts['total_amount'],
            'tax_amount': amounts['tax_amount'],
            'shipping_amount': amounts['shipping_amount'],
            'discount_amount': amounts['discount_amount'],
            'payment_method': random.choices(
                list(ORDER_PATTERNS['payment_methods'].keys()),
                weights=list(ORDER_PATTERNS['payment_methods'].values())
            )[0],
            'delivery_method': random.choices(
                list(ORDER_PATTERNS['delivery_methods'].keys()),
                weights=list(ORDER_PATTERNS['delivery_methods'].values())
            )[0],
            'billing_address_id': billing_id,
            'shipping_address_id': shipping_id,
            'created_at': now,
            'updated_at': now
        }

        # Create order items
        order_items = []
        for cart_item in cart_items:
            order_items.append({
                'order_item_id': str(uuid.uuid4()),
                'order_id': order['order_id'],
                'product_id': cart_item['product_id'],
                'quantity': cart_item['quantity'],
                'unit_price': cart_item['unit_price'],
                'discount_amount': round(cart_item['unit_price'] * cart_item['quantity'] *
                                      (amounts['discount_amount'] / amounts['total_amount'])
                                      if amounts['total_amount'] > 0 else 0, 2),
                'created_at': now
            })

        return order, order_items

    def update_order_status(self, order_id: str, current_status: str) -> str:
        """Update order status following logical progression"""
        status_flow = {
            'pending': 'processing',
            'processing': 'shipped',
            'shipped': 'delivered'
        }

        new_status = status_flow.get(current_status)
        if new_status:
            now = datetime.now()
            self.update_record(
                'orders',
                order_id,
                {
                    'status': new_status,
                    'updated_at': now
                },
                'order_id'
            )
            return new_status
        return current_status

    def generate_event(self) -> Dict[str, Any]:
        """Generate order events"""
        # 30% chance to create new order, 70% to update existing
        if self.active_orders and random.random() > 0.3:
            # Update existing order
            order = random.choice(self.active_orders)
            new_status = self.update_order_status(order['order_id'], order['status'])
            return {
                'type': 'order_update',
                'order_id': order['order_id'],
                'new_status': new_status
            }
        elif self.pending_carts:
            # Create new order
            cart_data = random.choice(self.pending_carts)
            order, order_items = self.generate_order(cart_data)
            if order and order_items:
                # Insert records
                self.insert_record('orders', order)
                for item in order_items:
                    self.insert_record('order_items', item)
                return {
                    'type': 'order_create',
                    'order': order,
                    'items': order_items
                }

    def run(self):
        """Run the order generator"""
        while True:
            try:
                event = self.generate_event()
                if event:
                    self.send_event('orders', event)

                # Orders update every few minutes
                time.sleep(random.uniform(60, 300))

                # Refresh data periodically
                if random.random() < 0.2:
                    self.load_active_data()

            except Exception as e:
                self.logger.error(f"Error in order generator: {str(e)}")
                time.sleep(5)