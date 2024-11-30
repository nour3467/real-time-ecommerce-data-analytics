from datetime import datetime
import uuid
import random
from typing import Dict, Any, List
from .event_generator import BaseGenerator
from .config import PRODUCT_PATTERNS

class ProductGenerator(BaseGenerator):
    def __init__(self, db_config: Dict[str, Any], kafka_config: Dict[str, Any]):
        super().__init__(db_config, kafka_config)
        self.fake = Faker()
        self.load_categories()

    def load_categories(self):
        """Load available categories"""
        query = "SELECT category_id FROM product_categories"
        self.categories = [row['category_id'] for row in self.execute_query(query)]

    def generate_sku(self) -> str:
        """Generate unique SKU"""
        while True:
            sku = f"SKU-{self.fake.unique.random_number(digits=6)}"
            if not self.execute_query(
                "SELECT 1 FROM products WHERE sku = %s", (sku,)
            ):
                return sku

    def generate_price_cost(self) -> Tuple[float, float]:
        """Generate realistic price and cost"""
        price_ranges = PRODUCT_PATTERNS['products']['price_ranges']
        range_type = random.choice(list(price_ranges.keys()))
        min_price, max_price = price_ranges[range_type]

        price = round(random.uniform(min_price, max_price), 2)
        cost = round(price * random.uniform(0.4, 0.7), 2)  # 40-70% of price
        return price, cost

    def generate_stock_quantity(self) -> int:
        """Generate realistic stock quantity"""
        stock_levels = PRODUCT_PATTERNS['products']['stock_levels']
        level = random.choices(
            list(stock_levels.keys()),
            weights=[0.05, 0.15, 0.60, 0.20]  # Adjusted weights
        )[0]

        if level == 'out_of_stock':
            return 0
        else:
            min_stock, max_stock = stock_levels[level]
            return random.randint(min_stock, max_stock)

    def generate_product(self) -> Dict[str, Any]:
        """Generate new product"""
        if not self.categories:
            self.logger.warning("No categories available")
            return None

        price, cost = self.generate_price_cost()
        now = datetime.now()

        return {
            'product_id': str(uuid.uuid4()),
            'sku': self.generate_sku(),
            'name': self.fake.product_name(),
            'description': self.fake.text(max_nb_chars=200),
            'category_id': random.choice(self.categories),
            'price': price,
            'cost': cost,
            'stock_quantity': self.generate_stock_quantity(),
            'is_active': True,
            'created_at': now,
            'updated_at': now
        }

    def update_product(self, product_id: str) -> Dict[str, Any]:
        """Update existing product"""
        update_types = [
            ('price', lambda: self.generate_price_cost()[0]),
            ('stock_quantity', self.generate_stock_quantity),
            ('is_active', lambda: random.random() > 0.9)  # 10% chance to deactivate
        ]

        field, generator = random.choice(update_types)
        value = generator()

        update_data = {
            field: value,
            'updated_at': datetime.now()
        }

        self.update_record('products', product_id, update_data, 'product_id')
        return {'product_id': product_id, **update_data}

    def generate_event(self) -> Dict[str, Any]:
        """Generate product events"""
        existing_products = self.execute_query(
            "SELECT product_id FROM products WHERE is_active = true"
        )

        # 70% chance to update existing product if available
        if existing_products and random.random() < 0.7:
            product_id = random.choice(existing_products)['product_id']
            return {
                'type': 'product_update',
                'data': self.update_product(product_id)
            }
        else:
            product = self.generate_product()
            if product:
                self.insert_record('products', product)
                return {
                    'type': 'product_create',
                    'data': product
                }

    def run(self):
        """Run the product generator"""
        while True:
            try:
                event = self.generate_event()
                if event:
                    self.send_event('products', event)
                time.sleep(random.uniform(5, 30))  # Products update more frequently
            except Exception as e:
                self.logger.error(f"Error in product generator: {str(e)}")
                time.sleep(5)