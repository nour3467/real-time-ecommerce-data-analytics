from datetime import datetime
import uuid
import random
from typing import Dict, Any, List
from .event_generator import BaseGenerator
from .config import PRODUCT_PATTERNS

class ProductCategoryGenerator(BaseGenerator):
    def __init__(self, db_config: Dict[str, Any], kafka_config: Dict[str, Any]):
        super().__init__(db_config, kafka_config)
        self.categories = self.load_categories()

    def load_categories(self) -> Dict[str, Dict]:
        """Load existing category hierarchy"""
        query = """
            SELECT category_id, parent_category_id, name, description
            FROM product_categories
        """
        return {
            row['category_id']: dict(row)
            for row in self.execute_query(query)
        }

    def get_category_depth(self, category_id: str) -> int:
        """Calculate depth of category in hierarchy"""
        depth = 0
        current = self.categories.get(category_id)
        while current and current['parent_category_id']:
            depth += 1
            current = self.categories.get(current['parent_category_id'])
        return depth

    def generate_category(self, parent_id: str = None) -> Dict[str, Any]:
        """Generate a new category"""
        category_types = [
            ('Electronics', 'Electronic devices and accessories'),
            ('Clothing', 'Fashion and apparel'),
            ('Home & Garden', 'Home improvement and decoration'),
            ('Books', 'Books and literature'),
            ('Sports', 'Sports equipment and accessories')
        ]
        name, description = random.choice(category_types)
        if parent_id:
            name = f"{name} - {self.fake.word().title()}"

        now = datetime.now()
        return {
            'category_id': str(uuid.uuid4()),
            'parent_category_id': parent_id,
            'name': name,
            'description': description,
            'created_at': now,
            'updated_at': now
        }

    def generate_event(self) -> Dict[str, Any]:
        """Generate category events"""
        max_depth = PRODUCT_PATTERNS['categories']['max_depth']

        if not self.categories or random.random() < 0.3:  # 30% chance for new root category
            if len(self.categories) < 5:  # Limit root categories
                category = self.generate_category()
                self.insert_record('product_categories', category)
                self.categories[category['category_id']] = category
                return {'type': 'category_create', 'category': category}
        else:
            # Select random parent category
            potential_parents = [
                cid for cid, cat in self.categories.items()
                if self.get_category_depth(cid) < max_depth - 1
            ]

            if potential_parents:
                parent_id = random.choice(potential_parents)
                category = self.generate_category(parent_id)
                self.insert_record('product_categories', category)
                self.categories[category['category_id']] = category
                return {'type': 'category_create', 'category': category}

    def run(self):
        """Run the category generator"""
        while True:
            try:
                event = self.generate_event()
                if event:
                    self.send_event('product_categories', event)
                time.sleep(random.uniform(60, 300))  # Categories change slowly
            except Exception as e:
                self.logger.error(f"Error in category generator: {str(e)}")
                time.sleep(5)