import logging
import time
import signal
import sys
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any
from .config import DB_CONFIG, KAFKA_CONFIG
from .user_generator import UserGenerator
from .session_generator import SessionGenerator
from .product_category_generator import ProductCategoryGenerator
from .product_generator import ProductGenerator
from .product_view_generator import ProductViewGenerator
from .wishlist_generator import WishlistGenerator
from .cart_generator import CartGenerator
from .order_generator import OrderGenerator
from .support_ticket_generator import SupportTicketGenerator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataGeneratorOrchestrator:
    def __init__(self):
        self.is_running = True
        self.setup_signal_handlers()

        # Initialize generators in dependency order
        self.generators = {
            'user': UserGenerator(DB_CONFIG, KAFKA_CONFIG),
            'product_category': ProductCategoryGenerator(DB_CONFIG, KAFKA_CONFIG),
            'product': ProductGenerator(DB_CONFIG, KAFKA_CONFIG),
            'session': SessionGenerator(DB_CONFIG, KAFKA_CONFIG),
            'product_view': ProductViewGenerator(DB_CONFIG, KAFKA_CONFIG),
            'wishlist': WishlistGenerator(DB_CONFIG, KAFKA_CONFIG),
            'cart': CartGenerator(DB_CONFIG, KAFKA_CONFIG),
            'order': OrderGenerator(DB_CONFIG, KAFKA_CONFIG),
            'support_ticket': SupportTicketGenerator(DB_CONFIG, KAFKA_CONFIG)
        }

        # Define generator dependencies
        self.dependencies = {
            'product': ['product_category'],
            'product_view': ['product', 'session'],
            'wishlist': ['user', 'product'],
            'cart': ['user', 'session', 'product'],
            'order': ['cart'],
            'support_ticket': ['user', 'order']
        }

    def setup_signal_handlers(self):
        """Setup graceful shutdown handlers"""
        signal.signal(signal.SIGINT, self.handle_shutdown)
        signal.signal(signal.SIGTERM, self.handle_shutdown)

    def handle_shutdown(self, signum, frame):
        """Handle shutdown signals"""
        logger.info("Shutdown signal received. Stopping generators...")
        self.is_running = False

    def check_dependencies(self, generator_name: str) -> bool:
        """Check if dependencies are ready for a generator"""
        if generator_name not in self.dependencies:
            return True

        for dependency in self.dependencies[generator_name]:
            query = f"SELECT 1 FROM {dependency} LIMIT 1"
            try:
                with self.generators[generator_name].db_conn.cursor() as cur:
                    cur.execute(query)
                    if not cur.fetchone():
                        logger.warning(f"Dependency {dependency} not ready for {generator_name}")
                        return False
            except Exception as e:
                logger.error(f"Error checking dependency {dependency}: {str(e)}")
                return False
        return True

    def run_generator(self, name: str, generator: Any):
        """Run a single generator with dependency checking"""
        logger.info(f"Starting {name} generator")

        # Wait for dependencies
        while self.is_running and not self.check_dependencies(name):
            logger.info(f"Waiting for {name} dependencies...")
            time.sleep(10)

        if not self.is_running:
            return

        try:
            generator.run()
        except Exception as e:
            logger.error(f"Error in {name} generator: {str(e)}")
        finally:
            try:
                generator.cleanup()
            except Exception as e:
                logger.error(f"Error cleaning up {name} generator: {str(e)}")

    def run(self):
        """Run all generators with proper orchestration"""
        logger.info("Starting data generation orchestration")

        with ThreadPoolExecutor(max_workers=len(self.generators)) as executor:
            futures = {}

            # Start generators in dependency order
            for name, generator in self.generators.items():
                if name not in self.dependencies:
                    futures[executor.submit(self.run_generator, name, generator)] = name
                    time.sleep(2)  # Stagger starts

            # Start dependent generators
            for name, generator in self.generators.items():
                if name in self.dependencies:
                    futures[executor.submit(self.run_generator, name, generator)] = name
                    time.sleep(2)  # Stagger starts

            # Monitor futures
            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Generator {futures[future]} failed: {str(e)}")

    def cleanup(self):
        """Cleanup all generators"""
        logger.info("Cleaning up generators...")
        for name, generator in self.generators.items():
            try:
                generator.cleanup()
            except Exception as e:
                logger.error(f"Error cleaning up {name} generator: {str(e)}")

if __name__ == "__main__":
    orchestrator = DataGeneratorOrchestrator()
    try:
        orchestrator.run()
    except Exception as e:
        logger.error(f"Orchestrator failed: {str(e)}")
    finally:
        orchestrator.cleanup()
        sys.exit(0)