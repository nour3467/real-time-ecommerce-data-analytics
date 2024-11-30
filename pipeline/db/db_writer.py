import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class DatabaseWriter:
    def __init__(self, db_config: Dict[str, Any]):
        self.db_config = db_config
        self.conn = None
        self.connect()

    def connect(self):
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(
                **self.db_config,
                cursor_factory=RealDictCursor
            )
            self.conn.autocommit = True
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise

    def write_event(self, table: str, data: Dict[str, Any]):
        """Write event data to specified table"""
        try:
            with self.conn.cursor() as cur:
                columns = list(data.keys())
                values = list(data.values())
                placeholders = ["%s"] * len(columns)

                query = f"""
                    INSERT INTO {table} ({', '.join(columns)})
                    VALUES ({', '.join(placeholders)})
                    ON CONFLICT (id) DO UPDATE
                    SET {', '.join(f"{col} = EXCLUDED.{col}" for col in columns)}
                """

                cur.execute(query, values)
                logger.debug(f"Written to {table}: {data.get('id')}")

        except Exception as e:
            logger.error(f"Error writing to database: {e}")
            logger.error(f"Failed data: {data}")
            raise