import os
import psycopg2
from dotenv import load_dotenv

def load_env():
    """Load environment variables from the appropriate .env file"""
    env_path = os.path.join(os.path.dirname(__file__), 'config', '.env.development')
    if not os.path.exists(env_path):
        raise FileNotFoundError(f"Environment file not found at {env_path}")
    load_dotenv(env_path)

def get_db_config():
    """Get database configuration from environment variables"""
    return {
        'dbname': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT')
    }

def run_migrations():
    # Load environment variables
    load_env()

    # Get database configuration
    db_config = get_db_config()

    # Connect to database
    conn = psycopg2.connect(**db_config)
    conn.autocommit = True
    cur = conn.cursor()

    # Get all SQL files
    migrations_path = os.path.join(os.path.dirname(__file__), 'migrations')
    migration_files = sorted(os.listdir(migrations_path))

    # Run each file
    for file in migration_files:
        if file.endswith('.sql'):
            print(f"Running migration: {file}")
            with open(os.path.join(migrations_path, file), 'r') as f:
                cur.execute(f.read())
            print(f"Completed migration: {file}")

    cur.close()
    conn.close()

if __name__ == "__main__":
    run_migrations()