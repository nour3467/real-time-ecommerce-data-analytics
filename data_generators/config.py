# data_generators/config.py
CONFIG = {
    'local': {
        'kafka_bootstrap_servers': 'localhost:9092',
        'postgres_conn': 'postgresql://admin:password@localhost:5432/ecommerce'
    },
    'production': {
        'kafka_bootstrap_servers': 'your-aws-msk-brokers',
        'postgres_conn': 'your-rds-connection-string'
    }
}