import os
from typing import Dict, Any

# Database Configuration
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", 5432),
    "database": os.getenv("DB_NAME", "ecommerce"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "admin_password"),
}

# Kafka Configuration
KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "topics": {
        "users": "users",
        "user_demographics": "user_demographics",
        "user_addresses": "user_addresses",
        "sessions": "sessions",
        "products": "products",
        "product_categories": "product_categories",
        "product_views": "product_views",
        "wishlists": "wishlists",
        "carts": "carts",
        "cart_items": "cart_items",
        "orders": "orders",
        "order_items": "order_items",
        "support_tickets": "support_tickets",
        "ticket_messages": "ticket_messages",
    },
}

# User Related Patterns
USER_PATTERNS = {
    "demographics": {
        "age_ranges": ["18-24", "25-34", "35-44", "45-54", "55-64", "65+"],
        "genders": ["M", "F", "Other"],
        "income_brackets": ["0-25k", "25k-50k", "50k-75k", "75k-100k", "100k+"],
    },
    "address": {
        "types": ["shipping", "billing"],
        "multiple_addresses_probability": 0.3,
        "default_probability": 0.8,
    },
}

# Session Patterns
SESSION_PATTERNS = {
    "duration": {"min_minutes": 1, "max_minutes": 120, "typical_duration": 15},
    "device_types": {"mobile": 0.65, "desktop": 0.25, "tablet": 0.10},
    "browser_distribution": {
        "Chrome": 0.45,
        "Safari": 0.25,
        "Firefox": 0.15,
        "Edge": 0.10,
        "Other": 0.05,
    },
}

# Product Related Patterns
PRODUCT_PATTERNS = {
    "categories": {"max_depth": 3, "max_children": 5, "probability_of_parent": 0.7},
    "products": {
        "min_per_category": 5,
        "max_per_category": 50,
        "price_ranges": {
            "budget": (10, 50),
            "mid_range": (51, 200),
            "premium": (201, 1000),
        },
        "stock_levels": {
            "out_of_stock": 0.05,
            "low_stock": (1, 10),
            "normal_stock": (11, 100),
            "high_stock": (101, 1000),
        },
    },
}

# Cart and Order Patterns
CART_PATTERNS = {
    "status_distribution": {"active": 0.2, "abandoned": 0.7, "converted": 0.1},
    "items": {"min": 1, "max": 10, "average": 2.5},
}

ORDER_PATTERNS = {
    "status_flow": {
        "pending": 0.2,
        "processing": 0.3,
        "shipped": 0.3,
        "delivered": 0.15,
        "cancelled": 0.05,
    },
    "payment_methods": {
        "credit_card": 0.6,
        "debit_card": 0.2,
        "paypal": 0.15,
        "other": 0.05,
    },
    "delivery_methods": {"standard": 0.7, "express": 0.2, "next_day": 0.1},
}

# Support Ticket Patterns
SUPPORT_PATTERNS = {
    "issue_types": {
        "order_status": 0.3,
        "delivery_delay": 0.2,
        "product_issue": 0.2,
        "payment_issue": 0.15,
        "return_refund": 0.15,
    },
    "priority_distribution": {"low": 0.4, "medium": 0.4, "high": 0.15, "urgent": 0.05},
    "status_flow": {"open": 0.2, "in_progress": 0.3, "resolved": 0.4, "closed": 0.1},
    "satisfaction_score_distribution": {1: 0.05, 2: 0.10, 3: 0.20, 4: 0.40, 5: 0.25},
}

# General Business Patterns
BUSINESS_HOURS = {
    "peak_hours": [(9, 12), (13, 17), (19, 22)],
    "weekend_multiplier": 1.5,
    "holiday_multiplier": 2.0,
}

# Error Simulation
ERROR_PATTERNS = {
    "network_failure_rate": 0.02,
    "invalid_data_rate": 0.01,
    "missing_data_rate": 0.03,
}
