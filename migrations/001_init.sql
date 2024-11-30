-- Check if we already ran this migration
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_tables WHERE tablename = 'migrations') THEN
        -- Create migrations table first
        CREATE TABLE migrations (
            id SERIAL PRIMARY KEY,
            migration_name VARCHAR(255),
            executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Create extensions
        CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
        CREATE EXTENSION IF NOT EXISTS "citext";

        -- Create update timestamp function
        CREATE OR REPLACE FUNCTION trigger_set_timestamp()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $$ language plpgsql;

        -- Create tables
        -- Users
        CREATE TABLE users (
            user_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            email VARCHAR(255) UNIQUE NOT NULL,
            password_hash VARCHAR(255) NOT NULL,
            first_name VARCHAR(50),
            last_name VARCHAR(50),
            registration_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            last_login TIMESTAMP WITH TIME ZONE,
            is_active BOOLEAN DEFAULT TRUE,
            preferences JSONB,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE
        );

        CREATE TABLE user_demographics (
            demographic_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            user_id UUID REFERENCES users(user_id),
            age_range VARCHAR(20),
            gender VARCHAR(20),
            income_bracket VARCHAR(20),
            occupation VARCHAR(100),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE
        );

        CREATE TABLE user_addresses (
            address_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            user_id UUID REFERENCES users(user_id),
            address_type VARCHAR(20),
            street_address TEXT,
            city VARCHAR(100),
            state VARCHAR(50),
            country VARCHAR(50),
            postal_code VARCHAR(20),
            is_default BOOLEAN,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE
        );

        -- Sessions
        CREATE TABLE sessions (
            session_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            user_id UUID REFERENCES users(user_id),
            timestamp_start TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            timestamp_end TIMESTAMP WITH TIME ZONE,
            device_type VARCHAR(50),
            os_info VARCHAR(100),
            browser_info VARCHAR(100),
            ip_address INET,
            referral_source VARCHAR(255),
            utm_source VARCHAR(50),
            utm_medium VARCHAR(50),
            utm_campaign VARCHAR(50),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );

        -- Products
        CREATE TABLE product_categories (
            category_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            parent_category_id UUID REFERENCES product_categories(category_id),
            name VARCHAR(100),
            description TEXT,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE
        );

        CREATE TABLE products (
            product_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            sku VARCHAR(50) UNIQUE,
            name VARCHAR(255),
            description TEXT,
            category_id UUID REFERENCES product_categories(category_id),
            price DECIMAL(10,2),
            cost DECIMAL(10,2),
            stock_quantity INTEGER,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE
        );

        -- Product Interactions
        CREATE TABLE product_views (
            view_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            session_id UUID REFERENCES sessions(session_id),
            product_id UUID REFERENCES products(product_id),
            view_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            view_duration INTERVAL,
            source_page VARCHAR(255)
        );

        CREATE TABLE wishlists (
            wishlist_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            user_id UUID REFERENCES users(user_id),
            product_id UUID REFERENCES products(product_id),
            added_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            removed_timestamp TIMESTAMP WITH TIME ZONE,
            notes TEXT
        );

        -- Shopping Cart
        CREATE TABLE carts (
            cart_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            user_id UUID REFERENCES users(user_id),
            session_id UUID REFERENCES sessions(session_id),
            status VARCHAR(20),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE
        );

        CREATE TABLE cart_items (
            cart_item_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            cart_id UUID REFERENCES carts(cart_id),
            product_id UUID REFERENCES products(product_id),
            quantity INTEGER,
            added_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            removed_timestamp TIMESTAMP WITH TIME ZONE,
            unit_price DECIMAL(10,2)
        );

        -- Orders
        CREATE TABLE orders (
            order_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            user_id UUID REFERENCES users(user_id),
            cart_id UUID REFERENCES carts(cart_id),
            status VARCHAR(20),
            total_amount DECIMAL(10,2),
            tax_amount DECIMAL(10,2),
            shipping_amount DECIMAL(10,2),
            discount_amount DECIMAL(10,2),
            payment_method VARCHAR(50),
            delivery_method VARCHAR(50),
            billing_address_id UUID REFERENCES user_addresses(address_id),
            shipping_address_id UUID REFERENCES user_addresses(address_id),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE
        );

        CREATE TABLE order_items (
            order_item_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            order_id UUID REFERENCES orders(order_id),
            product_id UUID REFERENCES products(product_id),
            quantity INTEGER,
            unit_price DECIMAL(10,2),
            discount_amount DECIMAL(10,2),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );

        -- Support
        CREATE TABLE support_tickets (
            ticket_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            user_id UUID REFERENCES users(user_id),
            order_id UUID REFERENCES orders(order_id),
            issue_type VARCHAR(50),
            priority VARCHAR(20),
            status VARCHAR(20),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            resolved_at TIMESTAMP WITH TIME ZONE,
            satisfaction_score INTEGER
        );

        CREATE TABLE ticket_messages (
            message_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            ticket_id UUID REFERENCES support_tickets(ticket_id),
            sender_type VARCHAR(20),
            message_text TEXT,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );

        -- Create triggers for updated_at
        CREATE TRIGGER set_timestamp_users
            BEFORE UPDATE ON users
            FOR EACH ROW
            EXECUTE FUNCTION trigger_set_timestamp();

        CREATE TRIGGER set_timestamp_user_demographics
            BEFORE UPDATE ON user_demographics
            FOR EACH ROW
            EXECUTE FUNCTION trigger_set_timestamp();

        CREATE TRIGGER set_timestamp_user_addresses
            BEFORE UPDATE ON user_addresses
            FOR EACH ROW
            EXECUTE FUNCTION trigger_set_timestamp();

        CREATE TRIGGER set_timestamp_products
            BEFORE UPDATE ON products
            FOR EACH ROW
            EXECUTE FUNCTION trigger_set_timestamp();

        CREATE TRIGGER set_timestamp_product_categories
            BEFORE UPDATE ON product_categories
            FOR EACH ROW
            EXECUTE FUNCTION trigger_set_timestamp();

        CREATE TRIGGER set_timestamp_carts
            BEFORE UPDATE ON carts
            FOR EACH ROW
            EXECUTE FUNCTION trigger_set_timestamp();

        CREATE TRIGGER set_timestamp_orders
            BEFORE UPDATE ON orders
            FOR EACH ROW
            EXECUTE FUNCTION trigger_set_timestamp();

        -- Create indexes
        CREATE INDEX idx_users_email ON users(email);
        CREATE INDEX idx_sessions_user_timestamp ON sessions(user_id, timestamp_start);
        CREATE INDEX idx_product_views_session ON product_views(session_id, view_timestamp);
        CREATE INDEX idx_cart_items_cart ON cart_items(cart_id, added_timestamp);
        CREATE INDEX idx_orders_user ON orders(user_id, created_at);
        CREATE INDEX idx_support_tickets_user ON support_tickets(user_id, created_at);

        -- Record this migration
        INSERT INTO migrations (migration_name) VALUES ('001_init');
    END IF;
END $$;