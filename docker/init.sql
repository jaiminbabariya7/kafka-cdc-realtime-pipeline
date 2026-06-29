-- Enable logical replication and create the e-commerce schema

CREATE TABLE IF NOT EXISTS customers (
    customer_id      UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    first_name       VARCHAR(100),
    last_name        VARCHAR(100),
    email            VARCHAR(255) UNIQUE NOT NULL,
    country          VARCHAR(100),
    city             VARCHAR(100),
    registration_date DATE         DEFAULT CURRENT_DATE,
    is_active        BOOLEAN      DEFAULT TRUE,
    updated_at       TIMESTAMPTZ  DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS products (
    product_id   UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    product_name VARCHAR(255) NOT NULL,
    category     VARCHAR(100),
    unit_price   NUMERIC(10,2),
    unit_cost    NUMERIC(10,2),
    stock_qty    INT          DEFAULT 0,
    updated_at   TIMESTAMPTZ  DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS orders (
    order_id    UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id UUID         REFERENCES customers(customer_id),
    order_date  DATE         DEFAULT CURRENT_DATE,
    status      VARCHAR(50)  DEFAULT 'pending',
    channel     VARCHAR(50),
    net_revenue NUMERIC(10,2),
    created_at  TIMESTAMPTZ  DEFAULT NOW(),
    updated_at  TIMESTAMPTZ  DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS order_items (
    item_id     UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id    UUID         REFERENCES orders(order_id),
    product_id  UUID         REFERENCES products(product_id),
    quantity    INT          NOT NULL CHECK (quantity > 0),
    unit_price  NUMERIC(10,2),
    discount_pct NUMERIC(5,4) DEFAULT 0,
    created_at  TIMESTAMPTZ  DEFAULT NOW()
);

-- Publication for Debezium CDC
CREATE PUBLICATION debezium_pub FOR TABLE customers, products, orders, order_items;