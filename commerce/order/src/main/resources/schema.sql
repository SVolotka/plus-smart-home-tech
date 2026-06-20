CREATE TABLE IF NOT EXISTS orders (
    order_id UUID PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    shopping_cart_id UUID NOT NULL,
    payment_id UUID,
    delivery_id UUID,
    state VARCHAR(20) NOT NULL,
    delivery_weight DOUBLE PRECISION,
    delivery_volume DOUBLE PRECISION,
    fragile BOOLEAN,
    total_price DOUBLE PRECISION,
    delivery_price DOUBLE PRECISION,
    product_price DOUBLE PRECISION,
    country VARCHAR(100),
    city VARCHAR(100),
    street VARCHAR(100),
    house VARCHAR(100),
    flat VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS order_products (
    order_id UUID NOT NULL,
    product_id UUID NOT NULL,
    quantity BIGINT NOT NULL,
    PRIMARY KEY (order_id, product_id),
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);