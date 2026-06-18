CREATE TABLE IF NOT EXISTS deliveries (
    delivery_id UUID PRIMARY KEY,
    order_id UUID NOT NULL,
    from_country VARCHAR(100),
    from_city VARCHAR(100),
    from_street VARCHAR(100),
    from_house VARCHAR(100),
    from_flat VARCHAR(100),
    to_country VARCHAR(100),
    to_city VARCHAR(100),
    to_street VARCHAR(100),
    to_house VARCHAR(100),
    to_flat VARCHAR(100),
    delivery_state VARCHAR(20) NOT NULL DEFAULT 'CREATED'
);