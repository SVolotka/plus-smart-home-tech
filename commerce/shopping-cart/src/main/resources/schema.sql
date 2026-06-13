CREATE TABLE IF NOT EXISTS shopping_carts (
    shopping_cart_id UUID PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    state VARCHAR(20) NOT NULL
);

CREATE TABLE IF NOT EXISTS cart_products (
    shopping_cart_id UUID NOT NULL,
    product_id UUID NOT NULL,
    quantity BIGINT NOT NULL,
    PRIMARY KEY (shopping_cart_id, product_id),
    FOREIGN KEY (shopping_cart_id) REFERENCES shopping_carts(shopping_cart_id)
);