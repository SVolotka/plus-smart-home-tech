package ru.yandex.practicum.commerce.interactionApi.exception;

public class ProductInShoppingCartNotInWarehouseException extends RuntimeException {
    public ProductInShoppingCartNotInWarehouseException(String message) {
        super(message);
    }
}
