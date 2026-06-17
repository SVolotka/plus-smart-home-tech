package ru.yandex.practicum.commerce.interactionApi.exception;

public class WarehouseServiceUnavailableException extends RuntimeException {
    public WarehouseServiceUnavailableException(String message) {
        super(message);
    }
}
