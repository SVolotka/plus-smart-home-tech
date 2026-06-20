package ru.yandex.practicum.commerce.interactionApi.exception;

public class NoDeliveryFoundException extends RuntimeException {
    public NoDeliveryFoundException(String message) {
        super(message);
    }
}
