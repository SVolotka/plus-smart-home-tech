package ru.yandex.practicum.commerce.shoppintCart.exception;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import ru.yandex.practicum.commerce.interactionApi.exception.NoProductsInShoppingCartException;
import ru.yandex.practicum.commerce.interactionApi.exception.NotAuthorizedUserException;
import ru.yandex.practicum.commerce.interactionApi.exception.WarehouseServiceUnavailableException;
import ru.yandex.practicum.commerce.interactionApi.model.ErrorResponse;

import java.time.LocalDateTime;
import java.util.NoSuchElementException;

@RestControllerAdvice
public class GlobalExceptionHandler {
    @ExceptionHandler(NotAuthorizedUserException.class)
    public ResponseEntity<ErrorResponse> handleNotAuthorized(NotAuthorizedUserException ex) {
        ErrorResponse error = ErrorResponse.builder()
                .httpStatus(HttpStatus.UNAUTHORIZED)
                .message(ex.getMessage())
                .userMessage("Пользователь не авторизован")
                .timestamp(LocalDateTime.now())
                .build();
        return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(error);
    }

    @ExceptionHandler(NoProductsInShoppingCartException.class)
    public ResponseEntity<ErrorResponse> handleNoProducts(NoProductsInShoppingCartException ex) {
        ErrorResponse error = ErrorResponse.builder()
                .httpStatus(HttpStatus.BAD_REQUEST)
                .message(ex.getMessage())
                .userMessage("Товары не найдены в корзине")
                .timestamp(LocalDateTime.now())
                .build();
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(error);
    }

    @ExceptionHandler(NoSuchElementException.class)
    public ResponseEntity<ErrorResponse> handleNotFound(NoSuchElementException ex) {
        ErrorResponse error = ErrorResponse.builder()
                .httpStatus(HttpStatus.NOT_FOUND)
                .message(ex.getMessage())
                .userMessage("Ресурс не найден")
                .timestamp(LocalDateTime.now())
                .build();
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(error);
    }

    @ExceptionHandler(WarehouseServiceUnavailableException.class)
    public ResponseEntity<ErrorResponse> handleWarehouseUnavailable(WarehouseServiceUnavailableException ex) {
        ErrorResponse error = ErrorResponse.builder()
                .httpStatus(HttpStatus.SERVICE_UNAVAILABLE)
                .message(ex.getMessage())
                .userMessage("Сервис склада временно недоступен")
                .timestamp(LocalDateTime.now())
                .build();
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(error);
    }
}
