package ru.yandex.practicum.commerce.order.exception;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import ru.yandex.practicum.commerce.interactionApi.exception.NoOrderFoundException;
import ru.yandex.practicum.commerce.interactionApi.exception.NoSpecifiedProductInWarehouseException;
import ru.yandex.practicum.commerce.interactionApi.exception.NotAuthorizedUserException;
import ru.yandex.practicum.commerce.interactionApi.model.ErrorResponse;

import java.time.LocalDateTime;

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

    @ExceptionHandler(NoSpecifiedProductInWarehouseException.class)
    public ResponseEntity<ErrorResponse> handleNoProduct(NoSpecifiedProductInWarehouseException ex) {
        ErrorResponse error = ErrorResponse.builder()
                .httpStatus(HttpStatus.BAD_REQUEST)
                .message(ex.getMessage())
                .userMessage("Товар не найден на складе")
                .timestamp(LocalDateTime.now())
                .build();
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(error);
    }

    @ExceptionHandler(NoOrderFoundException.class)
    public ResponseEntity<ErrorResponse> handleNoOrder(NoOrderFoundException ex) {
        ErrorResponse error = ErrorResponse.builder()
                .httpStatus(HttpStatus.BAD_REQUEST)
                .message(ex.getMessage())
                .userMessage("Заказ не найден")
                .timestamp(LocalDateTime.now())
                .build();
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(error);
    }
}
