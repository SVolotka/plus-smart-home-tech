package ru.yandex.practicum.commerce.delivery.exception;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import ru.yandex.practicum.commerce.interactionApi.exception.NoDeliveryFoundException;
import ru.yandex.practicum.commerce.interactionApi.model.ErrorResponse;

import java.time.LocalDateTime;

@RestControllerAdvice
public class GlobalExceptionHandler {
    @ExceptionHandler(NoDeliveryFoundException.class)
    public ResponseEntity<ErrorResponse> handleNoDelivery(NoDeliveryFoundException ex) {
        ErrorResponse error = ErrorResponse.builder()
                .httpStatus(HttpStatus.NOT_FOUND)
                .message(ex.getMessage())
                .userMessage("Доставка не найдена")
                .timestamp(LocalDateTime.now())
                .build();
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(error);
    }
}
