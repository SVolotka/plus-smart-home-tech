package ru.yandex.practicum.commerce.warehouse.exception;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import ru.yandex.practicum.commerce.interactionApi.exception.NoSpecifiedProductInWarehouseException;
import ru.yandex.practicum.commerce.interactionApi.exception.ProductInShoppingCartLowQuantityInWarehouse;
import ru.yandex.practicum.commerce.interactionApi.exception.ProductInShoppingCartNotInWarehouseException;
import ru.yandex.practicum.commerce.interactionApi.exception.SpecifiedProductAlreadyInWarehouseException;
import ru.yandex.practicum.commerce.interactionApi.model.ErrorResponse;

import java.time.LocalDateTime;

@RestControllerAdvice
public class GlobalExceptionHandler {
    @ExceptionHandler(SpecifiedProductAlreadyInWarehouseException.class)
    public ResponseEntity<ErrorResponse> handleAlreadyInWarehouse(SpecifiedProductAlreadyInWarehouseException ex) {
        ErrorResponse error = ErrorResponse.builder()
                .httpStatus(HttpStatus.BAD_REQUEST)
                .message(ex.getMessage())
                .userMessage("Товар уже зарегистрирован на складе")
                .timestamp(LocalDateTime.now())
                .build();
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(error);
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

    @ExceptionHandler(ProductInShoppingCartLowQuantityInWarehouse.class)
    public ResponseEntity<ErrorResponse> handleLowQuantity(ProductInShoppingCartLowQuantityInWarehouse ex) {
        ErrorResponse error = ErrorResponse.builder()
                .httpStatus(HttpStatus.BAD_REQUEST)
                .message(ex.getMessage())
                .userMessage("Товаров недостаточно на складе")
                .timestamp(LocalDateTime.now())
                .build();
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(error);
    }

    @ExceptionHandler(ProductInShoppingCartNotInWarehouseException.class)
    public ResponseEntity<ErrorResponse> handleNotInWarehouse(ProductInShoppingCartNotInWarehouseException ex) {
        ErrorResponse error = ErrorResponse.builder()
                .httpStatus(HttpStatus.BAD_REQUEST)
                .message(ex.getMessage())
                .userMessage("Товар отсутствует на складе")
                .timestamp(LocalDateTime.now())
                .build();
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(error);
    }
}
