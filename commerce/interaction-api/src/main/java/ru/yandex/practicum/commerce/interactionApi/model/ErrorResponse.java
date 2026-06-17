package ru.yandex.practicum.commerce.interactionApi.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.http.HttpStatus;

import java.time.LocalDateTime;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ErrorResponse {
   private HttpStatus httpStatus;
   private String message;
   private String userMessage;
   private LocalDateTime timestamp;
}
