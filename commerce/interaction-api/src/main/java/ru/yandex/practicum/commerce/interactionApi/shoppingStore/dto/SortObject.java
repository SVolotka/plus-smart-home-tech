package ru.yandex.practicum.commerce.interactionApi.shoppingStore.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class SortObject {
    private String direction;
    private String nullHandling;
    private boolean ascending;
    private String property;
    private boolean ignoreCase;
}
