package ru.yandex.practicum.commerce.interactionApi.shoppingStore.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class PageProductDto {
    private long totalElements;
    private int totalPages;
    private boolean first;
    private boolean last;
    private int size;
    private List<ProductDto> content;
    private int number;
    private List<SortObject> sort;
    private int numberOfElements;
    private PageableObject pageable;
    private boolean empty;
}
