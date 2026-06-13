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
public class PageableObject {
    private long offset;
    private List<SortObject> sort;
    private boolean unpaged;
    private boolean paged;
    private int pageNumber;
    private int pageSize;
}
