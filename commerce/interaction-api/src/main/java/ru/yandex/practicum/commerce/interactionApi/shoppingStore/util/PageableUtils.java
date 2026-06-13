package ru.yandex.practicum.commerce.interactionApi.shoppingStore.util;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import ru.yandex.practicum.commerce.interactionApi.shoppingStore.dto.PageProductDto;
import ru.yandex.practicum.commerce.interactionApi.shoppingStore.dto.PageableObject;
import ru.yandex.practicum.commerce.interactionApi.shoppingStore.dto.ProductDto;
import ru.yandex.practicum.commerce.interactionApi.shoppingStore.dto.SortObject;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class PageableUtils {

    private PageableUtils() {
    }

    public static PageableObject createPageableObject(int page, int size, String[] sort) {
        List<SortObject> sorts = Arrays.stream(sort)
                .map(PageableUtils::parseSort)
                .collect(Collectors.toList());

        return PageableObject.builder()
                .pageNumber(page)
                .pageSize(size)
                .sort(sorts)
                .unpaged(false)
                .paged(true)
                .offset((long) page * size)
                .build();
    }

    public static Pageable toSpringPageable(PageableObject pageableObject) {
        List<Sort.Order> orders = pageableObject.getSort().stream()
                .map(sortObject -> new Sort.Order(
                        sortObject.isAscending() ? Sort.Direction.ASC : Sort.Direction.DESC,
                        sortObject.getProperty()))
                .collect(Collectors.toList());

        return PageRequest.of(
                pageableObject.getPageNumber(),
                pageableObject.getPageSize(),
                Sort.by(orders)
        );
    }

    public static PageProductDto toPageProductDto(Page<ProductDto> page, PageableObject requestedPageable) {
        List<SortObject> sorts = page.getSort().stream()
                .map(order -> SortObject.builder()
                        .direction(order.getDirection().name())
                        .property(order.getProperty())
                        .ascending(order.isAscending())
                        .ignoreCase(order.isIgnoreCase())
                        .nullHandling(order.getNullHandling().name())
                        .build())
                .collect(Collectors.toList());

        PageableObject pageable = PageableObject.builder()
                .pageNumber(requestedPageable.getPageNumber())
                .pageSize(requestedPageable.getPageSize())
                .sort(sorts)
                .unpaged(requestedPageable.isUnpaged())
                .paged(requestedPageable.isPaged())
                .offset(requestedPageable.getOffset())
                .build();

        return PageProductDto.builder()
                .totalElements(page.getTotalElements())
                .totalPages(page.getTotalPages())
                .first(page.isFirst())
                .last(page.isLast())
                .size(page.getSize())
                .content(page.getContent())
                .number(page.getNumber())
                .sort(sorts)
                .numberOfElements(page.getNumberOfElements())
                .pageable(pageable)
                .empty(page.isEmpty())
                .build();
    }

    private static SortObject parseSort(String sortParam) {
        String[] parts = sortParam.split(",");
        String property = parts[0].trim();
        boolean ascending = true;
        if (parts.length > 1) {
            ascending = !parts[1].trim().equalsIgnoreCase("desc");
        }
        return SortObject.builder()
                .property(property)
                .direction(ascending ? "ASC" : "DESC")
                .ascending(ascending)
                .ignoreCase(false)
                .nullHandling("NATIVE")
                .build();
    }
}
