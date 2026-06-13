package ru.yandex.practicum.commerce.shoppingStore.service;

import ru.yandex.practicum.commerce.interactionApi.shoppingStore.dto.PageProductDto;
import ru.yandex.practicum.commerce.interactionApi.shoppingStore.dto.PageableObject;
import ru.yandex.practicum.commerce.interactionApi.shoppingStore.dto.ProductDto;
import ru.yandex.practicum.commerce.interactionApi.shoppingStore.enums.ProductCategory;
import ru.yandex.practicum.commerce.interactionApi.shoppingStore.enums.QuantityState;

import java.util.UUID;

public interface ProductService {
    PageProductDto getProducts(ProductCategory category, PageableObject pageableObject);

    ProductDto getProduct(UUID productId);

    ProductDto createProduct(ProductDto dto);

    ProductDto updateProduct(ProductDto dto);

    boolean removeProduct(UUID productId);

    boolean setQuantityState(UUID productId, QuantityState state);
}