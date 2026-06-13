package ru.yandex.practicum.commerce.shoppintCart.service;

import ru.yandex.practicum.commerce.interactionApi.shoppingCart.dto.ShoppingCartDto;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface ShoppingCartService {
    ShoppingCartDto getShoppingCart(String username);

    ShoppingCartDto addProducts(String username, Map<UUID, Long> products);

    void deactivateCart(String username);

    ShoppingCartDto removeProducts(String username, List<UUID> productIds);

    ShoppingCartDto changeQuantity(String username, UUID productId, long newQuantity);
}