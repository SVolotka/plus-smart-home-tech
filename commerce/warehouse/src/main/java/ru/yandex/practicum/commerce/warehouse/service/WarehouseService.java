package ru.yandex.practicum.commerce.warehouse.service;

import ru.yandex.practicum.commerce.interactionApi.shoppingCart.dto.ShoppingCartDto;
import ru.yandex.practicum.commerce.interactionApi.warehouse.dto.AddProductToWarehouseRequest;
import ru.yandex.practicum.commerce.interactionApi.warehouse.dto.AddressDto;
import ru.yandex.practicum.commerce.interactionApi.warehouse.dto.BookedProductsDto;
import ru.yandex.practicum.commerce.interactionApi.warehouse.dto.NewProductInWarehouseRequest;

public interface WarehouseService {
    void newProductInWarehouse(NewProductInWarehouseRequest request);

    void addProductToWarehouse(AddProductToWarehouseRequest request);

    BookedProductsDto checkProductQuantityEnoughForShoppingCart(ShoppingCartDto cartDto);

    AddressDto getWarehouseAddress();
}
