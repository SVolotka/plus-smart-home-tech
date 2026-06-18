package ru.yandex.practicum.commerce.interactionApi.feignClient;

import jakarta.validation.Valid;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import ru.yandex.practicum.commerce.interactionApi.shoppingCart.dto.ShoppingCartDto;
import ru.yandex.practicum.commerce.interactionApi.warehouse.dto.AddProductToWarehouseRequest;
import ru.yandex.practicum.commerce.interactionApi.warehouse.dto.AddressDto;
import ru.yandex.practicum.commerce.interactionApi.warehouse.dto.AssemblyProductsForOrderRequest;
import ru.yandex.practicum.commerce.interactionApi.warehouse.dto.BookedProductsDto;
import ru.yandex.practicum.commerce.interactionApi.warehouse.dto.NewProductInWarehouseRequest;
import ru.yandex.practicum.commerce.interactionApi.warehouse.dto.ShippedToDeliveryRequest;

import java.util.Map;
import java.util.UUID;

@FeignClient(name = "warehouse")
public interface WarehouseClient {

    @PutMapping("/api/v1/warehouse")
    void newProductInWarehouse(@Valid @RequestBody NewProductInWarehouseRequest request);

    @PostMapping("/api/v1/warehouse/check")
    BookedProductsDto checkProductQuantityEnoughForShoppingCart(@RequestBody ShoppingCartDto cartDto);

    @PostMapping("/api/v1/warehouse/add")
    void addProductToWarehouse(@Valid @RequestBody AddProductToWarehouseRequest request);

    @GetMapping("/api/v1/warehouse/address")
    AddressDto getWarehouseAddress();

    @PostMapping("/api/v1/warehouse/assembly")
    BookedProductsDto assemblyProductsForOrder(@Valid @RequestBody AssemblyProductsForOrderRequest request);

    @PostMapping("/api/v1/warehouse/shipped")
    void shippedToDelivery(@Valid @RequestBody ShippedToDeliveryRequest request);

    @PostMapping("/api/v1/warehouse/return")
    void acceptReturn(@RequestBody Map<UUID, Long> products);
}
