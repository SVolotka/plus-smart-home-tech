package ru.yandex.practicum.commerce.warehouse.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.commerce.interactionApi.feignClient.WarehouseClient;
import ru.yandex.practicum.commerce.interactionApi.shoppingCart.dto.ShoppingCartDto;
import ru.yandex.practicum.commerce.interactionApi.warehouse.dto.AddProductToWarehouseRequest;
import ru.yandex.practicum.commerce.interactionApi.warehouse.dto.AddressDto;
import ru.yandex.practicum.commerce.interactionApi.warehouse.dto.BookedProductsDto;
import ru.yandex.practicum.commerce.interactionApi.warehouse.dto.NewProductInWarehouseRequest;
import ru.yandex.practicum.commerce.warehouse.service.WarehouseService;

@RestController
@RequestMapping("/api/v1/warehouse")
@RequiredArgsConstructor
@Slf4j
public class WarehouseController implements WarehouseClient {
    private final WarehouseService warehouseService;

    @Override
    @PutMapping
    public void newProductInWarehouse(@Valid @RequestBody NewProductInWarehouseRequest request) {
        log.info("Запрос на добавление нового товара на склад: {}", request.getProductId());
        warehouseService.newProductInWarehouse(request);
    }

    @Override
    @PostMapping("/check")
    public BookedProductsDto checkProductQuantityEnoughForShoppingCart(@RequestBody ShoppingCartDto cartDto) {
        log.info("Проверка наличия товаров для корзины {}", cartDto.getShoppingCartId());
        return warehouseService.checkProductQuantityEnoughForShoppingCart(cartDto);
    }

    @Override
    @PostMapping("/add")
    public void addProductToWarehouse(@Valid @RequestBody AddProductToWarehouseRequest request) {
        log.info("Пополнение склада: товар {}, количество {}", request.getProductId(), request.getQuantity());
        warehouseService.addProductToWarehouse(request);
    }

    @Override
    @GetMapping("/address")
    public AddressDto getWarehouseAddress() {
        log.debug("Запрос адреса склада");
        return warehouseService.getWarehouseAddress();
    }
}
