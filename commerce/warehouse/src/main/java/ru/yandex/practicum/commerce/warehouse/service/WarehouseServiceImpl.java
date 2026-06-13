package ru.yandex.practicum.commerce.warehouse.service;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.interactionApi.exception.NoSpecifiedProductInWarehouseException;
import ru.yandex.practicum.commerce.interactionApi.exception.ProductInShoppingCartLowQuantityInWarehouse;
import ru.yandex.practicum.commerce.interactionApi.exception.SpecifiedProductAlreadyInWarehouseException;
import ru.yandex.practicum.commerce.interactionApi.shoppingCart.dto.ShoppingCartDto;
import ru.yandex.practicum.commerce.interactionApi.warehouse.dto.AddProductToWarehouseRequest;
import ru.yandex.practicum.commerce.interactionApi.warehouse.dto.AddressDto;
import ru.yandex.practicum.commerce.interactionApi.warehouse.dto.BookedProductsDto;
import ru.yandex.practicum.commerce.interactionApi.warehouse.dto.NewProductInWarehouseRequest;
import ru.yandex.practicum.commerce.warehouse.entity.WarehouseProduct;
import ru.yandex.practicum.commerce.warehouse.repository.WarehouseProductRepository;

import java.security.SecureRandom;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
@Transactional(readOnly = true)
public class WarehouseServiceImpl implements WarehouseService {

    private final WarehouseProductRepository warehouseRepository;

    private static final String[] ADDRESSES = new String[]{"ADDRESS_1", "ADDRESS_2"};
    private static final String CURRENT_ADDRESS =
            ADDRESSES[Random.from(new SecureRandom()).nextInt(0, ADDRESSES.length)];

    @PostConstruct
    private void logAddress() {
        log.info("Текущий адрес склада: {}", CURRENT_ADDRESS);
    }

    @Override
    @Transactional
    public void newProductInWarehouse(NewProductInWarehouseRequest request) {
        if (warehouseRepository.existsById(request.getProductId())) {
            throw new SpecifiedProductAlreadyInWarehouseException(
                    "Товар с id " + request.getProductId() + " уже есть на складе");
        }
        WarehouseProduct product = WarehouseProduct.builder()
                .productId(request.getProductId())
                .weight(request.getWeight())
                .fragile(request.isFragile())
                .width(request.getDimension().getWidth())
                .height(request.getDimension().getHeight())
                .depth(request.getDimension().getDepth())
                .quantity(0)
                .build();

        warehouseRepository.save(product);
        log.info("Добавлен новый товар на склад: {}", request.getProductId());
    }

    @Override
    @Transactional
    public void addProductToWarehouse(AddProductToWarehouseRequest request) {
        WarehouseProduct product = warehouseRepository.findById(request.getProductId())
                .orElseThrow(() -> new NoSpecifiedProductInWarehouseException(
                        "Товар " + request.getProductId() + " не найден на складе"));
        product.setQuantity(product.getQuantity() + request.getQuantity());
        warehouseRepository.save(product);
        log.info("Товар {} пополнен на {} единиц", request.getProductId(), request.getQuantity());
    }

    @Override
    public BookedProductsDto checkProductQuantityEnoughForShoppingCart(ShoppingCartDto cartDto) {
        Map<UUID, Long> cartProducts = cartDto.getProducts();
        double totalWeight = 0;
        double totalVolume = 0;
        boolean hasFragile = false;

        for (Map.Entry<UUID, Long> entry : cartProducts.entrySet()) {
            UUID productId = entry.getKey();
            long requestQuantity = entry.getValue();
            WarehouseProduct product = warehouseRepository.findById(productId)
                    .orElseThrow(() -> new ProductInShoppingCartLowQuantityInWarehouse(
                            "Товар " + productId + " отсутствует на складе"));
            if (product.getQuantity() < requestQuantity) {
                throw new ProductInShoppingCartLowQuantityInWarehouse(
                        "Недостаточно товара " + productId + " на складе");
            }
            totalWeight += product.getWeight() * requestQuantity;
            totalVolume += product.getWeight() * product.getHeight() * product.getDepth() * requestQuantity;
            if (product.isFragile()) {
                hasFragile = true;
            }
        }
        BookedProductsDto result = BookedProductsDto.builder()
                .deliveryWeight(totalWeight)
                .deliveryVolume(totalVolume)
                .fragile(hasFragile)
                .build();
        log.info("Проверка корзины {}: всё в наличии, вес={}, объём={}", cartDto.getShoppingCartId(), totalWeight, totalVolume);
        return result;
    }

    @Override
    public AddressDto getWarehouseAddress() {
        return AddressDto.builder()
                .country(CURRENT_ADDRESS)
                .city(CURRENT_ADDRESS)
                .street(CURRENT_ADDRESS)
                .house(CURRENT_ADDRESS)
                .flat(CURRENT_ADDRESS)
                .build();
    }
}