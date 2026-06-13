package ru.yandex.practicum.commerce.shoppingStore.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.commerce.interactionApi.shoppingStore.dto.PageProductDto;
import ru.yandex.practicum.commerce.interactionApi.shoppingStore.dto.PageableObject;
import ru.yandex.practicum.commerce.interactionApi.shoppingStore.dto.ProductDto;
import ru.yandex.practicum.commerce.interactionApi.shoppingStore.enums.ProductCategory;
import ru.yandex.practicum.commerce.interactionApi.shoppingStore.enums.QuantityState;
import ru.yandex.practicum.commerce.interactionApi.shoppingStore.util.PageableUtils;
import ru.yandex.practicum.commerce.shoppingStore.service.ProductService;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/shopping-store")
@RequiredArgsConstructor
@Slf4j
public class ProductController {

    private final ProductService productService;

@GetMapping
    public PageProductDto getProducts(
            @RequestParam ProductCategory category,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size,
            @RequestParam(defaultValue = "productName,asc") String sort) {
        log.debug("Запрос товаров: category={}, page={}, size={}, sort={}", category, page, size, sort);
        String[] sortArray = new String[]{sort};
        PageableObject pageableObject = PageableUtils.createPageableObject(page, size, sortArray);
        return productService.getProducts(category, pageableObject);
    }

    @GetMapping("/{productId}")
    public ProductDto getProduct(@PathVariable UUID productId) {
        log.debug("Запрос товара по id={}", productId);
        return productService.getProduct(productId);
    }

    @PutMapping
    public ProductDto createProduct(@Valid @RequestBody ProductDto dto) {
        log.info("Получен запрос на создание товара: {}", dto.getProductName());
        return productService.createProduct(dto);
    }

    @PostMapping
    public ProductDto updateProduct(@Valid @RequestBody ProductDto dto) {
        log.info("Получен запрос на обновление товара id={}", dto.getProductId());
        return productService.updateProduct(dto);
    }

    @PostMapping("/removeProductFromStore")
    public Boolean removeProductFromStore(@RequestBody UUID productId) {
        log.info("Получен запрос на удаление товара id={}", productId);
        return productService.removeProduct(productId);
    }

//    @PostMapping("/quantityState")
//    public Boolean setProductQuantityState(@Valid @RequestBody SetProductQuantityStateRequest request) {
//        log.info("Получен запрос на изменение статуса остатка: productId={}, state={}",
//                request.getProductId(), request.getQuantityState());
//        return productService.setQuantityState(request.getProductId(), request.getQuantityState());
//    }
@PostMapping("/quantityState")
public Boolean setProductQuantityState(@RequestParam UUID productId,
                                       @RequestParam QuantityState quantityState) {
    log.info("Запрос на изменение статуса остатка товара: productId={}, quantityState={}", productId, quantityState);
    return productService.setQuantityState(productId, quantityState);
}

}
