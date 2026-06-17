package ru.yandex.practicum.commerce.interactionApi.feignClient;

import jakarta.validation.Valid;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import ru.yandex.practicum.commerce.interactionApi.shoppingStore.dto.PageProductDto;
import ru.yandex.practicum.commerce.interactionApi.shoppingStore.dto.ProductDto;
import ru.yandex.practicum.commerce.interactionApi.shoppingStore.enums.ProductCategory;
import ru.yandex.practicum.commerce.interactionApi.shoppingStore.enums.QuantityState;

import java.util.UUID;

@FeignClient(name = "shopping-store")
public interface ProductClient {

    @GetMapping("/api/v1/shopping-store")
    PageProductDto getProducts(@RequestParam ProductCategory category,
                               @RequestParam(defaultValue = "0") int page,
                               @RequestParam(defaultValue = "10") int size,
                               @RequestParam(defaultValue = "productName,asc") String sort);

    @GetMapping("/api/v1/shopping-store/{productId}")
    ProductDto getProduct(@PathVariable UUID productId);

    @PutMapping("/api/v1/shopping-store")
    ProductDto createProduct(@Valid @RequestBody ProductDto dto);

    @PostMapping("/api/v1/shopping-store")
    ProductDto updateProduct(@Valid @RequestBody ProductDto dto);

    @PostMapping("/api/v1/shopping-store/removeProductFromStore")
    Boolean removeProductFromStore(@RequestBody UUID productId);

    @PostMapping("/api/v1/shopping-store/quantityState")
    Boolean setProductQuantityState(@RequestParam UUID productId,
                                    @RequestParam QuantityState quantityState);
}
