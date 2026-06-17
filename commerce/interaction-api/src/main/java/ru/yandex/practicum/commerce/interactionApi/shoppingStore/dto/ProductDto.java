package ru.yandex.practicum.commerce.interactionApi.shoppingStore.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.yandex.practicum.commerce.interactionApi.shoppingStore.enums.ProductCategory;
import ru.yandex.practicum.commerce.interactionApi.shoppingStore.enums.ProductState;
import ru.yandex.practicum.commerce.interactionApi.shoppingStore.enums.QuantityState;

import java.math.BigDecimal;
import java.util.UUID;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ProductDto {

    private UUID productId;

    @NotNull
    private String productName;

    @NotNull
    private String description;

    private String imageSrc;

    @NotNull
    private QuantityState quantityState;

    @NotNull
    private ProductState productState;

    @NotNull
    private ProductCategory productCategory;

    @NotNull
    @Min(1)
    private BigDecimal price;

}