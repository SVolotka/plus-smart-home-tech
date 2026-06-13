package ru.yandex.practicum.commerce.shoppingStore.mapper;

import org.mapstruct.*;
import ru.yandex.practicum.commerce.interactionApi.shoppingStore.dto.ProductDto;
import ru.yandex.practicum.commerce.shoppingStore.entity.Product;

@Mapper(componentModel = "spring",
        unmappedTargetPolicy = ReportingPolicy.IGNORE)
public interface ProductMapper {

    @Mapping(target = "productId", ignore = true)
    Product toEntity(ProductDto dto);

    ProductDto toDto(Product product);

    @BeanMapping(nullValuePropertyMappingStrategy = NullValuePropertyMappingStrategy.IGNORE)
    @Mapping(target = "productId", ignore = true)
    void updateEntityFromDto(ProductDto dto, @MappingTarget Product entity);
}