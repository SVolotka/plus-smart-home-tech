package ru.yandex.practicum.commerce.shoppintCart.mapper;

import org.mapstruct.Mapper;
import ru.yandex.practicum.commerce.interactionApi.shoppingCart.dto.ShoppingCartDto;
import ru.yandex.practicum.commerce.shoppintCart.entity.ShoppingCart;

@Mapper(componentModel = "spring")
public interface ShoppingCartMapper {
    ShoppingCartDto toDto(ShoppingCart cart);
}
