package ru.yandex.practicum.commerce.delivery.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import ru.yandex.practicum.commerce.delivery.entity.Delivery;
import ru.yandex.practicum.commerce.interactionApi.delivery.dto.DeliveryDto;

@Mapper(componentModel = "spring")
public interface DeliveryMapper {
    DeliveryDto toDto(Delivery delivery);

    @Mapping(target = "deliveryId", ignore = true)
    @Mapping(target = "deliveryState", ignore = true)
    Delivery toEntity(DeliveryDto dto);
}
