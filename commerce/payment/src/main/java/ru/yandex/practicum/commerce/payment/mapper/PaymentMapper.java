package ru.yandex.practicum.commerce.payment.mapper;

import org.mapstruct.Mapper;
import ru.yandex.practicum.commerce.interactionApi.payment.dto.PaymentDto;
import ru.yandex.practicum.commerce.payment.entity.Payment;

@Mapper(componentModel = "spring")
public interface PaymentMapper {
    PaymentDto toDto(Payment payment);
}
