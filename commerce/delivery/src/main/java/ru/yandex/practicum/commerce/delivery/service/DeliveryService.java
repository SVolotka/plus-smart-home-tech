package ru.yandex.practicum.commerce.delivery.service;

import ru.yandex.practicum.commerce.interactionApi.delivery.dto.DeliveryDto;
import ru.yandex.practicum.commerce.interactionApi.order.dto.OrderDto;

import java.util.UUID;

public interface DeliveryService {
    DeliveryDto planDelivery(DeliveryDto dto);

    void deliverySuccessful(UUID orderId);

    void deliveryPicked(UUID orderId);

    void deliveryFailed(UUID orderId);

    Double deliveryCost(OrderDto order);
}
