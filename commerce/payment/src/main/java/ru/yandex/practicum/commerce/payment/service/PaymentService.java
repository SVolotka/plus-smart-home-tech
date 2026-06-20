package ru.yandex.practicum.commerce.payment.service;

import ru.yandex.practicum.commerce.interactionApi.order.dto.OrderDto;
import ru.yandex.practicum.commerce.interactionApi.payment.dto.PaymentDto;

import java.util.UUID;

public interface PaymentService {
    Double productCost(OrderDto order);

    Double getTotalCost(OrderDto order);

    PaymentDto payment(OrderDto order);

    void paymentSuccess(UUID paymentId);

    void paymentFailed(UUID paymentId);
}
