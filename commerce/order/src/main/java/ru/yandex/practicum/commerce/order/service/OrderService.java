package ru.yandex.practicum.commerce.order.service;

import ru.yandex.practicum.commerce.interactionApi.order.dto.CreateNewOrderRequest;
import ru.yandex.practicum.commerce.interactionApi.order.dto.OrderDto;
import ru.yandex.practicum.commerce.interactionApi.order.dto.ProductReturnRequest;

import java.util.List;
import java.util.UUID;

public interface OrderService {
    OrderDto createNewOrder(String username, CreateNewOrderRequest request);

    List<OrderDto> getClientOrders(String username);

    OrderDto payment(UUID orderId);

    OrderDto paymentFailed(UUID orderId);

    OrderDto delivery(UUID orderId);

    OrderDto deliveryFailed(UUID orderId);

    OrderDto complete(UUID orderId);

    OrderDto calculateTotalCost(UUID orderId);

    OrderDto calculateDeliveryCost(UUID orderId);

    OrderDto assembly(UUID orderId);

    OrderDto assemblyFailed(UUID orderId);

    OrderDto productReturn(ProductReturnRequest request);
}
