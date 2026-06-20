package ru.yandex.practicum.commerce.order.controller;

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
import ru.yandex.practicum.commerce.interactionApi.exception.NotAuthorizedUserException;
import ru.yandex.practicum.commerce.interactionApi.feignClient.OrderClient;
import ru.yandex.practicum.commerce.interactionApi.order.dto.CreateNewOrderRequest;
import ru.yandex.practicum.commerce.interactionApi.order.dto.OrderDto;
import ru.yandex.practicum.commerce.interactionApi.order.dto.ProductReturnRequest;
import ru.yandex.practicum.commerce.order.service.OrderService;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/order")
@RequiredArgsConstructor
@Slf4j
public class OrderController implements OrderClient {

    private final OrderService orderService;

    @Override
    @GetMapping
    public List<OrderDto> getClientOrders(@RequestParam String username) {
        if (username == null || username.isBlank()) {
            throw new NotAuthorizedUserException("Имя пользователя не должно быть пустым");
        }
        log.info("Запрос заказов пользователя: {}", username);
        return orderService.getClientOrders(username);
    }

    @Override
    @PutMapping
    public OrderDto createNewOrder(@RequestParam String username,
                                   @Valid @RequestBody CreateNewOrderRequest request) {
        log.info("Создание нового заказа для пользователя {}", username);
        return orderService.createNewOrder(username, request);
    }

    @Override
    @PostMapping("/return")
    public OrderDto productReturn(@RequestBody ProductReturnRequest request) {
        log.info("Возврат товаров по заказу {}", request.getOrderId());
        return orderService.productReturn(request);
    }

    @Override
    @PostMapping("/payment")
    public OrderDto payment(@RequestBody UUID orderId) {
        log.info("Оплата заказа {}", orderId);
        return orderService.payment(orderId);
    }

    @Override
    @PostMapping("/payment/failed")
    public OrderDto paymentFailed(@RequestBody UUID orderId) {
        log.info("Ошибка оплаты заказа {}", orderId);
        return orderService.paymentFailed(orderId);
    }

    @Override
    @PostMapping("/delivery")
    public OrderDto delivery(@RequestBody UUID orderId) {
        log.info("Доставка заказа {}", orderId);
        return orderService.delivery(orderId);
    }

    @Override
    @PostMapping("/delivery/failed")
    public OrderDto deliveryFailed(@RequestBody UUID orderId) {
        log.info("Ошибка доставки заказа {}", orderId);
        return orderService.deliveryFailed(orderId);
    }

    @Override
    @PostMapping("/completed")
    public OrderDto complete(@RequestBody UUID orderId) {
        log.info("Завершение заказа {}", orderId);
        return orderService.complete(orderId);
    }

    @Override
    @PostMapping("/calculate/total")
    public OrderDto calculateTotalCost(@RequestBody UUID orderId) {
        log.info("Расчёт общей стоимости заказа {}", orderId);
        return orderService.calculateTotalCost(orderId);
    }

    @Override
    @PostMapping("/calculate/delivery")
    public OrderDto calculateDeliveryCost(@RequestBody UUID orderId) {
        log.info("Расчёт стоимости доставки заказа {}", orderId);
        return orderService.calculateDeliveryCost(orderId);
    }

    @Override
    @PostMapping("/assembly")
    public OrderDto assembly(@RequestBody UUID orderId) {
        log.info("Сборка заказа {}", orderId);
        return orderService.assembly(orderId);
    }

    @Override
    @PostMapping("/assembly/failed")
    public OrderDto assemblyFailed(@RequestBody UUID orderId) {
        log.info("Ошибка сборки заказа {}", orderId);
        return orderService.assemblyFailed(orderId);
    }

    @Override
    @GetMapping("/{orderId}")
    public OrderDto getOrder(@PathVariable UUID orderId) {
        log.info("Получение заказа {}", orderId);
        return orderService.getOrder(orderId);
    }
}
