package ru.yandex.practicum.commerce.delivery.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.commerce.delivery.service.DeliveryService;
import ru.yandex.practicum.commerce.interactionApi.delivery.dto.DeliveryDto;
import ru.yandex.practicum.commerce.interactionApi.feignClient.DeliveryClient;
import ru.yandex.practicum.commerce.interactionApi.order.dto.OrderDto;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/delivery")
@RequiredArgsConstructor
@Slf4j
public class DeliveryController implements DeliveryClient {
    private final DeliveryService deliveryService;

    @Override
    @PutMapping
    public DeliveryDto planDelivery(@RequestBody DeliveryDto delivery) {
        log.info("Создание доставки для заказа {}", delivery.getOrderId());
        return deliveryService.planDelivery(delivery);
    }

    @Override
    @PostMapping("/successful")
    public void deliverySuccessful(@RequestBody UUID orderId) {
        log.info("Успешная доставка заказа {}", orderId);
        deliveryService.deliverySuccessful(orderId);
    }

    @Override
    @PostMapping("/picked")
    public void deliveryPicked(@RequestBody UUID orderId) {
        log.info("Принятие в доставку заказа {}", orderId);
        deliveryService.deliveryPicked(orderId);
    }

    @Override
    @PostMapping("/failed")
    public void deliveryFailed(@RequestBody UUID orderId) {
        log.info("Неудачная доставка заказа {}", orderId);
        deliveryService.deliveryFailed(orderId);
    }

    @Override
    @PostMapping("/cost")
    public Double deliveryCost(@RequestBody OrderDto order) {
        log.info("Расчёт стоимости доставки для заказа {}", order.getOrderId());
        return deliveryService.deliveryCost(order);
    }
}