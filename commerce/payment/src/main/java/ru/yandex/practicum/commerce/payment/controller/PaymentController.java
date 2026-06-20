package ru.yandex.practicum.commerce.payment.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.commerce.interactionApi.feignClient.PaymentClient;
import ru.yandex.practicum.commerce.interactionApi.order.dto.OrderDto;
import ru.yandex.practicum.commerce.interactionApi.payment.dto.PaymentDto;
import ru.yandex.practicum.commerce.payment.service.PaymentService;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/payment")
@RequiredArgsConstructor
@Slf4j
public class PaymentController implements PaymentClient {

    private final PaymentService paymentService;

    @Override
    @PostMapping("/productCost")
    public Double productCost(@RequestBody OrderDto order) {
        log.info("Расчёт стоимости товаров для заказа {}", order.getOrderId());
        return paymentService.productCost(order);
    }

    @Override
    @PostMapping("/totalCost")
    public Double getTotalCost(@RequestBody OrderDto order) {
        log.info("Расчёт полной стоимости заказа {}", order.getOrderId());
        return paymentService.getTotalCost(order);
    }

    @Override
    @PostMapping
    public PaymentDto payment(@RequestBody OrderDto order) {
        log.info("Формирование оплаты для заказа {}", order.getOrderId());
        return paymentService.payment(order);
    }

    @Override
    @PostMapping("/refund")
    public void paymentSuccess(@RequestBody UUID paymentId) {
        log.info("Подтверждение успешной оплаты {}", paymentId);
        paymentService.paymentSuccess(paymentId);
    }

    @Override
    @PostMapping("/failed")
    public void paymentFailed(@RequestBody UUID paymentId) {
        log.info("Отказ в оплате {}", paymentId);
        paymentService.paymentFailed(paymentId);
    }
}
