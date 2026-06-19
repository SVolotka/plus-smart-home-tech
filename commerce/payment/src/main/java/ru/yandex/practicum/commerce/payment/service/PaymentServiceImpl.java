package ru.yandex.practicum.commerce.payment.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import ru.yandex.practicum.commerce.interactionApi.exception.NotEnoughInfoInOrderToCalculateException;
import ru.yandex.practicum.commerce.interactionApi.feignClient.OrderClient;
import ru.yandex.practicum.commerce.interactionApi.feignClient.ProductClient;
import ru.yandex.practicum.commerce.interactionApi.order.dto.OrderDto;
import ru.yandex.practicum.commerce.interactionApi.payment.dto.PaymentDto;
import ru.yandex.practicum.commerce.interactionApi.payment.enums.PaymentStatus;
import ru.yandex.practicum.commerce.payment.entity.Payment;
import ru.yandex.practicum.commerce.payment.mapper.PaymentMapper;
import ru.yandex.practicum.commerce.payment.repository.PaymentRepository;

import java.math.BigDecimal;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class PaymentServiceImpl implements PaymentService {

    private final PaymentRepository paymentRepository;
    private final PaymentMapper paymentMapper;
    private final ProductClient productClient;
    private final OrderClient orderClient;
    private final TransactionTemplate transactionTemplate;

    @Override
    public Double productCost(OrderDto order) {
        if (order.getProducts() == null || order.getProducts().isEmpty()) {
            throw new NotEnoughInfoInOrderToCalculateException("Нет товаров в заказе");
        }
        return order.getProducts().entrySet().stream()
                .mapToDouble(entry -> {
                    var product = productClient.getProduct(entry.getKey());
                    return product.getPrice()
                            .multiply(BigDecimal.valueOf(entry.getValue()))
                            .doubleValue();
                })
                .sum();
    }

    @Override
    public Double getTotalCost(OrderDto order) {
        double productCost = productCost(order);
        double deliveryCost = order.getDeliveryPrice() != null ? order.getDeliveryPrice() : 0.0;
        double tax = productCost * 0.10;
        return productCost + deliveryCost + tax;
    }

    @Override
    public PaymentDto payment(OrderDto order) {
        double total = getTotalCost(order);
        double delivery = order.getDeliveryPrice() != null ? order.getDeliveryPrice() : 0.0;
        double productCost = productCost(order);
        double tax = total - delivery - productCost;

        Payment payment = Payment.builder()
                .orderId(order.getOrderId())
                .totalPayment(total)
                .deliveryTotal(delivery)
                .productPrice(productCost)
                .feeTotal(tax)
                .status(PaymentStatus.PENDING)
                .build();

        Payment saved = transactionTemplate.execute(status -> paymentRepository.save(payment));
        log.info("Создана оплата {} для заказа {} на сумму {}", saved.getPaymentId(), order.getOrderId(), total);
        return paymentMapper.toDto(saved);
    }

    @Override
    public void paymentSuccess(UUID paymentId) {
        Payment payment = paymentRepository.findById(paymentId)
                .orElseThrow(() -> new RuntimeException("Оплата не найдена"));
        payment.setStatus(PaymentStatus.SUCCESS);
        transactionTemplate.execute(status -> {
            paymentRepository.save(payment);
            return null;
        });

        try {
            orderClient.payment(payment.getOrderId());
        } catch (Exception e) {
            log.error("Ошибка при обновлении статуса заказа {}: {}", payment.getOrderId(), e.getMessage());
        }
        log.info("Оплата {} проведена успешно", paymentId);
    }

    @Override
    public void paymentFailed(UUID paymentId) {
        Payment payment = paymentRepository.findById(paymentId)
                .orElseThrow(() -> new RuntimeException("Оплата не найдена"));
        payment.setStatus(PaymentStatus.FAILED);
        transactionTemplate.execute(status -> {
            paymentRepository.save(payment);
            return null;
        });

        try {
            orderClient.paymentFailed(payment.getOrderId());
        } catch (Exception e) {
            log.error("Ошибка при обновлении статуса заказа {}: {}", payment.getOrderId(), e.getMessage());
        }
        log.info("Оплата {} отклонена", paymentId);
    }
}
