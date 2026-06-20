package ru.yandex.practicum.commerce.order.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import ru.yandex.practicum.commerce.interactionApi.delivery.dto.DeliveryDto;
import ru.yandex.practicum.commerce.interactionApi.delivery.enums.DeliveryState;
import ru.yandex.practicum.commerce.interactionApi.exception.NoOrderFoundException;
import ru.yandex.practicum.commerce.interactionApi.exception.NoSpecifiedProductInWarehouseException;
import ru.yandex.practicum.commerce.interactionApi.feignClient.DeliveryClient;
import ru.yandex.practicum.commerce.interactionApi.feignClient.PaymentClient;
import ru.yandex.practicum.commerce.interactionApi.feignClient.WarehouseClient;
import ru.yandex.practicum.commerce.interactionApi.order.dto.CreateNewOrderRequest;
import ru.yandex.practicum.commerce.interactionApi.order.dto.OrderDto;
import ru.yandex.practicum.commerce.interactionApi.order.dto.ProductReturnRequest;
import ru.yandex.practicum.commerce.interactionApi.order.enums.OrderState;
import ru.yandex.practicum.commerce.interactionApi.warehouse.dto.AddressDto;
import ru.yandex.practicum.commerce.interactionApi.warehouse.dto.BookedProductsDto;
import ru.yandex.practicum.commerce.order.entity.Order;
import ru.yandex.practicum.commerce.order.mapper.OrderMapper;
import ru.yandex.practicum.commerce.order.repository.OrderRepository;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderServiceImpl implements OrderService {
    private final OrderRepository orderRepository;
    private final OrderMapper orderMapper;
    private final WarehouseClient warehouseClient;
    private final DeliveryClient deliveryClient;
    private final PaymentClient paymentClient;
    private final TransactionTemplate transactionTemplate;

    @Override
    public OrderDto createNewOrder(String username, CreateNewOrderRequest request) {
        BookedProductsDto booked;
        try {
            booked = warehouseClient.checkProductQuantityEnoughForShoppingCart(request.getShoppingCart());
        } catch (Exception e) {
            throw new NoSpecifiedProductInWarehouseException("Недостаточно товара на складе");
        }

        AddressDto warehouseAddress = warehouseClient.getWarehouseAddress();

        OrderDto savedOrder = transactionTemplate.execute(status -> {
            Order order = Order.builder()
                    .username(username)
                    .shoppingCartId(request.getShoppingCart().getShoppingCartId())
                    .products(request.getShoppingCart().getProducts())
                    .state(OrderState.NEW)
                    .deliveryWeight(booked.getDeliveryWeight())
                    .deliveryVolume(booked.getDeliveryVolume())
                    .fragile(booked.isFragile())
                    .build();
            Order saved = orderRepository.save(order);
            log.info("Создан заказ id={} для пользователя {}", saved.getOrderId(), username);
            return orderMapper.toDto(saved);
        });


        DeliveryDto deliveryDto = DeliveryDto.builder()
                .fromAddress(warehouseAddress)
                .toAddress(request.getDeliveryAddress())
                .orderId(savedOrder.getOrderId())
                .deliveryState(DeliveryState.CREATED)
                .build();
        DeliveryDto createdDelivery = deliveryClient.planDelivery(deliveryDto);

        return transactionTemplate.execute(status -> {
            Order order = orderRepository.findById(savedOrder.getOrderId())
                    .orElseThrow(() -> new NoOrderFoundException("Заказ не найден"));
            order.setDeliveryId(createdDelivery.getDeliveryId());
            orderRepository.save(order);
            log.info("Заказу {} присвоен deliveryId={}", order.getOrderId(), createdDelivery.getDeliveryId());
            return orderMapper.toDto(order);
        });
    }

    @Override
    @Transactional(readOnly = true)
    public List<OrderDto> getClientOrders(String username) {
        return orderRepository.findByUsername(username).stream()
                .map(orderMapper::toDto)
                .collect(Collectors.toList());
    }

    @Override
    @Transactional
    public OrderDto payment(UUID orderId) {
        Order order = findOrder(orderId);
        order.setState(OrderState.PAID);
        log.info("Заказ {} оплачен", orderId);
        return orderMapper.toDto(order);
    }

    @Override
    @Transactional
    public OrderDto paymentFailed(UUID orderId) {
        Order order = findOrder(orderId);
        order.setState(OrderState.PAYMENT_FAILED);
        log.info("Ошибка оплаты заказа {}", orderId);
        return orderMapper.toDto(order);
    }

    @Override
    @Transactional
    public OrderDto delivery(UUID orderId) {
        Order order = findOrder(orderId);
        order.setState(OrderState.DELIVERED);
        log.info("Заказ {} доставлен", orderId);
        return orderMapper.toDto(order);
    }

    @Override
    @Transactional
    public OrderDto deliveryFailed(UUID orderId) {
        Order order = findOrder(orderId);
        order.setState(OrderState.DELIVERY_FAILED);
        log.info("Ошибка доставки заказа {}", orderId);
        return orderMapper.toDto(order);
    }

    @Override
    @Transactional
    public OrderDto complete(UUID orderId) {
        Order order = findOrder(orderId);
        order.setState(OrderState.COMPLETED);
        log.info("Заказ {} завершён", orderId);
        return orderMapper.toDto(order);
    }

    @Override
    public OrderDto calculateTotalCost(UUID orderId) {
        Order order = findOrder(orderId);
        OrderDto dto = orderMapper.toDto(order);
        Double total = paymentClient.getTotalCost(dto);
        Double productPrice = paymentClient.productCost(dto);
        Double deliveryPrice = order.getDeliveryPrice() != null ? order.getDeliveryPrice() : 0;

        return transactionTemplate.execute(status -> {
            order.setProductPrice(productPrice);
            order.setDeliveryPrice(deliveryPrice);
            order.setTotalPrice(total);
            orderRepository.save(order);
            log.info("Рассчитана общая стоимость заказа {}", orderId);
            return orderMapper.toDto(order);
        });
    }

    @Override
    public OrderDto calculateDeliveryCost(UUID orderId) {
        Order order = findOrder(orderId);
        OrderDto orderDto = orderMapper.toDto(order);
        double deliveryCost = deliveryClient.deliveryCost(orderDto);
        order.setDeliveryPrice(deliveryCost);
        transactionTemplate.execute(status -> {
            orderRepository.save(order);
            return null;
        });
        log.info("Рассчитана стоимость доставки заказа {}", orderId);
        return orderMapper.toDto(order);
    }

    @Override
    @Transactional
    public OrderDto assembly(UUID orderId) {
        Order order = findOrder(orderId);
        order.setState(OrderState.ASSEMBLED);
        log.info("Заказ {} собран", orderId);
        return orderMapper.toDto(order);
    }

    @Override
    @Transactional
    public OrderDto assemblyFailed(UUID orderId) {
        Order order = findOrder(orderId);
        order.setState(OrderState.ASSEMBLY_FAILED);
        log.info("Ошибка сборки заказа {}", orderId);
        return orderMapper.toDto(order);
    }

    @Override
    public OrderDto productReturn(ProductReturnRequest request) {
        warehouseClient.acceptReturn(request.getProducts());

        return transactionTemplate.execute(status -> {
            Order order = findOrder(request.getOrderId());
            order.getProducts().keySet().removeAll(request.getProducts().keySet());
            if (order.getProducts().isEmpty()) {
                order.setState(OrderState.PRODUCT_RETURNED);
            }
            log.info("Возврат товаров по заказу {}", request.getOrderId());
            return orderMapper.toDto(order);
        });
    }

    @Override
    public OrderDto getOrder(UUID orderId) {
        return orderMapper.toDto(findOrder(orderId));
    }

    private Order findOrder(UUID orderId) {
        return orderRepository.findById(orderId)
                .orElseThrow(() -> new NoOrderFoundException("Заказ не найден: " + orderId));
    }
}
