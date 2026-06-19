package ru.yandex.practicum.commerce.delivery.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import ru.yandex.practicum.commerce.delivery.entity.Delivery;
import ru.yandex.practicum.commerce.delivery.mapper.DeliveryMapper;
import ru.yandex.practicum.commerce.delivery.repository.DeliveryRepository;
import ru.yandex.practicum.commerce.interactionApi.delivery.dto.DeliveryDto;
import ru.yandex.practicum.commerce.interactionApi.delivery.enums.DeliveryState;
import ru.yandex.practicum.commerce.interactionApi.exception.NoDeliveryFoundException;
import ru.yandex.practicum.commerce.interactionApi.feignClient.OrderClient;
import ru.yandex.practicum.commerce.interactionApi.feignClient.WarehouseClient;
import ru.yandex.practicum.commerce.interactionApi.order.dto.OrderDto;
import ru.yandex.practicum.commerce.interactionApi.warehouse.dto.AddressDto;
import ru.yandex.practicum.commerce.interactionApi.warehouse.dto.AssemblyProductsForOrderRequest;
import ru.yandex.practicum.commerce.interactionApi.warehouse.dto.ShippedToDeliveryRequest;

import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class DeliveryServiceImpl implements DeliveryService {

    private final DeliveryRepository deliveryRepository;
    private final DeliveryMapper deliveryMapper;
    private final WarehouseClient warehouseClient;
    private final OrderClient orderClient;
    private final TransactionTemplate transactionTemplate;

    @Override
    public DeliveryDto planDelivery(DeliveryDto dto) {
        Delivery delivery = deliveryMapper.toEntity(dto);
        delivery.setDeliveryState(DeliveryState.CREATED);
        Delivery saved = transactionTemplate.execute(status -> deliveryRepository.save(delivery));
        log.info("Создана доставка {} для заказа {}", saved.getDeliveryId(), saved.getOrderId());
        return deliveryMapper.toDto(saved);
    }

    @Override
    public void deliverySuccessful(UUID orderId) {
        Delivery delivery = deliveryRepository.findByOrderId(orderId)
                .orElseThrow(() -> new NoDeliveryFoundException("Доставка для заказа " + orderId + " не найдена"));
        delivery.setDeliveryState(DeliveryState.DELIVERED);
        transactionTemplate.execute(status -> {
            deliveryRepository.save(delivery);
            return null;
        });

        try {
            orderClient.delivery(orderId);
        } catch (Exception e) {
            log.error("Ошибка при обновлении статуса заказа {}: {}", orderId, e.getMessage());
        }
        log.info("Доставка {} выполнена успешно", delivery.getDeliveryId());
    }

    @Override
    public void deliveryPicked(UUID orderId) {
        Delivery delivery = deliveryRepository.findByOrderId(orderId)
                .orElseThrow(() -> new NoDeliveryFoundException("Доставка не найдена"));
        delivery.setDeliveryState(DeliveryState.IN_PROGRESS);
        transactionTemplate.execute(status -> {
            deliveryRepository.save(delivery);
            return null;
        });

        OrderDto order = orderClient.getOrder(orderId);

        AssemblyProductsForOrderRequest assemblyRequest = AssemblyProductsForOrderRequest.builder()
                .orderId(orderId)
                .products(order.getProducts())
                .build();
        warehouseClient.assemblyProductsForOrder(assemblyRequest);

        ShippedToDeliveryRequest shippedRequest = ShippedToDeliveryRequest.builder()
                .orderId(orderId)
                .deliveryId(delivery.getDeliveryId())
                .build();
        warehouseClient.shippedToDelivery(shippedRequest);

        try {
            orderClient.assembly(orderId);
        } catch (Exception e) {
            log.error("Ошибка при уведомлении заказа о сборке: {}", e.getMessage());
        }
        log.info("Заказ {} передан в доставку", orderId);
    }

    @Override
    public void deliveryFailed(UUID orderId) {
        Delivery delivery = deliveryRepository.findByOrderId(orderId)
                .orElseThrow(() -> new NoDeliveryFoundException("Доставка для заказа " + orderId + " не найдена"));
        delivery.setDeliveryState(DeliveryState.FAILED);
        transactionTemplate.execute(status -> {
            deliveryRepository.save(delivery);
            return null;
        });
        try {
            orderClient.deliveryFailed(orderId);
        } catch (Exception e) {
            log.error("Ошибка при обновлении статуса заказа {}: {}", orderId, e.getMessage());
        }
        log.info("Доставка {} провалена", delivery.getDeliveryId());
    }


    @Override
    public Double deliveryCost(OrderDto order) {
        AddressDto warehouseAddress = warehouseClient.getWarehouseAddress();
        Delivery delivery = deliveryRepository.findByOrderId(order.getOrderId())
                .orElseThrow(() -> new NoDeliveryFoundException("Доставка не найдена"));
        AddressDto clientAddress = AddressDto.builder()
                .country(delivery.getToAddress().getCountry())
                .city(delivery.getToAddress().getCity())
                .street(delivery.getToAddress().getStreet())
                .house(delivery.getToAddress().getHouse())
                .flat(delivery.getToAddress().getFlat())
                .build();

        double base = 5.0;
        double multiplier = warehouseAddress.getStreet().contains("ADDRESS_1") ? 1.0 : 2.0;
        double sum = base + (base * multiplier);
        if (Boolean.TRUE.equals(order.getFragile())) {
            sum += sum * 0.2;
        }
        sum += (order.getDeliveryWeight() != null ? order.getDeliveryWeight() : 0) * 0.3;
        sum += (order.getDeliveryVolume() != null ? order.getDeliveryVolume() : 0) * 0.2;

        if (!warehouseAddress.getStreet().equals(clientAddress.getStreet())) {
            sum += sum * 0.2;
        }
        return sum;
    }
}