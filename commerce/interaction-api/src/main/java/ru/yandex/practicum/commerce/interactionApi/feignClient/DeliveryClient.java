package ru.yandex.practicum.commerce.interactionApi.feignClient;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import ru.yandex.practicum.commerce.interactionApi.delivery.dto.DeliveryDto;
import ru.yandex.practicum.commerce.interactionApi.order.dto.OrderDto;

import java.util.UUID;

@FeignClient(name = "delivery")
public interface DeliveryClient {
    @PutMapping("/api/v1/delivery")
    DeliveryDto planDelivery(@RequestBody DeliveryDto delivery);

    @PostMapping("/api/v1/delivery/successful")
    void deliverySuccessful(@RequestBody UUID orderId);

    @PostMapping("/api/v1/delivery/picked")
    void deliveryPicked(@RequestBody UUID orderId);

    @PostMapping("/api/v1/delivery/failed")
    void deliveryFailed(@RequestBody UUID orderId);

    @PostMapping("/api/v1/delivery/cost")
    Double deliveryCost(@RequestBody OrderDto order);
}
