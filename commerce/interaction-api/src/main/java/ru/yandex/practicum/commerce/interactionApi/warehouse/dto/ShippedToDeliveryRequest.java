package ru.yandex.practicum.commerce.interactionApi.warehouse.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ShippedToDeliveryRequest {
    private UUID orderId;
    private UUID deliveryId;
}
