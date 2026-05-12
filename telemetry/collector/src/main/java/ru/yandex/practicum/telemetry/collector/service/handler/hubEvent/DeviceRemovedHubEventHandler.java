package ru.yandex.practicum.telemetry.collector.service.handler.hubEvent;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.DeviceRemovedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducer;
import ru.yandex.practicum.telemetry.collector.service.handler.HubEventHandler;

import java.time.Instant;

@Component
public class DeviceRemovedHubEventHandler implements HubEventHandler {
    private final KafkaEventProducer producer;

    public DeviceRemovedHubEventHandler(KafkaEventProducer producer) {
        this.producer = producer;
    }

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.DEVICE_REMOVED;
    }

    @Override
    public void handle(HubEventProto event) {
        DeviceRemovedEventProto deviceRemoved = event.getDeviceRemoved();
        DeviceRemovedEventAvro avroPayload = DeviceRemovedEventAvro.newBuilder()
                .setId(deviceRemoved.getId())
                .build();

        HubEventAvro eventAvro = HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(Instant.ofEpochSecond(event.getTimestamp().getSeconds(), event.getTimestamp().getNanos()))
                .setPayload(avroPayload)
                .build();

        producer.sendHubEvent(event.getHubId(), eventAvro, eventAvro.getTimestamp());

    }
}