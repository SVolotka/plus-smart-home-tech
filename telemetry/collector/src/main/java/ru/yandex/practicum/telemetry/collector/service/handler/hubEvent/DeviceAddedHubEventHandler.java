package ru.yandex.practicum.telemetry.collector.service.handler.hubEvent;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.DeviceAddedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducer;
import ru.yandex.practicum.telemetry.collector.service.handler.EnumMapper;
import ru.yandex.practicum.telemetry.collector.service.handler.HubEventHandler;

import java.time.Instant;

@Component
public class DeviceAddedHubEventHandler implements HubEventHandler {
    private final KafkaEventProducer producer;

    public DeviceAddedHubEventHandler(KafkaEventProducer producer) {
        this.producer = producer;
    }

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.DEVICE_ADDED;
    }

    @Override
    public void handle(HubEventProto event) {
        DeviceAddedEventProto deviceAdded = event.getDeviceAdded();
        DeviceTypeAvro deviceType = EnumMapper.map(deviceAdded.getType(), DeviceTypeAvro.class);
        DeviceAddedEventAvro avroPayload = DeviceAddedEventAvro.newBuilder()
                .setId(deviceAdded.getId())
                .setType(deviceType)
                .build();

        HubEventAvro eventAvro = HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(Instant.ofEpochSecond(event.getTimestamp().getSeconds(), event.getTimestamp().getNanos()))
                .setPayload(avroPayload)
                .build();

        producer.sendHubEvent(event.getHubId(), eventAvro, eventAvro.getTimestamp());
    }
}