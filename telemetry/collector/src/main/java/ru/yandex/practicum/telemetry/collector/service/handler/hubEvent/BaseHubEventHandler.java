package ru.yandex.practicum.telemetry.collector.service.handler.hubEvent;

import lombok.RequiredArgsConstructor;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.telemetry.collector.model.hubEvent.HubEvent;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducer;
import ru.yandex.practicum.telemetry.collector.service.handler.HubEventHandler;

@RequiredArgsConstructor
public abstract class BaseHubEventHandler<T> implements HubEventHandler {
    private final KafkaEventProducer producer;

    @Override
    public void handle(HubEvent event) {
        T payload = mapToAvro(event);
        HubEventAvro avroEvent = HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp())
                .setPayload(payload)
                .build();
        producer.sendHubEvent(avroEvent.getHubId(), avroEvent);
    }

    protected abstract T mapToAvro(HubEvent event);
}