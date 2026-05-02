package ru.yandex.practicum.telemetry.collector.service.handler.sensorEvent;

import lombok.RequiredArgsConstructor;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.telemetry.collector.model.sensorEvent.SensorEvent;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducer;
import ru.yandex.practicum.telemetry.collector.service.handler.SensorEventHandler;

@RequiredArgsConstructor
public abstract class BaseSensorEventHandler<T> implements SensorEventHandler {
    private final KafkaEventProducer producer;

    @Override
    public void handle(SensorEvent event) {
        T payload = mapToAvro(event);
        SensorEventAvro avroEvent = SensorEventAvro.newBuilder()
                .setId(event.getId())
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp())
                .setPayload(payload)
                .build();
        producer.sendSensorEvent(avroEvent.getHubId(), avroEvent, avroEvent.getTimestamp());
    }

    protected abstract T mapToAvro(SensorEvent event);
}