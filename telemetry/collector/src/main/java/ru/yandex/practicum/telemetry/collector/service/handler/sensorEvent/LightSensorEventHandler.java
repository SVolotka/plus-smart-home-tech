package ru.yandex.practicum.telemetry.collector.service.handler.sensorEvent;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.LightSensorProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducer;
import ru.yandex.practicum.telemetry.collector.service.handler.SensorEventHandler;

import java.time.Instant;

@Component
public class LightSensorEventHandler implements SensorEventHandler {
    private final KafkaEventProducer producer;

    public LightSensorEventHandler(KafkaEventProducer producer) {
        this.producer = producer;
    }

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.LIGHT_SENSOR;
    }

    @Override
    public void handle(SensorEventProto event) {
        LightSensorProto lightSensor = event.getLightSensor();
        LightSensorAvro avroPayload = LightSensorAvro.newBuilder()
                .setLinkQuality(lightSensor.getLinkQuality())
                .setLuminosity(lightSensor.getLuminosity())
                .build();

        SensorEventAvro eventAvro = SensorEventAvro.newBuilder()
                .setId(event.getId())
                .setHubId(event.getHubId())
                .setTimestamp(Instant.ofEpochSecond(event.getTimestamp().getSeconds(), event.getTimestamp().getNanos()))
                .setPayload(avroPayload)
                .build();

        producer.sendSensorEvent(event.getHubId(), eventAvro, eventAvro.getTimestamp());
    }
}