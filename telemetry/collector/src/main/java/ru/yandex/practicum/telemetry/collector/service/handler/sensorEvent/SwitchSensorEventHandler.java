package ru.yandex.practicum.telemetry.collector.service.handler.sensorEvent;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.telemetry.collector.model.enums.SensorEventType;
import ru.yandex.practicum.telemetry.collector.model.sensorEvent.SensorEvent;
import ru.yandex.practicum.telemetry.collector.model.sensorEvent.SwitchSensorEvent;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducer;

@Component("SWITCH_SENSOR_EVENT")
public class SwitchSensorEventHandler extends BaseSensorEventHandler<SwitchSensorAvro> {
    public SwitchSensorEventHandler(KafkaEventProducer producer) {
        super(producer);
    }

    @Override
    public SensorEventType getMessageType() {
        return SensorEventType.SWITCH_SENSOR_EVENT;
    }

    @Override
    protected SwitchSensorAvro mapToAvro(SensorEvent event) {
        SwitchSensorEvent e = (SwitchSensorEvent) event;
        return SwitchSensorAvro.newBuilder()
                .setState(e.getState())
                .build();
    }
}