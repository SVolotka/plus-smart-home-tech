package ru.yandex.practicum.telemetry.collector.service.handler;

import ru.yandex.practicum.telemetry.collector.model.enums.SensorEventType;
import ru.yandex.practicum.telemetry.collector.model.sensorEvent.SensorEvent;

public interface SensorEventHandler {
    SensorEventType getMessageType();
    void handle(SensorEvent event);
}