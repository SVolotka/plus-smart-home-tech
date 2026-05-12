package ru.yandex.practicum.telemetry.analyzer.service;

import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;

public interface ActionExecutionService {
    void execute(String hubId, String scenarioName, DeviceActionAvro action);
}