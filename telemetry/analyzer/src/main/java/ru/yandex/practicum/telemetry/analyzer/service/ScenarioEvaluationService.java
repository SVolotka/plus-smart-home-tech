package ru.yandex.practicum.telemetry.analyzer.service;

import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.telemetry.analyzer.entity.Scenario;

import java.util.List;

public interface ScenarioEvaluationService {
    List<DeviceActionAvro> evaluate(SensorsSnapshotAvro snapshot, Scenario scenario);
}