package ru.yandex.practicum.telemetry.collector.model.hubEvent;

import lombok.Getter;
import lombok.Setter;
import ru.yandex.practicum.telemetry.collector.model.enums.ConditionOperation;
import ru.yandex.practicum.telemetry.collector.model.enums.ConditionType;

@Getter
@Setter
public class ScenarioCondition {
    private String sensorId;
    private ConditionType type;
    private ConditionOperation operation;
    private Integer value;
}