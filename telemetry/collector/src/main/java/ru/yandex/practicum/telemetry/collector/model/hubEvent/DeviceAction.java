package ru.yandex.practicum.telemetry.collector.model.hubEvent;

import lombok.Getter;
import lombok.Setter;
import ru.yandex.practicum.telemetry.collector.model.enums.ActionType;

@Getter
@Setter
public class DeviceAction {
    private String sensorId;
    private ActionType type;
    private Integer value;
}