package ru.yandex.practicum.telemetry.collector.service.handler;

import ru.yandex.practicum.telemetry.collector.model.enums.HubEventType;
import ru.yandex.practicum.telemetry.collector.model.hubEvent.HubEvent;

public interface HubEventHandler {
    HubEventType getMessageType();
    void handle(HubEvent event);
}