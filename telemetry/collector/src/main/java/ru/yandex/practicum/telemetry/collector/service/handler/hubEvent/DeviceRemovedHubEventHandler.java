package ru.yandex.practicum.telemetry.collector.service.handler.hubEvent;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.telemetry.collector.model.enums.HubEventType;
import ru.yandex.practicum.telemetry.collector.model.hubEvent.DeviceRemovedEvent;
import ru.yandex.practicum.telemetry.collector.model.hubEvent.HubEvent;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducer;

@Component("DEVICE_REMOVED")
public class DeviceRemovedHubEventHandler extends BaseHubEventHandler<DeviceRemovedEventAvro> {
    public DeviceRemovedHubEventHandler(KafkaEventProducer producer) {
        super(producer);
    }

    @Override
    public HubEventType getMessageType() {
        return HubEventType.DEVICE_REMOVED;
    }

    @Override
    protected DeviceRemovedEventAvro mapToAvro(HubEvent event) {
        DeviceRemovedEvent e = (DeviceRemovedEvent) event;
        return DeviceRemovedEventAvro.newBuilder()
                .setId(e.getId())
                .build();
    }
}