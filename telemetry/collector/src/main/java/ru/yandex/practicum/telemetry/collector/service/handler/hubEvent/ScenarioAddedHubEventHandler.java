package ru.yandex.practicum.telemetry.collector.service.handler.hubEvent;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.ActionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.ConditionOperationAvro;
import ru.yandex.practicum.kafka.telemetry.event.ConditionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;
import ru.yandex.practicum.telemetry.collector.model.enums.HubEventType;
import ru.yandex.practicum.telemetry.collector.model.hubEvent.HubEvent;
import ru.yandex.practicum.telemetry.collector.model.hubEvent.ScenarioAddedEvent;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducer;
import ru.yandex.practicum.telemetry.collector.service.handler.EnumMapper;

import java.util.stream.Collectors;

@Component("SCENARIO_ADDED")
public class ScenarioAddedHubEventHandler extends BaseHubEventHandler<ScenarioAddedEventAvro> {
    public ScenarioAddedHubEventHandler(KafkaEventProducer producer) {
        super(producer);
    }

    @Override
    public HubEventType getMessageType() {
        return HubEventType.SCENARIO_ADDED;
    }

    @Override
    protected ScenarioAddedEventAvro mapToAvro(HubEvent event) {
        ScenarioAddedEvent e = (ScenarioAddedEvent) event;
        return ScenarioAddedEventAvro.newBuilder()
                .setName(e.getName())
                .setConditions(e.getConditions().stream()
                        .map(c -> ScenarioConditionAvro.newBuilder()
                                .setSensorId(c.getSensorId())
                                .setType(EnumMapper.map(c.getType(), ConditionTypeAvro.class))
                                .setOperation(EnumMapper.map(c.getOperation(), ConditionOperationAvro.class))
                                .setValue(c.getValue())
                                .build())
                        .collect(Collectors.toList()))
                .setActions(e.getActions().stream()
                        .map(a -> DeviceActionAvro.newBuilder()
                                .setSensorId(a.getSensorId())
                                .setType(EnumMapper.map(a.getType(), ActionTypeAvro.class))
                                .setValue(a.getValue())
                                .build())
                        .collect(Collectors.toList()))
                .build();
    }
}