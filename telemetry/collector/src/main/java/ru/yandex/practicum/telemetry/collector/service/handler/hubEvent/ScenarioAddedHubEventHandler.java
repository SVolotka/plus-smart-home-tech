package ru.yandex.practicum.telemetry.collector.service.handler.hubEvent;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioAddedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioConditionProto;
import ru.yandex.practicum.kafka.telemetry.event.ActionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.ConditionOperationAvro;
import ru.yandex.practicum.kafka.telemetry.event.ConditionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducer;
import ru.yandex.practicum.telemetry.collector.service.handler.EnumMapper;
import ru.yandex.practicum.telemetry.collector.service.handler.HubEventHandler;

import java.time.Instant;
import java.util.stream.Collectors;

@Component
public class ScenarioAddedHubEventHandler implements HubEventHandler {
    private final KafkaEventProducer producer;

    public ScenarioAddedHubEventHandler(KafkaEventProducer producer) {
        this.producer = producer;
    }

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.SCENARIO_ADDED;
    }

    @Override
    public void handle(HubEventProto event) {
        ScenarioAddedEventProto scenarioAdded = event.getScenarioAdded();
        ScenarioAddedEventAvro avroPayload = ScenarioAddedEventAvro.newBuilder()
                .setName(scenarioAdded.getName())
                .setConditions(scenarioAdded.getConditionList().stream()
                        .map(this::mapCondition)
                        .collect(Collectors.toList()))
                .setActions(scenarioAdded.getActionList().stream()
                        .map(this::mapAction)
                        .collect(Collectors.toList()))
                .build();

        HubEventAvro eventAvro = HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(Instant.ofEpochSecond(event.getTimestamp().getSeconds(), event.getTimestamp().getNanos()))
                .setPayload(avroPayload)
                .build();

        producer.sendHubEvent(event.getHubId(), eventAvro, eventAvro.getTimestamp());
    }

    private ScenarioConditionAvro mapCondition(ScenarioConditionProto scenarioCondition) {
        ScenarioConditionAvro.Builder builder = ScenarioConditionAvro.newBuilder()
                .setSensorId(scenarioCondition.getSensorId())
                .setType(EnumMapper.map(scenarioCondition.getType(), ConditionTypeAvro.class))
                .setOperation(EnumMapper.map(scenarioCondition.getOperation(), ConditionOperationAvro.class));
        if (scenarioCondition.hasBoolValue()) {
            builder.setValue(scenarioCondition.getBoolValue());
        } else if (scenarioCondition.hasIntValue()) {
            builder.setValue(scenarioCondition.getIntValue());
        }
        return builder.build();
    }

    private DeviceActionAvro mapAction(DeviceActionProto deviceAction) {
        DeviceActionAvro.Builder builder = DeviceActionAvro.newBuilder()
                .setSensorId(deviceAction.getSensorId())
                .setType(EnumMapper.map(deviceAction.getType(), ActionTypeAvro.class));
        if (deviceAction.hasValue()) {
            builder.setValue(deviceAction.getValue());
        }
        return builder.build();
    }
}