package ru.yandex.practicum.telemetry.analyzer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioConditionAvro;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;
import ru.yandex.practicum.telemetry.analyzer.entity.Action;
import ru.yandex.practicum.telemetry.analyzer.entity.Condition;
import ru.yandex.practicum.telemetry.analyzer.entity.Scenario;
import ru.yandex.practicum.telemetry.analyzer.entity.ScenarioAction;
import ru.yandex.practicum.telemetry.analyzer.entity.ScenarioActionId;
import ru.yandex.practicum.telemetry.analyzer.entity.ScenarioCondition;
import ru.yandex.practicum.telemetry.analyzer.entity.ScenarioConditionId;
import ru.yandex.practicum.telemetry.analyzer.entity.Sensor;
import ru.yandex.practicum.telemetry.analyzer.enums.ActionType;
import ru.yandex.practicum.telemetry.analyzer.enums.ConditionOperation;
import ru.yandex.practicum.telemetry.analyzer.enums.ConditionType;
import ru.yandex.practicum.telemetry.analyzer.repository.ActionRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.ConditionRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioActionRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioConditionRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioRepository;
import ru.yandex.practicum.telemetry.analyzer.repository.SensorRepository;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
@Slf4j
public class HubEventProcessor implements Runnable {

    @Value("${analyzer.kafka.consumer.hubs.topic}")
    private String topic;

    private final ConsumerFactory<String, HubEventAvro> consumerFactory;
    private final SensorRepository sensorRepo;
    private final ScenarioRepository scenarioRepo;
    private final ConditionRepository conditionRepo;
    private final ActionRepository actionRepo;
    private final ScenarioConditionRepository scRepo;
    private final ScenarioActionRepository saRepo;
    private final TransactionTemplate transactionTemplate;

    private volatile boolean running = true;

    @Override
    public void run() {
        try (Consumer<String, HubEventAvro> consumer = consumerFactory.createConsumer()) {
            consumer.subscribe(List.of(topic));
            Runtime.getRuntime().addShutdownHook(new Thread(this::stop));

            while (running) {
                ConsumerRecords<String, HubEventAvro> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, HubEventAvro> record : records) {
                    try {
                        transactionTemplate.executeWithoutResult(status ->
                                process(record.value())
                        );
                    } catch (Exception e) {
                        log.error("Error processing hub event", e);
                    }
                }
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        } catch (WakeupException e) {
            if (running) throw e;
        }
    }

    private void process(HubEventAvro event) {
        String hubId = event.getHubId();
        Object payload = event.getPayload();

        if (payload instanceof DeviceAddedEventAvro added) {
            sensorRepo.findByIdAndHubId(added.getId(), hubId).ifPresentOrElse(
                    existing -> {
                    },
                    () -> sensorRepo.save(Sensor.builder().id(added.getId()).hubId(hubId).build())
            );
        } else if (payload instanceof DeviceRemovedEventAvro removed) {
            sensorRepo.deleteByIdAndHubId(removed.getId(), hubId);
        } else if (payload instanceof ScenarioAddedEventAvro added) {
            saveScenario(hubId, added);
        } else if (payload instanceof ScenarioRemovedEventAvro removed) {
            scenarioRepo.findByHubIdAndName(hubId, removed.getName())
                    .ifPresent(this::deleteScenario);
        }
    }

    private void saveScenario(String hubId, ScenarioAddedEventAvro event) {
        Scenario scenario = scenarioRepo.findByHubIdAndName(hubId, event.getName())
                .orElseGet(() -> scenarioRepo.save(Scenario.builder().hubId(hubId).name(event.getName()).build()));

        scRepo.deleteByScenarioId(scenario.getId());
        saRepo.deleteByScenarioId(scenario.getId());

        List<ScenarioCondition> conditions = event.getConditions().stream()
                .map(avro -> mapCondition(scenario, avro))
                .collect(Collectors.toList());

        List<ScenarioAction> actions = event.getActions().stream()
                .map(avro -> mapAction(scenario, avro))
                .collect(Collectors.toList());

        scRepo.saveAll(conditions);
        saRepo.saveAll(actions);
    }

    private ScenarioCondition mapCondition(Scenario scenario, ScenarioConditionAvro avro) {
        Condition condition = conditionRepo.save(Condition.builder()
                .type(ConditionType.valueOf(avro.getType().name()))
                .operation(ConditionOperation.valueOf(avro.getOperation().name()))
                .value(convertAvroValue(avro.getValue()))
                .build());

        return ScenarioCondition.builder()
                .id(ScenarioConditionId.builder()
                        .scenarioId(scenario.getId())
                        .sensorId(avro.getSensorId())
                        .conditionId(condition.getId())
                        .build())
                .scenario(scenario)
                .sensor(sensorRepo.getReferenceById(avro.getSensorId()))
                .condition(condition)
                .build();
    }

    private ScenarioAction mapAction(Scenario scenario, DeviceActionAvro avro) {
        Action action = actionRepo.save(Action.builder()
                .type(ActionType.valueOf(avro.getType().name()))
                .value(avro.getValue() != null ? avro.getValue() : null)
                .build());

        return ScenarioAction.builder()
                .id(ScenarioActionId.builder()
                        .scenarioId(scenario.getId())
                        .sensorId(avro.getSensorId())
                        .actionId(action.getId())
                        .build())
                .scenario(scenario)
                .sensor(sensorRepo.getReferenceById(avro.getSensorId()))
                .action(action)
                .build();
    }

    private void deleteScenario(Scenario scenario) {
        scRepo.deleteByScenarioId(scenario.getId());
        saRepo.deleteByScenarioId(scenario.getId());
        scenarioRepo.delete(scenario);
    }

    private Integer convertAvroValue(Object avroValue) {
        return switch (avroValue) {
            case Integer i -> i;
            case Boolean b -> b ? 1 : 0;
            case null, default -> null;
        };
    }

    public void stop() {
        running = false;
    }
}