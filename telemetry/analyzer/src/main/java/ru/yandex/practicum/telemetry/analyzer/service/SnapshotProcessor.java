package ru.yandex.practicum.telemetry.analyzer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.telemetry.analyzer.entity.Scenario;
import ru.yandex.practicum.telemetry.analyzer.repository.ScenarioRepository;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@RequiredArgsConstructor
@Slf4j
public class SnapshotProcessor {
    @Value("${analyzer.kafka.consumer.snapshots.topic}")
    private String topic;

    private final ConsumerFactory<String, SensorsSnapshotAvro> consumerFactory;
    private final ScenarioRepository scenarioRepo;
    private final ScenarioEvaluationService evalService;
    private final ActionExecutionService execService;
    private final TransactionTemplate transactionTemplate;

    private volatile boolean running = true;

    public void start() {
        try (Consumer<String, SensorsSnapshotAvro> consumer = consumerFactory.createConsumer()) {
            consumer.subscribe(List.of(topic));
            Runtime.getRuntime().addShutdownHook(new Thread(this::stop));

            while (running) {
                ConsumerRecords<String, SensorsSnapshotAvro> records = consumer.poll(Duration.ofMillis(500));
                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

                for (ConsumerRecord<String, SensorsSnapshotAvro> record : records) {
                    try {
                        transactionTemplate.executeWithoutResult(status -> {
                            SensorsSnapshotAvro snapshot = record.value();
                            List<Scenario> scenarios = scenarioRepo.findByHubIdWithDetails(snapshot.getHubId());
                            for (Scenario scenario : scenarios) {
                                List<DeviceActionAvro> actions = evalService.evaluate(snapshot, scenario);
                                for (DeviceActionAvro action : actions) {
                                    execService.execute(snapshot.getHubId(), scenario.getName(), action);
                                }
                            }
                        });

                        offsets.put(new TopicPartition(record.topic(), record.partition()),
                                new OffsetAndMetadata(record.offset() + 1));
                    } catch (Exception ex) {
                        log.error("Snapshot error", ex);
                    }
                }

                if (!offsets.isEmpty()) {
                    consumer.commitSync(offsets);
                }
            }
        } catch (WakeupException e) {
            if (running) {
                throw e;
            }
        }
    }

    public void stop() {
        running = false;
    }
}