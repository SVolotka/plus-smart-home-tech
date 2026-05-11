package ru.yandex.practicum.telemetry.aggregator;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.telemetry.aggregator.kafka.KafkaSnapshotProducer;
import ru.yandex.practicum.telemetry.aggregator.service.SensorSnapshotService;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class AggregationStarter implements AutoCloseable {

    private static final Duration POLL_TIMEOUT = Duration.ofMillis(1000);

    @Value("${spring.kafka.producer.topic.sensors}")
    private String sensorsTopic;

    private final ConsumerFactory<String, SensorEventAvro> consumerFactory;
    private final KafkaSnapshotProducer snapshotProducer;
    private final SensorSnapshotService snapshotService;

    private Consumer<String, SensorEventAvro> consumer;
    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    public void start() {
        consumer = consumerFactory.createConsumer();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.wakeup();
            close();
        }));

        try {
            consumer.subscribe(Collections.singletonList(sensorsTopic));
            log.info("Подписан на топик: {}, group-id: {}", sensorsTopic, consumer.groupMetadata().groupId());

            while (true) {
                ConsumerRecords<String, SensorEventAvro> records = consumer.poll(POLL_TIMEOUT);

                Map<TopicPartition, OffsetAndMetadata> batchOffsets = new HashMap<>();

                for (ConsumerRecord<String, SensorEventAvro> record : records) {
                    try {
                        SensorEventAvro event = record.value();

                        log.debug("Обработка: offset={}, partition={}, hubId={}, sensorId={}",
                                record.offset(), record.partition(), event.getHubId(), event.getId());

                        snapshotService.updateSnapshot(event).ifPresent(snapshot ->
                                snapshotProducer.sendSnapshot(snapshot.getHubId(), snapshot)
                        );

                        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                        OffsetAndMetadata newOffset = new OffsetAndMetadata(record.offset() + 1);

                        batchOffsets.merge(tp, newOffset,
                                (old, newer) -> newer.offset() > old.offset() ? newer : old);

                    } catch (Exception e) {
                        log.error("Ошибка обработки: topic={}, partition={}, offset={}",
                                record.topic(), record.partition(), record.offset(), e);
                    }
                }

                if (!batchOffsets.isEmpty()) {
                    consumer.commitSync(batchOffsets);
                    currentOffsets.clear();
                    currentOffsets.putAll(batchOffsets);
                }

            }

        } catch (WakeupException e) {
            log.info("Получен сигнал wakeup, завершаем обработку");
        } catch (Exception e) {
            log.error("Необработанная ошибка в цикле агрегации", e);
        } finally {
            close();
        }
    }

    @Override
    public void close() {
        if (consumer != null) {
            try {
                if (!currentOffsets.isEmpty()) {
                    consumer.commitSync(currentOffsets);
                }
                snapshotProducer.close();
            } finally {
                log.info("Закрываем консьюмер");
                consumer.close();
            }
        }
    }
}