package ru.yandex.practicum.telemetry.aggregator.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Component
@RequiredArgsConstructor
@Slf4j
public class KafkaSnapshotProducer implements AutoCloseable {

    private final KafkaTemplate<String, SpecificRecordBase> kafkaTemplate;

    @Value("${spring.kafka.producer.topic.snapshots}")
    private String snapshotsTopic;

    public void sendSnapshot(String hubId, SpecificRecordBase snapshot, long timestamp) {
        ProducerRecord<String, SpecificRecordBase> record = new ProducerRecord<>(
                snapshotsTopic,
                null,
                timestamp,
                hubId,
                snapshot
        );

        try {
            CompletableFuture<SendResult<String, SpecificRecordBase>> future = kafkaTemplate.send(record);
            kafkaTemplate.flush();
            SendResult<String, SpecificRecordBase> result = future.get();
            RecordMetadata metadata = result.getRecordMetadata();

            log.info("Снапшот для хаба {} успешно сохранён в топик {} в партицию {} со смещением {}",
                    hubId, metadata.topic(), metadata.partition(), metadata.offset());
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            log.warn("Отправка снапшота для хаба {} в топик {} была прервана", hubId, snapshotsTopic, ex);
        } catch (ExecutionException ex) {
            log.warn("Не удалось записать снапшот для хаба {} в топик {}", hubId, snapshotsTopic, ex);
        }
    }

    @Override
    public void close() {
        kafkaTemplate.flush();
    }
}