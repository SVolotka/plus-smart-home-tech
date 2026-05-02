package ru.yandex.practicum.telemetry.collector.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Component
@RequiredArgsConstructor
@Slf4j
public class KafkaEventProducer implements AutoCloseable {
    private final KafkaTemplate<String, SpecificRecordBase> kafkaTemplate;

    @Value("${spring.kafka.producer.topic.sensors}")
    private String sensorsTopic;

    @Value("${spring.kafka.producer.topic.hubs}")
    private String hubsTopic;

    public void sendSensorEvent(String hubId, SpecificRecordBase event, Instant timestamp) {
        sendEvent(sensorsTopic, hubId, event, timestamp);
    }

    public void sendHubEvent(String hubId, SpecificRecordBase event, Instant timestamp) {
        sendEvent(hubsTopic, hubId, event, timestamp);
    }

    private void sendEvent(String topic, String hubId, SpecificRecordBase event, Instant timestamp) {
        ProducerRecord<String, SpecificRecordBase> record = new ProducerRecord<>(
                topic,
                null,
                timestamp.toEpochMilli(),
                hubId,
                event
        );

        try {
            CompletableFuture<SendResult<String, SpecificRecordBase>> future = kafkaTemplate.send(record);
            kafkaTemplate.flush();
            SendResult<String, SpecificRecordBase> result = future.get();
            RecordMetadata metadata = result.getRecordMetadata();
            log.info("Событие {} успешно сохранено в топик {} в партицию {} со смещением {}",
                    event.getClass().getSimpleName(), metadata.topic(), metadata.partition(), metadata.offset());
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            log.warn("Отправка события {} в топик {} была прервана", event.getClass().getSimpleName(), topic, ex);
        } catch (ExecutionException ex) {
            log.warn("Не удалось записать событие {} в топик {}", event.getClass().getSimpleName(), topic, ex);
        }
    }

    @Override
    public void close() {
        kafkaTemplate.flush();
    }
}