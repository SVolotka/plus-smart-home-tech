package ru.yandex.practicum.telemetry.collector.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class KafkaEventProducer {
    private final KafkaTemplate<String, SpecificRecordBase> kafkaTemplate;

    @Value("${spring.kafka.producer.topic.sensors}")
    private String sensorsTopic;

    @Value("${spring.kafka.producer.topic.hubs}")
    private String hubsTopic;

    public void sendSensorEvent(String hubId, SpecificRecordBase event) {
        kafkaTemplate.send(sensorsTopic, hubId, event)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.debug("Событие датчика отправлено: hubId={}", hubId);
                    } else {
                        log.error("Ошибка отправки события датчика hubId={}", hubId, ex);
                    }
                });
    }

    public void sendHubEvent(String hubId, SpecificRecordBase event) {
        kafkaTemplate.send(hubsTopic, hubId, event)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.debug("Событие хаба отправлено: hubId={}", hubId);
                    } else {
                        log.error("Ошибка отправки события хаба hubId={}", hubId, ex);
                    }
                });
    }
}