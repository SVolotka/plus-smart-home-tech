package ru.yandex.practicum.telemetry.analyzer.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import ru.yandex.practicum.kafka.deserializer.HubEventDeserializer;
import ru.yandex.practicum.kafka.deserializer.SnapshotDeserializer;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${analyzer.kafka.consumer.snapshots.group-id}")
    private String snapGroupId;

    @Value("${analyzer.kafka.consumer.hubs.group-id}")
    private String hubGroupId;

    @Value("${analyzer.kafka.consumer.snapshots.max-poll-records:50}")
    private int snapMax;

    @Value("${analyzer.kafka.consumer.hubs.max-poll-records:20}")
    private int hubMax;

    @Bean
    public DefaultKafkaConsumerFactory<String, SensorsSnapshotAvro> snapshotConsumerFactory() {
        return factory(snapGroupId, snapMax, SnapshotDeserializer.class);
    }

    @Bean
    public DefaultKafkaConsumerFactory<String, HubEventAvro> hubEventConsumerFactory() {
        return factory(hubGroupId, hubMax, HubEventDeserializer.class);
    }

    private <T> DefaultKafkaConsumerFactory<String, T> factory(String groupId, int maxPoll, Class<?> des) {
        Map<String, Object> p = new HashMap<>();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, des);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPoll);
        return new DefaultKafkaConsumerFactory<>(p);
    }
}