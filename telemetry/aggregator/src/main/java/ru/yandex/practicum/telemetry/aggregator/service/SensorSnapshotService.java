package ru.yandex.practicum.telemetry.aggregator.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
@RequiredArgsConstructor
public class SensorSnapshotService {

    private final Map<String, SensorsSnapshotAvro> snapshots = new ConcurrentHashMap<>();

    public Optional<SensorsSnapshotAvro> updateSnapshot(SensorEventAvro event) {
        String hubId = event.getHubId();
        String sensorId = event.getId();
        Instant eventTimestamp = event.getTimestamp();

        SensorsSnapshotAvro snapshot = snapshots.computeIfAbsent(hubId, id ->
                SensorsSnapshotAvro.newBuilder()
                        .setHubId(id)
                        .setSensorsState(new HashMap<>())
                        .setTimestamp(eventTimestamp)
                        .build()
        );

        SensorStateAvro prevState = snapshot.getSensorsState().get(sensorId);

        if (prevState != null) {

            if (prevState.getTimestamp().compareTo(eventTimestamp) > 0) {
                log.trace("Пропущено устаревшее событие: sensor={}, eventTs={}, lastTs={}",
                        sensorId, eventTimestamp, prevState.getTimestamp());
                return Optional.empty();
            }

            if (prevState.getData().equals(event.getPayload())) {
                log.trace("Дубликат данных для датчика {}, пропускаем", sensorId);
                return Optional.empty();
            }
        }

        SensorStateAvro newState = SensorStateAvro.newBuilder()
                .setTimestamp(eventTimestamp)
                .setData(event.getPayload())
                .build();

        snapshot.getSensorsState().put(sensorId, newState);
        snapshot.setTimestamp(eventTimestamp);

        log.debug("Снапшот обновлён: hub={}, sensor={}, ts={}", hubId, sensorId, eventTimestamp);
        return Optional.of(snapshot);
    }
}