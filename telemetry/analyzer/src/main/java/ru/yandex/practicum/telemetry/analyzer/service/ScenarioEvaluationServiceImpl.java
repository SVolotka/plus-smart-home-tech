package ru.yandex.practicum.telemetry.analyzer.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.ActionTypeAvro;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceActionAvro;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.telemetry.analyzer.entity.Condition;
import ru.yandex.practicum.telemetry.analyzer.entity.Scenario;
import ru.yandex.practicum.telemetry.analyzer.entity.ScenarioAction;
import ru.yandex.practicum.telemetry.analyzer.entity.ScenarioCondition;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class ScenarioEvaluationServiceImpl implements ScenarioEvaluationService {

    @Override
    public List<DeviceActionAvro> evaluate(SensorsSnapshotAvro snapshot, Scenario scenario) {

        Map<String, SensorStateAvro> states = snapshot.getSensorsState();
        if (states == null || scenario.getConditions() == null) {
            return Collections.emptyList();
        }

        boolean allMatch = scenario.getConditions().stream()
                .allMatch(sc ->
                        sc.getSensor() != null &&
                                states.containsKey(sc.getSensor().getId()) &&
                                checkCondition(sc, states.get(sc.getSensor().getId()))
                );

        if (!allMatch) {
            return Collections.emptyList();
        }

        if (scenario.getActions() == null) {
            return Collections.emptyList();
        }

        return scenario.getActions().stream()
                .map(this::toAvroAction)
                .collect(Collectors.toList());
    }


    private boolean checkCondition(ScenarioCondition sc, SensorStateAvro state) {
        Condition condition = sc.getCondition();
        if (condition == null || condition.getValue() == null || state == null) {
            return false;
        }

        Object payload = state.getData();
        if (payload == null) return false;

        Predicate<Integer> operation = switch (condition.getOperation()) {
            case EQUALS -> value -> value.equals(condition.getValue());
            case GREATER_THAN -> value -> value > condition.getValue();
            case LOWER_THAN -> value -> value < condition.getValue();
        };

        return switch (condition.getType()) {
            case TEMPERATURE -> payload instanceof ClimateSensorAvro climate &&
                    operation.test(climate.getTemperatureC());
            case HUMIDITY -> payload instanceof ClimateSensorAvro climate &&
                    operation.test(climate.getHumidity());
            case CO2LEVEL -> payload instanceof ClimateSensorAvro climate &&
                    operation.test(climate.getCo2Level());
            case LUMINOSITY -> payload instanceof LightSensorAvro light &&
                    operation.test(light.getLuminosity());
            case MOTION -> payload instanceof MotionSensorAvro motion &&
                    (motion.getMotion() == (condition.getValue() == 1));
            case SWITCH -> payload instanceof SwitchSensorAvro sw &&
                    (sw.getState() == (condition.getValue() == 1));
        };
    }

    private DeviceActionAvro toAvroAction(ScenarioAction sc) {
        DeviceActionAvro.Builder builder = DeviceActionAvro.newBuilder()
                .setSensorId(sc.getSensor().getId())
                .setType(ActionTypeAvro.valueOf(sc.getAction().getType().name()));

        if (sc.getAction().getValue() != null) {
            builder.setValue(sc.getAction().getValue());
        }
        return builder.build();
    }
}