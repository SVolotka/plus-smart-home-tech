package ru.yandex.practicum.telemetry.analyzer.entity;

import jakarta.persistence.*;
import lombok.*;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "scenario_actions")
@EqualsAndHashCode(of = "id")
public class ScenarioAction {

    @EmbeddedId
    private ScenarioActionId id;

    @MapsId("scenarioId")
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "scenario_id")
    private Scenario scenario;

    @MapsId("sensorId")
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "sensor_id")
    private Sensor sensor;

    @MapsId("actionId")
    @ManyToOne(fetch = FetchType.LAZY, cascade = CascadeType.PERSIST)
    @JoinColumn(name = "action_id")
    private Action action;
}