package ru.yandex.practicum.telemetry.analyzer.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.telemetry.analyzer.entity.ScenarioAction;
import ru.yandex.practicum.telemetry.analyzer.entity.ScenarioActionId;

public interface ScenarioActionRepository extends JpaRepository<ScenarioAction, ScenarioActionId> {
    void deleteByScenarioId(Long scenarioId);
}