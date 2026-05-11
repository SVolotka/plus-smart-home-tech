package ru.yandex.practicum.telemetry.analyzer.entity;

import jakarta.persistence.*;
import lombok.*;
import ru.yandex.practicum.telemetry.analyzer.enums.ActionType;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "actions")
@EqualsAndHashCode(of = "id")
public class Action {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Enumerated(EnumType.STRING)
    @Column(name = "type", nullable = false)
    private ActionType type;

    @Column(name = "value")
    private Integer value;
}