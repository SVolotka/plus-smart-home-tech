package ru.yandex.practicum.telemetry.collector.service.handler;

import lombok.experimental.UtilityClass;

@UtilityClass
public class EnumMapper {
    public static <T extends Enum<T>> T map(Enum<?> source, Class<T> targetType) {
        if (source == null) {
            return null;
        }
        return Enum.valueOf(targetType, source.name());
    }
}