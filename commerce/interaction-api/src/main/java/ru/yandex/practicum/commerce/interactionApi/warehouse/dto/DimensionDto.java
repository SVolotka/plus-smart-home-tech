package ru.yandex.practicum.commerce.interactionApi.warehouse.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class DimensionDto {
    @NotNull
    @Min(1)
    private double width;

    @NotNull
    @Min(1)
    private double height;

    @NotNull
    @Min(1)
    private double depth;
}
