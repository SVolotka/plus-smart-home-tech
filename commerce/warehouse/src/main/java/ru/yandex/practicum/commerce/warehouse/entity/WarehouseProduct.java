package ru.yandex.practicum.commerce.warehouse.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.util.UUID;

@Entity
@Table(name = "warehouse_products")
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
@EqualsAndHashCode(of = "productId")
@Builder
public class WarehouseProduct {
    @Id
    @Column(columnDefinition = "UUID")
    private UUID productId;

    @Column(nullable = false)
    private double weight;

    @Column(nullable = false)
    private boolean fragile;

    @Column(nullable = false)
    private double width;

    @Column(nullable = false)
    private double height;

    @Column(nullable = false)
    private double depth;

    @Column(nullable = false)
    private long quantity;
}
