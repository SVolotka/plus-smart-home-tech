package ru.yandex.practicum.commerce.shoppintCart.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.commerce.interactionApi.shoppingCart.enums.ShoppingCartState;
import ru.yandex.practicum.commerce.shoppintCart.entity.ShoppingCart;

import java.util.Optional;
import java.util.UUID;

public interface ShoppingCartRepository extends JpaRepository<ShoppingCart, UUID> {
    Optional<ShoppingCart> findByUsernameAndState(String username, ShoppingCartState state);
}
