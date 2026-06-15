package ru.yandex.practicum.commerce.shoppintCart.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.client.circuitbreaker.CircuitBreaker;
import org.springframework.cloud.client.circuitbreaker.CircuitBreakerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.interactionApi.exception.NoProductsInShoppingCartException;
import ru.yandex.practicum.commerce.interactionApi.exception.WarehouseServiceUnavailableException;
import ru.yandex.practicum.commerce.interactionApi.feignClient.WarehouseClient;
import ru.yandex.practicum.commerce.interactionApi.shoppingCart.dto.ShoppingCartDto;
import ru.yandex.practicum.commerce.interactionApi.shoppingCart.enums.ShoppingCartState;
import ru.yandex.practicum.commerce.shoppintCart.entity.ShoppingCart;
import ru.yandex.practicum.commerce.shoppintCart.mapper.ShoppingCartMapper;
import ru.yandex.practicum.commerce.shoppintCart.repository.ShoppingCartRepository;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class ShoppingCartServiceImpl implements ShoppingCartService {
    private final ShoppingCartMapper cartMapper;
    private final ShoppingCartRepository cartRepository;
    private final WarehouseClient warehouseClient;
    private final CircuitBreakerFactory circuitBreakerFactory;

    @Override
    @Transactional
    public ShoppingCartDto getShoppingCart(String username) {
        ShoppingCart cart = cartRepository.findByUsernameAndState(username, ShoppingCartState.ACTIVE)
                .orElseGet(() -> createNewCart(username));
        log.debug("Получена корзина для пользователя {}: {}", username, cart.getShoppingCartId());
        return cartMapper.toDto(cart);
    }

    @Override
    @Transactional
    public ShoppingCartDto addProducts(String username, Map<UUID, Long> products) {
        ShoppingCartDto shoppingCartDto = ShoppingCartDto.builder()
                .products(products)
                .shoppingCartId(UUID.randomUUID())
                .build();
        checkWarehouseAvailability(shoppingCartDto);
        return addProductsInTransaction(username, products);
    }

    @Override
    @Transactional
    public void deactivateCart(String username) {
        ShoppingCart cart = cartRepository.findByUsernameAndState(username, ShoppingCartState.ACTIVE)
                .orElseThrow(() -> {
                    log.warn("Активная корзина для пользователя {} не найдена", username);
                    return new NoSuchElementException("Активная корзина не найдена");
                });
        cart.setState(ShoppingCartState.DEACTIVATED);
        cartRepository.save(cart);
        log.info("Корзина пользователя {} деактивирована", username);
    }

    @Override
    @Transactional
    public ShoppingCartDto removeProducts(String username, List<UUID> productIds) {
        ShoppingCart cart = cartRepository.findByUsernameAndState(username, ShoppingCartState.ACTIVE)
                .orElseGet(() -> createNewCart(username));
        boolean removed = productIds.stream()
                .map(cart.getProducts()::remove)
                .anyMatch(Objects::nonNull);
        if (!removed) {
            throw new NoProductsInShoppingCartException("Указанные товары не найдены в корзине");
        }
        cartRepository.save(cart);
        log.info("Из корзины пользователя {} удалены товары: {}", username, productIds);
        return cartMapper.toDto(cart);
    }

    @Override
    @Transactional
    public ShoppingCartDto changeQuantity(String username, UUID productId, long newQuantity) {
        ShoppingCart cart = cartRepository.findByUsernameAndState(username, ShoppingCartState.ACTIVE)
                .orElseGet(() -> createNewCart(username));
        if (!cart.getProducts().containsKey(productId)) {
            throw new NoProductsInShoppingCartException("Товар " + productId + " отсутствует в корзине");
        }
        cart.getProducts().put(productId, newQuantity);
        cartRepository.save(cart);
        log.info("Изменено количество товара {} в корзине пользователя {}: {}", productId, username, newQuantity);
        return cartMapper.toDto(cart);
    }

    @Transactional
    public ShoppingCartDto addProductsInTransaction(String username, Map<UUID, Long> products) {
        ShoppingCart cart = cartRepository.findByUsernameAndState(username, ShoppingCartState.ACTIVE)
                .orElseGet(() -> createNewCart(username));
        products.forEach((productId, quantity) ->
                cart.getProducts().merge(productId, quantity, Long::sum));
        cartRepository.save(cart);
        log.info("Товары добавлены в корзину пользователя {}: {}", username, products);
        return cartMapper.toDto(cart);
    }

    private ShoppingCart createNewCart(String username) {
        ShoppingCart cart = ShoppingCart.builder()
                .username(username)
                .state(ShoppingCartState.ACTIVE)
                .products(new HashMap<>())
                .build();
        ShoppingCart saved = cartRepository.save(cart);
        log.info("Создана новая корзина для пользователя {}: {}", username, saved.getShoppingCartId());
        return saved;
    }

    private void checkWarehouseAvailability(ShoppingCartDto cartDto) {
        CircuitBreaker circuitBreaker = circuitBreakerFactory.create("warehouse");
        circuitBreaker.run(
                () -> {
                    warehouseClient.checkProductQuantityEnoughForShoppingCart(cartDto);
                    return null;
                },
                throwable -> fallbackCheckAvailability(cartDto, throwable)
        );
    }

    private Void fallbackCheckAvailability(ShoppingCartDto cartDto, Throwable t) {
        log.error("Сервис склада недоступен для проверки корзины: {}", t.getMessage());
        throw new WarehouseServiceUnavailableException("Сервис склада временно недоступен. Попробуйте позже.");
    }
}
