package ru.yandex.practicum.commerce.shoppingStore.service;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.interactionApi.exception.ProductNotFoundException;
import ru.yandex.practicum.commerce.interactionApi.shoppingStore.dto.PageProductDto;
import ru.yandex.practicum.commerce.interactionApi.shoppingStore.dto.PageableObject;
import ru.yandex.practicum.commerce.interactionApi.shoppingStore.dto.ProductDto;
import ru.yandex.practicum.commerce.interactionApi.shoppingStore.enums.ProductCategory;
import ru.yandex.practicum.commerce.interactionApi.shoppingStore.enums.ProductState;
import ru.yandex.practicum.commerce.interactionApi.shoppingStore.enums.QuantityState;
import ru.yandex.practicum.commerce.interactionApi.shoppingStore.util.PageableUtils;
import ru.yandex.practicum.commerce.shoppingStore.entity.Product;
import ru.yandex.practicum.commerce.shoppingStore.mapper.ProductMapper;
import ru.yandex.practicum.commerce.shoppingStore.repository.ProductRepository;

import java.util.UUID;

@Service
@AllArgsConstructor
@Slf4j
@Transactional(readOnly = true)
public class ProductServiceImpl implements ProductService {

    private final ProductRepository productRepository;
    private final ProductMapper productMapper;

    @Override
    public PageProductDto getProducts(ProductCategory category, PageableObject pageableObject) {
        log.debug("Получение товаров категории {} с пагинацией: {}", category, pageableObject);
        Pageable springPageable = PageableUtils.toSpringPageable(pageableObject);
        Specification<Product> spec = (root, query, cb) ->
                cb.equal(root.get("productCategory"), category);
        Page<ProductDto> page = productRepository.findAll(spec, springPageable)
                .map(productMapper::toDto);
        return PageableUtils.toPageProductDto(page, pageableObject);
    }

    @Override
    public ProductDto getProduct(UUID productId) {
        log.debug("Запрос товара по id={}", productId);
        Product product = productRepository.findById(productId)
                .orElseThrow(() -> {
                    log.warn("Товар с id={} не найден", productId);
                    return new ProductNotFoundException("Товар не найден: " + productId);
                });
        return productMapper.toDto(product);
    }

    @Override
    @Transactional
    public ProductDto createProduct(ProductDto dto) {
        log.info("Создание нового товара: {}", dto.getProductName());
        Product product = productMapper.toEntity(dto);
        product.setProductState(ProductState.ACTIVE);
        Product saved = productRepository.save(product);
        log.info("Товар создан: id={}, name={}, category={}",
                saved.getProductId(), saved.getProductName(), saved.getProductCategory());
        return productMapper.toDto(saved);
    }

    @Override
    public ProductDto updateProduct(ProductDto dto) {
        log.info("Обновление товара id={}", dto.getProductId());
        Product existing = productRepository.findById(dto.getProductId())
                .orElseThrow(() -> {
                    log.warn("Товар с id={} не найден при попытке обновления", dto.getProductId());
                    return new ProductNotFoundException("Товар не найден: " + dto.getProductId());
                });
        productMapper.updateEntityFromDto(dto, existing);
        Product updated = productRepository.save(existing);
        log.info("Товар обновлён: id={}, name={}", updated.getProductId(), updated.getProductName());
        return productMapper.toDto(updated);
    }

    @Override
    @Transactional
    public boolean removeProduct(UUID productId) {
        log.info("Удаление товара id={}", productId);
        Product existing = productRepository.findById(productId)
                .orElseThrow(() -> {
                    log.warn("Товар с id={} не найден при попытке удаления", productId);
                    return new ProductNotFoundException("Товар не найден: " + productId);
                });
        existing.setProductState(ProductState.DEACTIVATE);
        productRepository.save(existing);
        log.info("Товар id={} деактивирован", productId);
        return true;
    }

    @Override
    @Transactional
    public boolean setQuantityState(UUID productId, QuantityState state) {
        log.info("Изменение статуса остатка товара id={} на {}", productId, state);
        Product existing = productRepository.findById(productId)
                .orElseThrow(() -> {
                    log.warn("Товар с id={} не найден при попытке изменения статуса остатка", productId);
                    return new ProductNotFoundException("Товар не найден: " + productId);
                });
        existing.setQuantityState(state);
        productRepository.save(existing);
        log.info("Статус остатка товара id={} обновлён на {}", productId, state);
        return true;
    }
}