package com.example.hotel.order.infrastructure.persistence;

import com.example.hotel.order.domain.model.BookingOrder;
import com.example.hotel.order.domain.repository.OrderRepository;
import org.springframework.stereotype.Repository;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Repository
public class InMemoryOrderRepository implements OrderRepository {

    private final Map<String, BookingOrder> orders = new ConcurrentHashMap<>();

    @Override
    public void save(BookingOrder order) {
        orders.put(order.orderId(), order);
    }

    @Override
    public Optional<BookingOrder> findById(String orderId) {
        return Optional.ofNullable(orders.get(orderId));
    }

    @Override
    public List<BookingOrder> findAll() {
        return orders.values().stream()
                .sorted(Comparator.comparing(BookingOrder::createdAt).reversed())
                .toList();
    }

    @Override
    public void deleteById(String orderId) {
        orders.remove(orderId);
    }
}
