package com.example.hotel.order.domain.repository;

import com.example.hotel.order.domain.model.BookingOrder;

import java.util.List;
import java.util.Optional;

public interface OrderRepository {

    void save(BookingOrder order);

    Optional<BookingOrder> findById(String orderId);

    List<BookingOrder> findAll();

    void deleteById(String orderId);
}
