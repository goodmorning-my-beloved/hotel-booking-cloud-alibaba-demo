package com.example.hotel.order.application.port.in;

import com.example.hotel.order.application.result.BookingView;

import java.util.List;
import java.util.Optional;

public interface OrderQueryUseCase {

    Optional<BookingView> getOrder(String orderId);

    List<BookingView> listOrders();
}
