package com.example.hotel.order.application.result;

import java.math.BigDecimal;
import java.time.LocalDate;

public record BookingView(
        String orderId,
        Long userId,
        Long roomId,
        LocalDate checkIn,
        LocalDate checkOut,
        BigDecimal amount,
        String status
) {
}
