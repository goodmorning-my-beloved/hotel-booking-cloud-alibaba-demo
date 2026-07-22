package com.example.hotel.order.domain.event;

import java.math.BigDecimal;
import java.time.Instant;

public record BookingCreated(
        String orderId,
        Long userId,
        Long roomId,
        BigDecimal amount,
        Instant occurredAt
) {
}
