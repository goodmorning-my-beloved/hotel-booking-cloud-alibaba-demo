package com.example.hotel.common.dto;

import java.math.BigDecimal;
import java.time.Instant;

public record BookingCreatedEvent(
        String orderId,
        Long userId,
        Long roomId,
        BigDecimal amount,
        Instant createdAt
) {
}
