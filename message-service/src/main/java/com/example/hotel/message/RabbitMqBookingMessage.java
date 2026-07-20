package com.example.hotel.message;

import java.math.BigDecimal;
import java.time.Instant;

public record RabbitMqBookingMessage(
        String messageId,
        String orderId,
        Long userId,
        Long roomId,
        BigDecimal amount,
        boolean fail,
        Instant createdAt
) {
}
