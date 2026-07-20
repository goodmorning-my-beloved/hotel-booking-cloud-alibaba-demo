package com.example.hotel.message;

import java.time.Instant;

public record RabbitMqConsumedEvent(
        String messageId,
        String orderId,
        String status,
        String detail,
        Instant receivedAt
) {
}
