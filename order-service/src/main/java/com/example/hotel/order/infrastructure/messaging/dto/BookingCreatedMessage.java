package com.example.hotel.order.infrastructure.messaging.dto;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * 订单消息适配器发布到 RabbitMQ/Kafka 的线级消息结构。
 */
public record BookingCreatedMessage(
        String orderId,
        Long userId,
        Long roomId,
        BigDecimal amount,
        Instant createdAt
) {
}
