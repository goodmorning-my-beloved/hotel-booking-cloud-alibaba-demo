package com.example.hotel.message.infrastructure.messaging.dto;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * message-service 在 RabbitMQ/Kafka 入站边界理解的线级消息协议。
 *
 * <p>字段需要与 order-service 发布的 JSON 兼容，但两个上下文不共享同一个 Java DTO。</p>
 */
public record BookingCreatedMessage(
        String orderId,
        Long userId,
        Long roomId,
        BigDecimal amount,
        Instant createdAt
) {
}
