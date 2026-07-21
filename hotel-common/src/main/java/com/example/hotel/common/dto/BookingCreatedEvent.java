package com.example.hotel.common.dto;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * 原酒店下单链路里的“订单已创建”事件。
 *
 * <p>order-service 会把它通过 Spring Cloud Stream 同时发到 RabbitMQ 和 Kafka。
 * 这和 message-service 里的 RabbitMqBookingMessage demo 不同：这里偏业务事件，demo 那边偏面试知识点演示。</p>
 *
 * @param orderId 订单号。
 * @param userId 用户 ID。
 * @param roomId 房间 ID。
 * @param amount 订单金额。
 * @param createdAt 事件创建时间。
 */
public record BookingCreatedEvent(
        String orderId,
        Long userId,
        Long roomId,
        BigDecimal amount,
        Instant createdAt
) {
}
