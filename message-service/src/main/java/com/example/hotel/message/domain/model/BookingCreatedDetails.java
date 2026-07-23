package com.example.hotel.message.domain.model;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * message-service 对“订单已创建”事实的本地领域表达。
 *
 * <p>它不依赖 order-service 的 Java 类型；消息入站适配器负责把线级 JSON 协议翻译成本对象。</p>
 */
public record BookingCreatedDetails(
        String orderId,
        Long userId,
        Long roomId,
        BigDecimal amount,
        Instant createdAt
) {
}
