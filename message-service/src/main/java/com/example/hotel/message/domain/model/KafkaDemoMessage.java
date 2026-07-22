package com.example.hotel.message.domain.model;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Kafka 学习演示消息。
 *
 * @param messageId 消息幂等键，消费者用它识别重复投递。
 * @param orderId 业务订单号，用来模拟酒店订单事件。
 * @param customerId 用户 ID。
 * @param roomId 房间 ID。
 * @param amount 订单金额。
 * @param businessKey Kafka message key，决定消息进入哪个 partition。
 * @param fail 是否模拟消费失败，触发 retry 和 DLT。
 * @param createdAt 生产时间。
 */
public record KafkaDemoMessage(
        String messageId,
        String orderId,
        Long customerId,
        Long roomId,
        BigDecimal amount,
        String businessKey,
        boolean fail,
        Instant createdAt
) {
}
