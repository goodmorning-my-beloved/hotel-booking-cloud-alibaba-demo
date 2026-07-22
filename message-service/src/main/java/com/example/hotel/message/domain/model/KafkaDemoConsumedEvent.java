package com.example.hotel.message.domain.model;

import java.time.Instant;

/**
 * Kafka 消费处理记录，用于 /kafka/demo/status 观察。
 */
public record KafkaDemoConsumedEvent(
        String messageId,
        String orderId,
        String businessKey,
        String consumerGroup,
        String consumerName,
        String topic,
        int partition,
        long offset,
        String status,
        String detail,
        Instant receivedAt
) {
}
