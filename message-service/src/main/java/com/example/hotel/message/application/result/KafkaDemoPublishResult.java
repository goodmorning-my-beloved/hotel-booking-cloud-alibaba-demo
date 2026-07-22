package com.example.hotel.message.application.result;

import java.time.Instant;

/**
 * Kafka 生产结果。partition/offset 是 Broker ACK 后返回的准确位置。
 */
public record KafkaDemoPublishResult(
        String messageId,
        String orderId,
        String businessKey,
        String topic,
        int partition,
        long offset,
        boolean acknowledged,
        String note,
        Instant publishedAt
) {
}
