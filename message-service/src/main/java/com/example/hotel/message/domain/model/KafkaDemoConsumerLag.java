package com.example.hotel.message.domain.model;

/**
 * Kafka consumer group 的 offset/lag 观察数据。
 */
public record KafkaDemoConsumerLag(
        String consumerGroup,
        String topic,
        int partition,
        Long committedOffset,
        long endOffset,
        Long lag
) {
}
