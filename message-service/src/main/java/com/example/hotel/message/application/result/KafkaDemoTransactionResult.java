package com.example.hotel.message.application.result;

import java.time.Instant;
import java.util.List;

/**
 * Kafka 事务演示结果。只有事务提交后，read_committed 消费者才会看到其中的消息。
 */
public record KafkaDemoTransactionResult(
        boolean committed,
        int attemptedMessages,
        List<KafkaDemoPublishResult> publishedMessages,
        String detail,
        Instant completedAt
) {
}
