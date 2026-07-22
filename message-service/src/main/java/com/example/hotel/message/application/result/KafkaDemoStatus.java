package com.example.hotel.message.application.result;

import com.example.hotel.message.domain.model.KafkaDemoConsumedEvent;
import com.example.hotel.message.domain.model.KafkaDemoConsumerLag;
import com.example.hotel.message.domain.model.KafkaDemoTopicInfo;

import java.util.List;

/**
 * Kafka demo 的可观察状态快照。
 */
public record KafkaDemoStatus(
        String topic,
        String deadLetterTopic,
        String primaryConsumerGroup,
        String auditConsumerGroup,
        String deadLetterConsumerGroup,
        long published,
        long publishAcked,
        long consumed,
        long auditConsumed,
        long duplicated,
        long failedForRetry,
        long deadLettered,
        List<String> processedMessageIds,
        List<KafkaDemoConsumedEvent> recentEvents,
        List<KafkaDemoTopicInfo> topics,
        List<KafkaDemoConsumerLag> consumerLags
) {
}
