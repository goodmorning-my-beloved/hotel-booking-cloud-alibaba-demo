package com.example.hotel.message.application.result;

import com.example.hotel.message.domain.model.KafkaDemoConsumedEvent;
import com.example.hotel.message.domain.model.KafkaDemoConsumerLag;
import com.example.hotel.message.domain.model.KafkaDemoDltIncident;
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
        boolean primaryPauseRequested,
        long published,
        long publishAcked,
        long consumed,
        long auditConsumed,
        long duplicated,
        long failedForRetry,
        long deadLettered,
        long poisonPublished,
        long transactionsCommitted,
        long transactionsAborted,
        List<String> processedMessageIds,
        List<KafkaDemoDltIncident> dltIncidents,
        List<KafkaDemoConsumedEvent> recentEvents,
        List<KafkaDemoTopicInfo> topics,
        List<KafkaDemoConsumerLag> consumerLags
) {
}
