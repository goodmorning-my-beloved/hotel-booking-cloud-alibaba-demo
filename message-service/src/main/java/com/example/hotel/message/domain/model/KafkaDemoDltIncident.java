package com.example.hotel.message.domain.model;

import java.time.Instant;

/**
 * Kafka DLT 中一条待处理消息对应的持久化工单。
 */
public record KafkaDemoDltIncident(
        String incidentId,
        String sourceKey,
        String dltTopic,
        int dltPartition,
        long dltOffset,
        String originalTopic,
        Integer originalPartition,
        Long originalOffset,
        String messageId,
        String businessKey,
        boolean poisonPayload,
        String exceptionType,
        String exceptionMessage,
        String payloadPreview,
        String status,
        String resolutionNote,
        Instant createdAt,
        Instant updatedAt,
        Instant resolvedAt
) {
}
