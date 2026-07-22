package com.example.hotel.message.domain.model;

import java.util.List;
import java.util.Map;

/**
 * Kafka topic 元数据快照。
 */
public record KafkaDemoTopicInfo(
        String topic,
        int partitionCount,
        List<Integer> partitions,
        Map<String, String> configs
) {
}
