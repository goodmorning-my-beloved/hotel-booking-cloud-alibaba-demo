package com.example.hotel.message.application.service;

import com.example.hotel.message.application.port.out.KafkaDemoInspector;
import com.example.hotel.message.application.port.out.KafkaDemoMessagePublisher;
import com.example.hotel.message.application.port.out.KafkaDemoTopology;
import com.example.hotel.message.application.result.KafkaDemoPublishResult;
import com.example.hotel.message.application.result.KafkaDemoStatus;
import com.example.hotel.message.domain.model.KafkaDemoMessage;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Kafka 教学用例。这里编排“发布、批量发布、查询状态”，不直接依赖 KafkaTemplate 或 AdminClient。
 */
@Service
public class KafkaDemoApplicationService {

    private final KafkaDemoMessagePublisher messagePublisher;
    private final KafkaDemoTopology topology;
    private final KafkaDemoInspector inspector;
    private final KafkaDemoMetrics metrics;

    public KafkaDemoApplicationService(KafkaDemoMessagePublisher messagePublisher,
                                       KafkaDemoTopology topology,
                                       KafkaDemoInspector inspector,
                                       KafkaDemoMetrics metrics) {
        this.messagePublisher = messagePublisher;
        this.topology = topology;
        this.inspector = inspector;
        this.metrics = metrics;
    }

    public KafkaDemoPublishResult publishNormal(String messageId, String key) {
        KafkaDemoMessage message = newMessage(messageId, key, false);
        return messagePublisher.publish(topology.demoTopic(), message,
                "normal send: producer waits for broker ack and returns topic/partition/offset");
    }

    public List<KafkaDemoPublishResult> publishKeyOrder(String key, int count) {
        String stableKey = normalizeKey(key);
        int size = normalizeBatchSize(count);
        List<KafkaDemoPublishResult> results = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            KafkaDemoMessage message = newMessage(null, stableKey, false);
            results.add(messagePublisher.publish(topology.demoTopic(), message,
                    "same key should go to same partition, offsets increase in send order; index=" + i));
        }
        return results;
    }

    public List<KafkaDemoPublishResult> publishConsumerGroupBatch(int count) {
        int size = normalizeBatchSize(count);
        List<KafkaDemoPublishResult> results = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            String key = "group-demo-" + (i % 3);
            KafkaDemoMessage message = newMessage(null, key, false);
            results.add(messagePublisher.publish(topology.demoTopic(), message,
                    "consumer group demo: partitions are shared by consumers in the same group; index=" + i));
        }
        return results;
    }

    public List<KafkaDemoPublishResult> publishDuplicate(String messageId) {
        String duplicateId = normalizeMessageId(messageId);
        KafkaDemoMessage first = newMessage(duplicateId, "duplicate-demo", false);
        KafkaDemoMessage second = new KafkaDemoMessage(
                duplicateId,
                "ORD-" + UUID.randomUUID().toString().substring(0, 8),
                1L,
                101L,
                new BigDecimal("499.00"),
                "duplicate-demo",
                false,
                Instant.now());
        return List.of(
                messagePublisher.publish(topology.demoTopic(), first,
                        "first message for consumer idempotency demo"),
                messagePublisher.publish(topology.demoTopic(), second,
                        "same messageId, primary consumer should commit offset without duplicate side effects"));
    }

    public KafkaDemoPublishResult publishDeadLetter(String messageId, String key) {
        KafkaDemoMessage message = newMessage(messageId, key, true);
        return messagePublisher.publish(topology.demoTopic(), message,
                "consumer will fail, retry, then publish to the DLT");
    }

    public KafkaDemoStatus status() {
        KafkaDemoMetrics.Snapshot snapshot = metrics.snapshot();
        return new KafkaDemoStatus(
                topology.demoTopic(),
                topology.deadLetterTopic(),
                topology.primaryConsumerGroup(),
                topology.auditConsumerGroup(),
                topology.deadLetterConsumerGroup(),
                snapshot.published(),
                snapshot.publishAcked(),
                snapshot.consumed(),
                snapshot.auditConsumed(),
                snapshot.duplicated(),
                snapshot.failedForRetry(),
                snapshot.deadLettered(),
                snapshot.processedMessageIds(),
                snapshot.recentEvents(),
                inspector.topics(),
                inspector.consumerLags());
    }

    private KafkaDemoMessage newMessage(String messageId, String key, boolean fail) {
        return new KafkaDemoMessage(
                normalizeMessageId(messageId),
                "ORD-" + UUID.randomUUID().toString().substring(0, 8),
                1L,
                101L,
                new BigDecimal("499.00"),
                normalizeKey(key),
                fail,
                Instant.now());
    }

    private int normalizeBatchSize(int count) {
        if (count < 1) {
            return 3;
        }
        return Math.min(count, 20);
    }

    private String normalizeMessageId(String messageId) {
        return messageId == null || messageId.isBlank()
                ? "KMSG-" + UUID.randomUUID().toString().substring(0, 8)
                : messageId;
    }

    private String normalizeKey(String key) {
        return key == null || key.isBlank() ? "room-101" : key;
    }
}
