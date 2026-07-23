package com.example.hotel.message.application.service;

import com.example.hotel.message.application.port.out.KafkaDemoConsumerControl;
import com.example.hotel.message.application.port.out.KafkaDemoDltIncidentStore;
import com.example.hotel.message.application.port.out.KafkaDemoIdempotencyStore;
import com.example.hotel.message.application.port.out.KafkaDemoInspector;
import com.example.hotel.message.application.port.out.KafkaDemoMessagePublisher;
import com.example.hotel.message.application.port.out.KafkaDemoTopology;
import com.example.hotel.message.application.result.KafkaDemoPublishResult;
import com.example.hotel.message.application.result.KafkaDemoStatus;
import com.example.hotel.message.application.result.KafkaDemoTransactionResult;
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
    private final KafkaDemoIdempotencyStore idempotencyStore;
    private final KafkaDemoDltIncidentStore dltIncidentStore;
    private final KafkaDemoConsumerControl consumerControl;

    public KafkaDemoApplicationService(KafkaDemoMessagePublisher messagePublisher,
                                       KafkaDemoTopology topology,
                                       KafkaDemoInspector inspector,
                                       KafkaDemoMetrics metrics,
                                       KafkaDemoIdempotencyStore idempotencyStore,
                                       KafkaDemoDltIncidentStore dltIncidentStore,
                                       KafkaDemoConsumerControl consumerControl) {
        this.messagePublisher = messagePublisher;
        this.topology = topology;
        this.inspector = inspector;
        this.metrics = metrics;
        this.idempotencyStore = idempotencyStore;
        this.dltIncidentStore = dltIncidentStore;
        this.consumerControl = consumerControl;
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
            String key = topology.keyForPartition(i % topology.partitionCount());
            KafkaDemoMessage message = newMessage(null, key, false);
            results.add(messagePublisher.publish(topology.demoTopic(), message,
                    "consumer group demo: generated key targets partition "
                            + (i % topology.partitionCount()) + "; index=" + i));
        }
        return results;
    }

    public List<KafkaDemoPublishResult> publishAsyncBatch(int count) {
        int size = normalizeBatchSize(count);
        List<KafkaDemoMessage> messages = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            messages.add(newMessage(null,
                    topology.keyForPartition(i % topology.partitionCount()),
                    false));
        }
        return messagePublisher.publishAsyncBatch(
                topology.demoTopic(),
                messages,
                "async batch: producer can combine records using batch.size, linger.ms and compression");
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

    public KafkaDemoPublishResult publishPoison(String key) {
        String messageId = "KMSG-POISON-" + UUID.randomUUID().toString().substring(0, 8);
        byte[] invalidJson = ("{\"messageId\":\"" + messageId
                + "\",\"businessKey\":\"" + normalizeKey(key)
                + "\",\"amount\":not-valid-json").getBytes(java.nio.charset.StandardCharsets.UTF_8);
        return messagePublisher.publishRaw(
                topology.demoTopic(),
                messageId,
                normalizeKey(key),
                invalidJson,
                "invalid JSON: ErrorHandlingDeserializer sends it directly to DLT without blocking the partition");
    }

    public KafkaDemoTransactionResult publishTransaction(boolean failAfterFirst) {
        String transactionKey = "transaction-" + UUID.randomUUID().toString().substring(0, 8);
        return messagePublisher.publishTransaction(
                topology.demoTopic(),
                List.of(
                        newMessage(null, transactionKey, false),
                        newMessage(null, transactionKey, false)),
                failAfterFirst);
    }

    public KafkaDemoStatus pausePrimaryConsumer() {
        consumerControl.pausePrimary();
        return status();
    }

    public KafkaDemoStatus resumePrimaryConsumer() {
        consumerControl.resumePrimary();
        return status();
    }

    public KafkaDemoStatus resolveDltIncident(String incidentId, String resolutionNote) {
        dltIncidentStore.resolve(
                incidentId,
                resolutionNote == null || resolutionNote.isBlank()
                        ? "verified and resolved manually"
                        : resolutionNote);
        return status();
    }

    public KafkaDemoStatus status() {
        KafkaDemoMetrics.Snapshot snapshot = metrics.snapshot();
        return new KafkaDemoStatus(
                topology.demoTopic(),
                topology.deadLetterTopic(),
                topology.primaryConsumerGroup(),
                topology.auditConsumerGroup(),
                topology.deadLetterConsumerGroup(),
                consumerControl.primaryPauseRequested(),
                snapshot.published(),
                snapshot.publishAcked(),
                snapshot.consumed(),
                snapshot.auditConsumed(),
                snapshot.duplicated(),
                snapshot.failedForRetry(),
                snapshot.deadLettered(),
                snapshot.poisonPublished(),
                snapshot.transactionsCommitted(),
                snapshot.transactionsAborted(),
                idempotencyStore.recentProcessedMessageIds(100),
                dltIncidentStore.recent(50),
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
