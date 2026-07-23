package com.example.hotel.message.application.service;

import com.example.hotel.message.application.port.out.KafkaDemoInspector;
import com.example.hotel.message.application.port.out.KafkaDemoIdempotencyStore;
import com.example.hotel.message.application.port.out.KafkaDemoMessagePublisher;
import com.example.hotel.message.application.port.out.KafkaDemoTopology;
import com.example.hotel.message.application.result.KafkaDemoPublishResult;
import com.example.hotel.message.application.result.KafkaDemoTransactionResult;
import com.example.hotel.message.domain.model.KafkaDemoDltIncident;
import com.example.hotel.message.domain.model.KafkaDemoMessage;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaDemoApplicationServiceTest {

    private final KafkaDemoMetrics metrics = new KafkaDemoMetrics();
    private final FakeTopology topology = new FakeTopology();
    private final FakePublisher publisher = new FakePublisher(metrics);
    private final KafkaDemoApplicationService service = new KafkaDemoApplicationService(
            publisher,
            topology,
            new EmptyInspector(),
            metrics,
            new EmptyIdempotencyStore(),
            new EmptyDltStore(),
            new NoopConsumerControl());

    @Test
    void publishesSameKeyMessagesToDemoTopic() {
        List<KafkaDemoPublishResult> results = service.publishKeyOrder("room-302", 4);

        assertThat(results).hasSize(4);
        assertThat(results)
                .allSatisfy(result -> {
                    assertThat(result.topic()).isEqualTo(topology.demoTopic());
                    assertThat(result.businessKey()).isEqualTo("room-302");
                    assertThat(result.acknowledged()).isTrue();
                });
        assertThat(service.status().published()).isEqualTo(4);
        assertThat(service.status().publishAcked()).isEqualTo(4);
    }

    @Test
    void duplicateDemoUsesSameMessageIdTwice() {
        List<KafkaDemoPublishResult> results = service.publishDuplicate("KMSG-1");

        assertThat(results).hasSize(2);
        assertThat(results)
                .extracting(KafkaDemoPublishResult::messageId)
                .containsExactly("KMSG-1", "KMSG-1");
        assertThat(publisher.messages)
                .extracting(KafkaDemoMessage::messageId)
                .containsExactly("KMSG-1", "KMSG-1");
    }

    private static class FakePublisher implements KafkaDemoMessagePublisher {

        private final KafkaDemoMetrics metrics;
        private final List<KafkaDemoMessage> messages = new ArrayList<>();
        private long offset;

        private FakePublisher(KafkaDemoMetrics metrics) {
            this.metrics = metrics;
        }

        @Override
        public KafkaDemoPublishResult publish(String topic, KafkaDemoMessage message, String note) {
            metrics.published();
            metrics.publishAcked();
            messages.add(message);
            return new KafkaDemoPublishResult(
                    message.messageId(),
                    message.orderId(),
                    message.businessKey(),
                    topic,
                    Math.abs(message.businessKey().hashCode()) % 3,
                    offset++,
                    true,
                    note,
                    Instant.now());
        }

        @Override
        public List<KafkaDemoPublishResult> publishAsyncBatch(String topic,
                                                              List<KafkaDemoMessage> messages,
                                                              String note) {
            return messages.stream().map(message -> publish(topic, message, note)).toList();
        }

        @Override
        public KafkaDemoPublishResult publishRaw(String topic,
                                                 String messageId,
                                                 String businessKey,
                                                 byte[] payload,
                                                 String note) {
            metrics.published();
            metrics.publishAcked();
            return new KafkaDemoPublishResult(
                    messageId, null, businessKey, topic, 0, offset++, true, note, Instant.now());
        }

        @Override
        public KafkaDemoTransactionResult publishTransaction(String topic,
                                                             List<KafkaDemoMessage> messages,
                                                             boolean failAfterFirst) {
            if (failAfterFirst) {
                return new KafkaDemoTransactionResult(
                        false, messages.size(), List.of(), "aborted", Instant.now());
            }
            return new KafkaDemoTransactionResult(
                    true,
                    messages.size(),
                    publishAsyncBatch(topic, messages, "transaction"),
                    "committed",
                    Instant.now());
        }
    }

    private static class FakeTopology implements KafkaDemoTopology {

        @Override
        public String demoTopic() {
            return "demo-topic";
        }

        @Override
        public String deadLetterTopic() {
            return "demo-topic.DLT";
        }

        @Override
        public String primaryConsumerGroup() {
            return "primary-group";
        }

        @Override
        public String auditConsumerGroup() {
            return "audit-group";
        }

        @Override
        public String deadLetterConsumerGroup() {
            return "dlt-group";
        }

        @Override
        public int partitionCount() {
            return 3;
        }

        @Override
        public String keyForPartition(int partition) {
            return "partition-key-" + partition;
        }

        @Override
        public List<String> demoTopics() {
            return List.of(demoTopic(), deadLetterTopic());
        }

        @Override
        public List<String> demoConsumerGroups() {
            return List.of(primaryConsumerGroup(), auditConsumerGroup(), deadLetterConsumerGroup());
        }
    }

    private static class EmptyInspector implements KafkaDemoInspector {

        @Override
        public List<com.example.hotel.message.domain.model.KafkaDemoTopicInfo> topics() {
            return List.of();
        }

        @Override
        public List<com.example.hotel.message.domain.model.KafkaDemoConsumerLag> consumerLags() {
            return List.of();
        }
    }

    private static class EmptyIdempotencyStore implements KafkaDemoIdempotencyStore {

        @Override
        public boolean processOnce(KafkaDemoMessage message) {
            return true;
        }

        @Override
        public List<String> recentProcessedMessageIds(int limit) {
            return List.of();
        }
    }

    private static class EmptyDltStore
            implements com.example.hotel.message.application.port.out.KafkaDemoDltIncidentStore {

        @Override
        public KafkaDemoDltIncident saveIfAbsent(KafkaDemoDltIncident incident, String payloadBase64) {
            return incident;
        }

        @Override
        public List<KafkaDemoDltIncident> recent(int limit) {
            return List.of();
        }

        @Override
        public void resolve(String incidentId, String resolutionNote) {
        }
    }

    private static class NoopConsumerControl
            implements com.example.hotel.message.application.port.out.KafkaDemoConsumerControl {

        @Override
        public void pausePrimary() {
        }

        @Override
        public void resumePrimary() {
        }

        @Override
        public boolean primaryPauseRequested() {
            return false;
        }
    }
}
