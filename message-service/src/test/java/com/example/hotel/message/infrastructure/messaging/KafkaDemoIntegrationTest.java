package com.example.hotel.message.infrastructure.messaging;

import com.example.hotel.message.application.port.out.KafkaDemoDltIncidentStore;
import com.example.hotel.message.application.port.out.KafkaDemoIdempotencyStore;
import com.example.hotel.message.application.port.out.KafkaDemoMessagePublisher;
import com.example.hotel.message.application.port.out.KafkaDemoTopology;
import com.example.hotel.message.application.result.KafkaDemoTransactionResult;
import com.example.hotel.message.application.service.KafkaDemoConsumeApplicationService;
import com.example.hotel.message.application.service.KafkaDemoMetrics;
import com.example.hotel.message.domain.model.KafkaDemoDltIncident;
import com.example.hotel.message.domain.model.KafkaDemoMessage;
import com.example.hotel.message.infrastructure.config.KafkaDemoConfig;
import com.example.hotel.message.infrastructure.persistence.KafkaDemoDltIncidentRepository;
import com.example.hotel.message.infrastructure.persistence.KafkaDemoIdempotencyRepository;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(
        classes = KafkaDemoIntegrationTest.TestApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "spring.main.web-application-type=none",
                "spring.cloud.discovery.enabled=false",
                "spring.cloud.nacos.discovery.enabled=false",
                "spring.cloud.nacos.config.enabled=false",
                "spring.cloud.nacos.config.import-check.enabled=false",
                "logging.level.kafka=WARN",
                "logging.level.org.apache.kafka=WARN",
                "logging.level.org.springframework.kafka=WARN",
                "spring.datasource.url=jdbc:h2:mem:kafka-demo-it;MODE=MySQL;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false",
                "spring.datasource.driver-class-name=org.h2.Driver",
                "spring.sql.init.mode=always",
                "spring.sql.init.schema-locations=classpath:schema.sql",
                "hotel.kafka.demo.topic=hotel.kafka.demo.orders",
                "hotel.kafka.demo.dead-letter-topic=hotel.kafka.demo.orders.DLT",
                "hotel.kafka.demo.partitions=3",
                "hotel.kafka.demo.replicas=1",
                "hotel.kafka.demo.listener-concurrency=1",
                "hotel.kafka.demo.retry-backoff-ms=10",
                "hotel.kafka.demo.retry-max-retries=1",
                "hotel.kafka.demo.transaction-id-prefix=kafka-demo-it-"
        })
@EmbeddedKafka(
        kraft = true,
        partitions = 3,
        topics = {"hotel.kafka.demo.orders", "hotel.kafka.demo.orders.DLT"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers",
        brokerProperties = {
                "transaction.state.log.replication.factor=1",
                "transaction.state.log.min.isr=1",
                "offsets.topic.replication.factor=1"
        })
@DirtiesContext
class KafkaDemoIntegrationTest {

    private final KafkaDemoMessagePublisher publisher;
    private final KafkaDemoTopology topology;
    private final KafkaDemoMetrics metrics;
    private final KafkaDemoIdempotencyStore idempotencyStore;
    private final KafkaDemoDltIncidentStore dltIncidentStore;

    @Autowired
    KafkaDemoIntegrationTest(KafkaDemoMessagePublisher publisher,
                             KafkaDemoTopology topology,
                             KafkaDemoMetrics metrics,
                             KafkaDemoIdempotencyStore idempotencyStore,
                             KafkaDemoDltIncidentStore dltIncidentStore) {
        this.publisher = publisher;
        this.topology = topology;
        this.metrics = metrics;
        this.idempotencyStore = idempotencyStore;
        this.dltIncidentStore = dltIncidentStore;
    }

    @Test
    void verifiesPartitioningPersistentIdempotencyPoisonDltAndTransactions() {
        String partitionZeroKey = topology.keyForPartition(0);
        String partitionOneKey = topology.keyForPartition(1);
        String partitionTwoKey = topology.keyForPartition(2);

        assertThat(List.of(partitionZeroKey, partitionOneKey, partitionTwoKey))
                .doesNotHaveDuplicates();
        assertThat(publisher.publish(topology.demoTopic(),
                message("KMSG-NORMAL", "ORD-NORMAL", partitionZeroKey, false),
                "integration-test").partition()).isZero();

        publisher.publish(topology.demoTopic(),
                message("KMSG-DUP", "ORD-DUP-1", partitionOneKey, false),
                "first");
        publisher.publish(topology.demoTopic(),
                message("KMSG-DUP", "ORD-DUP-2", partitionOneKey, false),
                "duplicate");

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(idempotencyStore.recentProcessedMessageIds(20))
                    .contains("KMSG-NORMAL", "KMSG-DUP");
            assertThat(metrics.snapshot().duplicated()).isGreaterThanOrEqualTo(1);
        });

        publisher.publishRaw(
                topology.demoTopic(),
                "KMSG-POISON",
                partitionTwoKey,
                "{\"messageId\":\"KMSG-POISON\",\"amount\":broken-json"
                        .getBytes(StandardCharsets.UTF_8),
                "poison integration test");

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            List<KafkaDemoDltIncident> incidents = dltIncidentStore.recent(20);
            assertThat(incidents).anySatisfy(incident -> {
                assertThat(incident.poisonPayload()).isTrue();
                assertThat(incident.payloadPreview()).contains("broken-json");
                assertThat(incident.originalTopic()).isEqualTo(topology.demoTopic());
            });
        });

        long consumedBeforeTransactions = metrics.snapshot().consumed();
        KafkaDemoTransactionResult aborted = publisher.publishTransaction(
                topology.demoTopic(),
                List.of(
                        message("KMSG-TX-A1", "ORD-TX-A1", partitionZeroKey, false),
                        message("KMSG-TX-A2", "ORD-TX-A2", partitionZeroKey, false)),
                true);
        assertThat(aborted.committed()).isFalse();

        KafkaDemoTransactionResult committed = publisher.publishTransaction(
                topology.demoTopic(),
                List.of(
                        message("KMSG-TX-C1", "ORD-TX-C1", partitionZeroKey, false),
                        message("KMSG-TX-C2", "ORD-TX-C2", partitionZeroKey, false)),
                false);
        assertThat(committed.committed()).isTrue();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(idempotencyStore.recentProcessedMessageIds(50))
                    .contains("KMSG-TX-C1", "KMSG-TX-C2")
                    .doesNotContain("KMSG-TX-A1", "KMSG-TX-A2");
            assertThat(metrics.snapshot().consumed())
                    .isGreaterThanOrEqualTo(consumedBeforeTransactions + 2);
        });
    }

    private KafkaDemoMessage message(String messageId,
                                     String orderId,
                                     String key,
                                     boolean fail) {
        return new KafkaDemoMessage(
                messageId,
                orderId,
                1L,
                101L,
                new BigDecimal("499.00"),
                key,
                fail,
                Instant.now());
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    @Import({
            KafkaDemoConfig.class,
            KafkaDemoMetrics.class,
            KafkaDemoConsumeApplicationService.class,
            KafkaDemoPublisherAdapter.class,
            KafkaDemoConsumerAdapter.class,
            KafkaDemoIdempotencyRepository.class,
            KafkaDemoDltIncidentRepository.class
    })
    static class TestApplication {
    }
}
