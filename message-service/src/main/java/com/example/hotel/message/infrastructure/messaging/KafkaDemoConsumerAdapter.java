package com.example.hotel.message.infrastructure.messaging;

import com.example.hotel.message.application.port.out.KafkaDemoDltIncidentStore;
import com.example.hotel.message.application.service.KafkaDemoMetrics;
import com.example.hotel.message.application.service.KafkaDemoConsumeApplicationService;
import com.example.hotel.message.application.port.out.KafkaDemoTopology;
import com.example.hotel.message.domain.model.KafkaDemoDltIncident;
import com.example.hotel.message.domain.model.KafkaDemoMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.UUID;

@Component
public class KafkaDemoConsumerAdapter {

    public static final String PRIMARY_LISTENER_ID = "kafkaDemoPrimaryListener";
    public static final String AUDIT_LISTENER_ID = "kafkaDemoAuditListener";
    public static final String DLT_LISTENER_ID = "kafkaDemoDltListener";

    private static final Logger log = LoggerFactory.getLogger(KafkaDemoConsumerAdapter.class);

    private final KafkaDemoMetrics metrics;
    private final KafkaDemoTopology topology;
    private final KafkaDemoConsumeApplicationService consumeApplicationService;
    private final KafkaDemoDltIncidentStore dltIncidentStore;
    private final ObjectMapper objectMapper;

    public KafkaDemoConsumerAdapter(KafkaDemoMetrics metrics,
                                    KafkaDemoTopology topology,
                                    KafkaDemoConsumeApplicationService consumeApplicationService,
                                    KafkaDemoDltIncidentStore dltIncidentStore,
                                    ObjectMapper objectMapper) {
        this.metrics = metrics;
        this.topology = topology;
        this.consumeApplicationService = consumeApplicationService;
        this.dltIncidentStore = dltIncidentStore;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            id = PRIMARY_LISTENER_ID,
            topics = "${hotel.kafka.demo.topic:hotel.kafka.demo.orders}",
            groupId = "${hotel.kafka.demo.consumer-groups.primary:hotel-kafka-demo-primary}",
            containerFactory = "kafkaDemoListenerContainerFactory")
    public void consumePrimary(ConsumerRecord<String, KafkaDemoMessage> record, Acknowledgment acknowledgment) {
        KafkaDemoMessage payload = record.value();
        String consumerName = Thread.currentThread().getName();
        if (payload.fail()) {
            metrics.failedForRetry();
            record(payload, record, topology.primaryConsumerGroup(), consumerName, "retrying",
                    "simulated failure; DefaultErrorHandler will retry and then publish to DLT");
            throw new IllegalStateException("simulated Kafka consumer failure for messageId=" + payload.messageId());
        }

        if (!consumeApplicationService.process(payload)) {
            record(payload, record, topology.primaryConsumerGroup(), consumerName, "duplicate",
                    "database unique key proves messageId was already processed; commit without duplicate side effects");
            acknowledgment.acknowledge();
            return;
        }

        record(payload, record, topology.primaryConsumerGroup(), consumerName, "consumed",
                "idempotency row and business projection committed in one DB transaction; offset commit requested");
        acknowledgment.acknowledge();
    }

    @KafkaListener(
            id = AUDIT_LISTENER_ID,
            topics = "${hotel.kafka.demo.topic:hotel.kafka.demo.orders}",
            groupId = "${hotel.kafka.demo.consumer-groups.audit:hotel-kafka-demo-audit}",
            containerFactory = "kafkaDemoListenerContainerFactory")
    public void consumeAuditCopy(ConsumerRecord<String, KafkaDemoMessage> record, Acknowledgment acknowledgment) {
        KafkaDemoMessage payload = record.value();
        metrics.auditConsumed();
        record(payload, record, topology.auditConsumerGroup(), Thread.currentThread().getName(), "audit-copy",
                "different consumer group receives its own copy and keeps independent offsets");
        acknowledgment.acknowledge();
    }

    @KafkaListener(
            id = DLT_LISTENER_ID,
            topics = "${hotel.kafka.demo.dead-letter-topic:hotel.kafka.demo.orders.DLT}",
            groupId = "${hotel.kafka.demo.consumer-groups.dead-letter:hotel-kafka-demo-dlt}",
            containerFactory = "kafkaDemoDltListenerContainerFactory")
    public void consumeDeadLetter(ConsumerRecord<String, byte[]> record, Acknowledgment acknowledgment) {
        byte[] rawPayload = record.value() == null ? new byte[0] : record.value();
        KafkaDemoMessage payload = deserializeDltPayload(rawPayload);
        boolean poisonPayload = payload == null;
        Instant now = Instant.now();
        String sourceKey = record.topic() + "-" + record.partition() + "-" + record.offset();
        KafkaDemoDltIncident incident = new KafkaDemoDltIncident(
                "KDLT-" + UUID.randomUUID().toString().substring(0, 12),
                sourceKey,
                record.topic(),
                record.partition(),
                record.offset(),
                headerString(record, KafkaHeaders.DLT_ORIGINAL_TOPIC),
                headerInt(record, KafkaHeaders.DLT_ORIGINAL_PARTITION),
                headerLong(record, KafkaHeaders.DLT_ORIGINAL_OFFSET),
                payload == null ? null : payload.messageId(),
                payload == null ? record.key() : payload.businessKey(),
                poisonPayload,
                headerString(record, KafkaHeaders.DLT_EXCEPTION_FQCN),
                headerString(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE),
                payloadPreview(rawPayload),
                "PENDING",
                null,
                now,
                now,
                null);
        dltIncidentStore.saveIfAbsent(incident, Base64.getEncoder().encodeToString(rawPayload));
        metrics.deadLettered();
        metrics.record(
                payload == null ? null : payload.messageId(),
                payload == null ? null : payload.orderId(),
                payload == null ? record.key() : payload.businessKey(),
                topology.deadLetterConsumerGroup(),
                Thread.currentThread().getName(),
                record.topic(),
                record.partition(),
                record.offset(),
                poisonPayload ? "poison-dead-lettered" : "dead-lettered",
                poisonPayload
                        ? "raw payload could not be deserialized; persisted as a DLT incident"
                        : "message arrived in DLT after configured retries were exhausted");
        acknowledgment.acknowledge();
    }

    private void record(KafkaDemoMessage payload,
                        ConsumerRecord<String, KafkaDemoMessage> record,
                        String consumerGroup,
                        String consumerName,
                        String status,
                        String detail) {
        metrics.record(
                payload.messageId(),
                payload.orderId(),
                payload.businessKey(),
                consumerGroup,
                consumerName,
                record.topic(),
                record.partition(),
                record.offset(),
                status,
                detail);
        log.info("Kafka demo {}: messageId={}, key={}, group={}, topic={}, partition={}, offset={}",
                status,
                payload.messageId(),
                payload.businessKey(),
                consumerGroup,
                record.topic(),
                record.partition(),
                record.offset());
    }

    private KafkaDemoMessage deserializeDltPayload(byte[] payload) {
        try {
            return objectMapper.readValue(payload, KafkaDemoMessage.class);
        } catch (Exception ignored) {
            return null;
        }
    }

    private String payloadPreview(byte[] payload) {
        String preview = new String(payload, StandardCharsets.UTF_8);
        return preview.length() <= 1000 ? preview : preview.substring(0, 1000);
    }

    private String headerString(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    private Integer headerInt(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null || header.value().length != Integer.BYTES
                ? null
                : ByteBuffer.wrap(header.value()).getInt();
    }

    private Long headerLong(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null || header.value().length != Long.BYTES
                ? null
                : ByteBuffer.wrap(header.value()).getLong();
    }
}
