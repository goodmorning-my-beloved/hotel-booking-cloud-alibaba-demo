package com.example.hotel.message.infrastructure.messaging;

import com.example.hotel.message.application.service.KafkaDemoMetrics;
import com.example.hotel.message.domain.model.KafkaDemoMessage;
import com.example.hotel.message.infrastructure.config.KafkaDemoConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class KafkaDemoConsumerAdapter {

    private static final Logger log = LoggerFactory.getLogger(KafkaDemoConsumerAdapter.class);

    private final KafkaDemoMetrics metrics;

    public KafkaDemoConsumerAdapter(KafkaDemoMetrics metrics) {
        this.metrics = metrics;
    }

    @KafkaListener(
            topics = KafkaDemoConfig.DEMO_TOPIC,
            groupId = KafkaDemoConfig.PRIMARY_GROUP,
            containerFactory = "kafkaDemoListenerContainerFactory")
    public void consumePrimary(ConsumerRecord<String, KafkaDemoMessage> record, Acknowledgment acknowledgment) {
        KafkaDemoMessage payload = record.value();
        String consumerName = Thread.currentThread().getName();
        if (payload.fail()) {
            metrics.failedForRetry();
            record(payload, record, KafkaDemoConfig.PRIMARY_GROUP, consumerName, "retrying",
                    "simulated failure; DefaultErrorHandler will retry and then publish to DLT");
            throw new IllegalStateException("simulated Kafka consumer failure for messageId=" + payload.messageId());
        }

        if (!metrics.markProcessed(payload.messageId())) {
            metrics.duplicated();
            record(payload, record, KafkaDemoConfig.PRIMARY_GROUP, consumerName, "duplicate",
                    "messageId already processed, commit offset without duplicate side effects");
            acknowledgment.acknowledge();
            return;
        }

        metrics.consumed();
        record(payload, record, KafkaDemoConfig.PRIMARY_GROUP, consumerName, "consumed",
                "business action finished, manual offset commit requested");
        acknowledgment.acknowledge();
    }

    @KafkaListener(
            topics = KafkaDemoConfig.DEMO_TOPIC,
            groupId = KafkaDemoConfig.AUDIT_GROUP,
            containerFactory = "kafkaDemoListenerContainerFactory")
    public void consumeAuditCopy(ConsumerRecord<String, KafkaDemoMessage> record, Acknowledgment acknowledgment) {
        KafkaDemoMessage payload = record.value();
        metrics.auditConsumed();
        record(payload, record, KafkaDemoConfig.AUDIT_GROUP, Thread.currentThread().getName(), "audit-copy",
                "different consumer group receives its own copy and keeps independent offsets");
        acknowledgment.acknowledge();
    }

    @KafkaListener(
            topics = KafkaDemoConfig.DEMO_DLT_TOPIC,
            groupId = KafkaDemoConfig.DLT_GROUP,
            containerFactory = "kafkaDemoListenerContainerFactory")
    public void consumeDeadLetter(ConsumerRecord<String, KafkaDemoMessage> record, Acknowledgment acknowledgment) {
        KafkaDemoMessage payload = record.value();
        metrics.deadLettered();
        record(payload, record, KafkaDemoConfig.DLT_GROUP, Thread.currentThread().getName(), "dead-lettered",
                "message arrived in DLT after configured retries were exhausted");
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
}
