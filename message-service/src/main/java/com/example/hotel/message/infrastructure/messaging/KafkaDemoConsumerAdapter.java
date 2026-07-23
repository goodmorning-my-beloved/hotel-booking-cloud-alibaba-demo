package com.example.hotel.message.infrastructure.messaging;

import com.example.hotel.message.application.service.KafkaDemoMetrics;
import com.example.hotel.message.application.port.out.KafkaDemoTopology;
import com.example.hotel.message.domain.model.KafkaDemoMessage;
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
    private final KafkaDemoTopology topology;

    public KafkaDemoConsumerAdapter(KafkaDemoMetrics metrics, KafkaDemoTopology topology) {
        this.metrics = metrics;
        this.topology = topology;
    }

    @KafkaListener(
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

        if (!metrics.markProcessed(payload.messageId())) {
            metrics.duplicated();
            record(payload, record, topology.primaryConsumerGroup(), consumerName, "duplicate",
                    "messageId already processed, commit offset without duplicate side effects");
            acknowledgment.acknowledge();
            return;
        }

        metrics.consumed();
        record(payload, record, topology.primaryConsumerGroup(), consumerName, "consumed",
                "business action finished, manual offset commit requested");
        acknowledgment.acknowledge();
    }

    @KafkaListener(
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
            topics = "${hotel.kafka.demo.dead-letter-topic:hotel.kafka.demo.orders.DLT}",
            groupId = "${hotel.kafka.demo.consumer-groups.dead-letter:hotel-kafka-demo-dlt}",
            containerFactory = "kafkaDemoListenerContainerFactory")
    public void consumeDeadLetter(ConsumerRecord<String, KafkaDemoMessage> record, Acknowledgment acknowledgment) {
        KafkaDemoMessage payload = record.value();
        metrics.deadLettered();
        record(payload, record, topology.deadLetterConsumerGroup(), Thread.currentThread().getName(), "dead-lettered",
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
