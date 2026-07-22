package com.example.hotel.message.application.service;

import com.example.hotel.message.application.port.out.RabbitMqDlqIncidentStore;
import com.example.hotel.message.application.port.out.RabbitMqMessagePublisher;
import com.example.hotel.message.application.port.out.RabbitMqOutboxStore;
import com.example.hotel.message.application.port.out.RabbitMqTopology;
import com.example.hotel.message.application.result.RabbitMqDemoPublishResult;
import com.example.hotel.message.application.result.RabbitMqDemoStatus;
import com.example.hotel.message.application.result.RabbitMqDlqResolveResult;
import com.example.hotel.message.domain.model.RabbitMqBookingMessage;
import com.example.hotel.message.domain.model.RabbitMqDlqIncident;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * RabbitMQ 教学用例。这里描述“发布、查询、补偿”，不直接操作 RabbitTemplate 或 Channel。
 */
@Service
public class RabbitMqDemoApplicationService {

    private static final int MAX_RECENT_OUTBOX_MESSAGES = 20;
    private static final int MAX_RECENT_DLQ_INCIDENTS = 20;

    private final RabbitMqMessagePublisher messagePublisher;
    private final RabbitMqOutboxStore outboxStore;
    private final RabbitMqDlqIncidentStore dlqIncidentStore;
    private final RabbitMqTopology topology;
    private final RabbitMqDemoMetrics metrics;

    public RabbitMqDemoApplicationService(RabbitMqMessagePublisher messagePublisher,
                                          RabbitMqOutboxStore outboxStore,
                                          RabbitMqDlqIncidentStore dlqIncidentStore,
                                          RabbitMqTopology topology,
                                          RabbitMqDemoMetrics metrics) {
        this.messagePublisher = messagePublisher;
        this.outboxStore = outboxStore;
        this.dlqIncidentStore = dlqIncidentStore;
        this.topology = topology;
        this.metrics = metrics;
    }

    public RabbitMqDemoPublishResult publishNormal(String messageId) {
        return publish(newMessage(messageId, false), topology.bookingExchange(), topology.bookingRoutingKey(),
                "normal persistent message, consumer will ack after successful processing");
    }

    public List<RabbitMqDemoPublishResult> publishDuplicate(String messageId) {
        String duplicateMessageId = normalizeMessageId(messageId);
        RabbitMqBookingMessage first = newMessage(duplicateMessageId, false);
        RabbitMqBookingMessage second = new RabbitMqBookingMessage(
                duplicateMessageId,
                "ORD-" + UUID.randomUUID().toString().substring(0, 8),
                1L,
                101L,
                new BigDecimal("399.00"),
                false,
                Instant.now());
        return List.of(
                publish(first, topology.bookingExchange(), topology.bookingRoutingKey(),
                        "first message for idempotency demo"),
                publish(second, topology.bookingExchange(), topology.bookingRoutingKey(),
                        "same messageId, consumer should ack and ignore"));
    }

    public RabbitMqDemoPublishResult publishDeadLetter(String messageId) {
        return publish(newMessage(messageId, true), topology.bookingExchange(), topology.bookingRoutingKey(),
                "consumer will nack without requeue, RabbitMQ routes it to the DLQ");
    }

    public RabbitMqDemoPublishResult publishUnroutable(String messageId) {
        return publish(newMessage(messageId, false), topology.bookingExchange(), "hotel.booking.unroutable",
                "mandatory publish with no matching binding, return callback records the problem");
    }

    public RabbitMqDemoPublishResult publishTopic(String messageId, String routingKey) {
        return publish(newMessage(messageId, false), topology.topicExchange(), normalizeTopicRoutingKey(routingKey),
                "topic wildcard demo: hotel.booking.* matches one word, hotel.booking.# matches multiple words");
    }

    public RabbitMqDemoPublishResult publishFanout(String messageId) {
        return publish(newMessage(messageId, false), topology.fanoutExchange(), "",
                "fanout broadcast demo: same message is copied to every bound queue");
    }

    public RabbitMqDemoStatus status() {
        RabbitMqDemoMetrics.Snapshot snapshot = metrics.snapshot();
        return new RabbitMqDemoStatus(
                topology.bookingExchange(),
                topology.bookingQueue(),
                topology.deadLetterExchange(),
                topology.deadLetterQueue(),
                topology.topicExchange(),
                topology.topicBindings(),
                topology.fanoutExchange(),
                topology.fanoutQueues(),
                snapshot.published(),
                snapshot.publishAcked(),
                snapshot.returned(),
                snapshot.consumed(),
                snapshot.duplicated(),
                snapshot.rejectedToDeadLetter(),
                snapshot.deadLetterResolved(),
                snapshot.topicWildcardConsumed(),
                snapshot.fanoutBroadcastConsumed(),
                outboxStore.countByStatus(),
                outboxStore.recent(MAX_RECENT_OUTBOX_MESSAGES),
                dlqIncidentStore.countByStatus(),
                dlqIncidentStore.recent(MAX_RECENT_DLQ_INCIDENTS),
                snapshot.processedMessageIds(),
                snapshot.recentEvents());
    }

    public List<RabbitMqDlqIncident> dlqIncidents(String status) {
        if (status == null || status.isBlank()) {
            return dlqIncidentStore.recent(MAX_RECENT_DLQ_INCIDENTS);
        }
        return dlqIncidentStore.findByStatus(status, MAX_RECENT_DLQ_INCIDENTS);
    }

    public RabbitMqDlqResolveResult resolveDlqIncident(String incidentId, String compensationNote) {
        RabbitMqDlqIncident incident = dlqIncidentStore.findById(incidentId);
        String note = normalizeCompensationNote(compensationNote);
        if (incident == null) {
            return new RabbitMqDlqResolveResult(
                    false, incidentId, topology.deadLetterQueue(), null, null, null,
                    null, null, "not-found", note,
                    "no DLQ incident found by incidentId", Instant.now());
        }

        if ("RESOLVED".equals(incident.status())) {
            return new RabbitMqDlqResolveResult(
                    true, incident.incidentId(), incident.queueName(), incident.messageId(), incident.orderId(),
                    incident.deadLetterReason(), incident.status(), incident.status(), "already-resolved",
                    incident.compensationNote(),
                    "incident was already resolved, do not run compensation again", incident.resolvedAt());
        }

        if (!dlqIncidentStore.markResolving(incidentId)) {
            RabbitMqDlqIncident latest = dlqIncidentStore.findById(incidentId);
            return new RabbitMqDlqResolveResult(
                    true, latest.incidentId(), latest.queueName(), latest.messageId(), latest.orderId(),
                    latest.deadLetterReason(), incident.status(), latest.status(), "skip", latest.compensationNote(),
                    "incident is not PENDING or FAILED, another operator may be processing it", Instant.now());
        }

        try {
            String idempotencyKey = incident.messageId() == null ? incident.incidentId() : incident.messageId();
            boolean firstCompensation = metrics.markProcessed(idempotencyKey);
            String detail = firstCompensation
                    ? "manual compensation task finished by incidentId"
                    : "business message was already compensated before, mark incident resolved without side effects";
            dlqIncidentStore.markResolved(incidentId, note);
            metrics.deadLetterResolved();
            metrics.record(idempotencyKey, incident.orderId(), "dlq-incident-resolved",
                    detail + "; incidentId=" + incidentId + "; note=" + note);

            RabbitMqDlqIncident resolved = dlqIncidentStore.findById(incidentId);
            return new RabbitMqDlqResolveResult(
                    true, resolved.incidentId(), resolved.queueName(), resolved.messageId(), resolved.orderId(),
                    resolved.deadLetterReason(), incident.status(), resolved.status(), "mark-incident-resolved", note,
                    "DLQ message was already ACKed after creating the PENDING incident; this call resolves the stored compensation task",
                    resolved.resolvedAt());
        } catch (RuntimeException exception) {
            dlqIncidentStore.markFailed(incidentId, exception.getMessage());
            throw exception;
        }
    }

    private RabbitMqDemoPublishResult publish(RabbitMqBookingMessage message,
                                              String exchange,
                                              String routingKey,
                                              String note) {
        return messagePublisher.publish(message, exchange, routingKey, note);
    }

    private RabbitMqBookingMessage newMessage(String messageId, boolean fail) {
        return new RabbitMqBookingMessage(
                normalizeMessageId(messageId),
                "ORD-" + UUID.randomUUID().toString().substring(0, 8),
                1L,
                101L,
                new BigDecimal("399.00"),
                fail,
                Instant.now());
    }

    private String normalizeMessageId(String messageId) {
        return messageId == null || messageId.isBlank()
                ? "MSG-" + UUID.randomUUID().toString().substring(0, 8)
                : messageId;
    }

    private String normalizeTopicRoutingKey(String routingKey) {
        return routingKey == null || routingKey.isBlank() ? topology.bookingRoutingKey() : routingKey;
    }

    private String normalizeCompensationNote(String compensationNote) {
        return compensationNote == null || compensationNote.isBlank()
                ? "manual troubleshooting or compensation has been completed"
                : compensationNote;
    }
}
