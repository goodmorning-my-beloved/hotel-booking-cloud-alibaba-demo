package com.example.hotel.message.infrastructure.messaging;

import com.example.hotel.message.application.port.out.RabbitMqDlqIncidentStore;
import com.example.hotel.message.application.port.out.RabbitMqTopology;
import com.example.hotel.message.application.service.RabbitMqDemoMetrics;
import com.example.hotel.message.domain.model.RabbitMqBookingMessage;
import com.example.hotel.message.domain.model.RabbitMqDlqIncident;
import com.example.hotel.message.infrastructure.config.RabbitMqDemoProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * RabbitMQ 入站适配器：负责反序列化、ACK/NACK 和队列绑定，再把处理结果写入应用状态。
 */
@Component
public class RabbitMqConsumerAdapter {

    private static final Logger log = LoggerFactory.getLogger(RabbitMqConsumerAdapter.class);

    private final ObjectMapper objectMapper;
    private final RabbitMqDlqIncidentStore dlqIncidentStore;
    private final RabbitMqDemoMetrics metrics;
    private final RabbitMqTopology topology;
    private final RabbitMqDemoProperties properties;

    public RabbitMqConsumerAdapter(ObjectMapper objectMapper,
                                   RabbitMqDlqIncidentStore dlqIncidentStore,
                                   RabbitMqDemoMetrics metrics,
                                   RabbitMqTopology topology,
                                   RabbitMqDemoProperties properties) {
        this.objectMapper = objectMapper;
        this.dlqIncidentStore = dlqIncidentStore;
        this.metrics = metrics;
        this.topology = topology;
        this.properties = properties;
    }

    @RabbitListener(queues = "${hotel.rabbitmq.demo.booking-queue:hotel.booking.created.queue}", ackMode = "MANUAL")
    public void consume(RabbitMqBookingMessage payload, Message rawMessage, Channel channel) throws IOException {
        long deliveryTag = rawMessage.getMessageProperties().getDeliveryTag();
        String messageId = rawMessage.getMessageProperties().getMessageId();
        if (messageId == null || messageId.isBlank()) {
            messageId = payload.messageId();
        }

        if (payload.fail()) {
            metrics.rejectedToDeadLetter();
            record(messageId, payload.orderId(), "dead-lettered", "simulated failure, basicNack requeue=false");
            channel.basicNack(deliveryTag, false, false);
            return;
        }

        if (!metrics.markProcessed(messageId)) {
            metrics.duplicated();
            record(messageId, payload.orderId(), "duplicate", "messageId already processed, ack without side effects");
            channel.basicAck(deliveryTag, false);
            return;
        }

        metrics.consumed();
        record(messageId, payload.orderId(), "consumed", "business action finished, manual ack sent");
        channel.basicAck(deliveryTag, false);
    }

    @RabbitListener(queues = "${hotel.rabbitmq.demo.dead-letter-queue:hotel.booking.created.dlq}", ackMode = "MANUAL")
    public void captureDeadLetterIncident(Message rawMessage, Channel channel) throws IOException {
        long deliveryTag = rawMessage.getMessageProperties().getDeliveryTag();
        try {
            String payloadJson = new String(rawMessage.getBody(), StandardCharsets.UTF_8);
            RabbitMqBookingMessage payload = readBookingMessageOrNull(payloadJson);
            String messageId = rawMessage.getMessageProperties().getMessageId();
            if ((messageId == null || messageId.isBlank()) && payload != null) {
                messageId = payload.messageId();
            }
            String orderId = payload == null ? null : payload.orderId();
            String sourceKey = dlqSourceKey(rawMessage, payloadJson, messageId);
            RabbitMqDlqIncident incident = dlqIncidentStore.insertPendingIfAbsent(
                    "INC-" + UUID.randomUUID().toString().substring(0, 8),
                    sourceKey,
                    topology.deadLetterQueue(),
                    messageId,
                    orderId,
                    firstDeathReason(rawMessage.getMessageProperties().getHeaders()),
                    payloadJson,
                    headersJson(rawMessage.getMessageProperties().getHeaders()));

            channel.basicAck(deliveryTag, false);
            record(messageId == null ? incident.incidentId() : messageId,
                    orderId,
                    "dlq-incident-pending",
                    "DLQ listener stored PENDING incidentId=" + incident.incidentId());
        } catch (Exception exception) {
            channel.basicNack(deliveryTag, false, true);
            throw new IllegalStateException("Failed to store DLQ incident, message has been requeued", exception);
        }
    }

    @RabbitListener(queues = "${hotel.rabbitmq.demo.topic-single-word-queue:hotel.booking.topic.single-word.queue}", ackMode = "MANUAL")
    public void consumeTopicSingleWord(RabbitMqBookingMessage payload,
                                       Message rawMessage,
                                       Channel channel) throws IOException {
        long deliveryTag = rawMessage.getMessageProperties().getDeliveryTag();
        String routingKey = rawMessage.getMessageProperties().getReceivedRoutingKey();
        metrics.topicWildcardConsumed();
        record(payload.messageId(), payload.orderId(), "topic-single-word-consumed",
                "queue=" + properties.getTopicSingleWordQueue()
                        + "; binding=" + properties.getTopicSingleWordPattern()
                        + "; routingKey=" + routingKey);
        channel.basicAck(deliveryTag, false);
    }

    @RabbitListener(queues = "${hotel.rabbitmq.demo.topic-multi-word-queue:hotel.booking.topic.multi-word.queue}", ackMode = "MANUAL")
    public void consumeTopicMultiWord(RabbitMqBookingMessage payload,
                                      Message rawMessage,
                                      Channel channel) throws IOException {
        long deliveryTag = rawMessage.getMessageProperties().getDeliveryTag();
        String routingKey = rawMessage.getMessageProperties().getReceivedRoutingKey();
        metrics.topicWildcardConsumed();
        record(payload.messageId(), payload.orderId(), "topic-multi-word-consumed",
                "queue=" + properties.getTopicMultiWordQueue()
                        + "; binding=" + properties.getTopicMultiWordPattern()
                        + "; routingKey=" + routingKey);
        channel.basicAck(deliveryTag, false);
    }

    @RabbitListener(queues = "${hotel.rabbitmq.demo.fanout-sms-queue:hotel.booking.fanout.sms.queue}", ackMode = "MANUAL")
    public void consumeFanoutSms(RabbitMqBookingMessage payload,
                                 Message rawMessage,
                                 Channel channel) throws IOException {
        long deliveryTag = rawMessage.getMessageProperties().getDeliveryTag();
        metrics.fanoutBroadcastConsumed();
        record(payload.messageId(), payload.orderId(), "fanout-sms-consumed",
                "queue=" + properties.getFanoutSmsQueue() + "; routing key ignored by fanout exchange");
        channel.basicAck(deliveryTag, false);
    }

    @RabbitListener(queues = "${hotel.rabbitmq.demo.fanout-points-queue:hotel.booking.fanout.points.queue}", ackMode = "MANUAL")
    public void consumeFanoutPoints(RabbitMqBookingMessage payload,
                                    Message rawMessage,
                                    Channel channel) throws IOException {
        long deliveryTag = rawMessage.getMessageProperties().getDeliveryTag();
        metrics.fanoutBroadcastConsumed();
        record(payload.messageId(), payload.orderId(), "fanout-points-consumed",
                "queue=" + properties.getFanoutPointsQueue() + "; same broadcast message copy");
        channel.basicAck(deliveryTag, false);
    }

    private RabbitMqBookingMessage readBookingMessageOrNull(String payloadJson) {
        try {
            return objectMapper.readValue(payloadJson, RabbitMqBookingMessage.class);
        } catch (IOException exception) {
            return null;
        }
    }

    private String headersJson(Map<String, Object> headers) throws IOException {
        if (headers == null || headers.isEmpty()) {
            return "{}";
        }
        Map<String, String> simpleHeaders = new LinkedHashMap<>();
        headers.forEach((key, value) -> simpleHeaders.put(key, value == null ? null : value.toString()));
        return objectMapper.writeValueAsString(simpleHeaders);
    }

    private String dlqSourceKey(Message rawMessage, String payloadJson, String messageId) {
        Object outboxId = rawMessage.getMessageProperties().getHeaders().get("x-outbox-id");
        String stableId = outboxId == null ? null : outboxId.toString();
        if (stableId == null || stableId.isBlank()) {
            stableId = messageId;
        }
        if (stableId == null || stableId.isBlank()) {
            stableId = sha256(payloadJson);
        }
        return topology.deadLetterQueue() + ":" + stableId;
    }

    private String sha256(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is not available", exception);
        }
    }

    private String firstDeathReason(Map<String, Object> headers) {
        if (headers == null) {
            return null;
        }
        Object reason = headers.get("x-first-death-reason");
        return reason == null ? null : reason.toString();
    }

    private void record(String messageId, String orderId, String status, String detail) {
        metrics.record(messageId, orderId, status, detail);
        log.info("RabbitMQ demo message {}: messageId={}, orderId={}, detail={}", status, messageId, orderId, detail);
    }
}
