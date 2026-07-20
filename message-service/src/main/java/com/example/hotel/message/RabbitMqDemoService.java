package com.example.hotel.message;

import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class RabbitMqDemoService {

    private static final Logger log = LoggerFactory.getLogger(RabbitMqDemoService.class);
    private static final int MAX_RECENT_EVENTS = 50;

    private final RabbitTemplate rabbitTemplate;
    private final AtomicLong published = new AtomicLong();
    private final AtomicLong publishAcked = new AtomicLong();
    private final AtomicLong returned = new AtomicLong();
    private final AtomicLong consumed = new AtomicLong();
    private final AtomicLong duplicated = new AtomicLong();
    private final AtomicLong rejectedToDeadLetter = new AtomicLong();
    private final Set<String> processedMessageIds = ConcurrentHashMap.newKeySet();
    private final List<RabbitMqConsumedEvent> recentEvents = new ArrayList<>();

    public RabbitMqDemoService(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
        this.rabbitTemplate.setReturnsCallback(returnedMessage -> {
            returned.incrementAndGet();
            log.warn("RabbitMQ returned message. exchange={}, routingKey={}, replyCode={}, replyText={}",
                    returnedMessage.getExchange(),
                    returnedMessage.getRoutingKey(),
                    returnedMessage.getReplyCode(),
                    returnedMessage.getReplyText());
        });
    }

    public RabbitMqDemoPublishResult publishNormal(String messageId) {
        RabbitMqBookingMessage message = newMessage(messageId, false);
        return publish(message, RabbitMqDemoConfig.BOOKING_ROUTING_KEY,
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
                publish(first, RabbitMqDemoConfig.BOOKING_ROUTING_KEY, "first message for idempotency demo"),
                publish(second, RabbitMqDemoConfig.BOOKING_ROUTING_KEY, "same messageId, consumer should ack and ignore")
        );
    }

    public RabbitMqDemoPublishResult publishDeadLetter(String messageId) {
        RabbitMqBookingMessage message = newMessage(messageId, true);
        return publish(message, RabbitMqDemoConfig.BOOKING_ROUTING_KEY,
                "consumer will nack without requeue, RabbitMQ routes it to the DLQ");
    }

    public RabbitMqDemoPublishResult publishUnroutable(String messageId) {
        RabbitMqBookingMessage message = newMessage(messageId, false);
        return publish(message, "hotel.booking.unroutable",
                "mandatory publish with no matching binding, return callback records the problem");
    }

    public RabbitMqDemoStatus status() {
        List<String> ids = processedMessageIds.stream()
                .sorted()
                .toList();
        List<RabbitMqConsumedEvent> events;
        synchronized (recentEvents) {
            events = recentEvents.stream()
                    .sorted(Comparator.comparing(RabbitMqConsumedEvent::receivedAt).reversed())
                    .toList();
        }
        return new RabbitMqDemoStatus(
                RabbitMqDemoConfig.BOOKING_EXCHANGE,
                RabbitMqDemoConfig.BOOKING_QUEUE,
                RabbitMqDemoConfig.BOOKING_DLX,
                RabbitMqDemoConfig.BOOKING_DLQ,
                published.get(),
                publishAcked.get(),
                returned.get(),
                consumed.get(),
                duplicated.get(),
                rejectedToDeadLetter.get(),
                ids,
                events);
    }

    @RabbitListener(queues = RabbitMqDemoConfig.BOOKING_QUEUE, ackMode = "MANUAL")
    public void consume(RabbitMqBookingMessage payload, Message rawMessage, Channel channel) throws IOException {
        long deliveryTag = rawMessage.getMessageProperties().getDeliveryTag();
        String messageId = rawMessage.getMessageProperties().getMessageId();
        if (messageId == null || messageId.isBlank()) {
            messageId = payload.messageId();
        }

        if (payload.fail()) {
            rejectedToDeadLetter.incrementAndGet();
            record(messageId, payload.orderId(), "dead-lettered", "simulated failure, basicNack requeue=false");
            channel.basicNack(deliveryTag, false, false);
            return;
        }

        if (!processedMessageIds.add(messageId)) {
            duplicated.incrementAndGet();
            record(messageId, payload.orderId(), "duplicate", "messageId already processed, ack without side effects");
            channel.basicAck(deliveryTag, false);
            return;
        }

        consumed.incrementAndGet();
        record(messageId, payload.orderId(), "consumed", "business action finished, manual ack sent");
        channel.basicAck(deliveryTag, false);
    }

    private RabbitMqDemoPublishResult publish(RabbitMqBookingMessage message, String routingKey, String note) {
        CorrelationData correlationData = new CorrelationData(message.messageId());
        try {
            rabbitTemplate.convertAndSend(RabbitMqDemoConfig.BOOKING_EXCHANGE, routingKey, message, amqpMessage -> {
                amqpMessage.getMessageProperties().setMessageId(message.messageId());
                amqpMessage.getMessageProperties().setDeliveryMode(MessageDeliveryMode.PERSISTENT);
                amqpMessage.getMessageProperties().setHeader("x-demo-feature", note);
                return amqpMessage;
            }, correlationData);
            published.incrementAndGet();
            String confirm = waitForConfirm(correlationData);
            return new RabbitMqDemoPublishResult(
                    message.messageId(),
                    message.orderId(),
                    RabbitMqDemoConfig.BOOKING_EXCHANGE,
                    routingKey,
                    true,
                    confirm,
                    note);
        } catch (AmqpException ex) {
            throw new IllegalStateException("RabbitMQ publish failed: " + ex.getMessage(), ex);
        }
    }

    private String waitForConfirm(CorrelationData correlationData) {
        try {
            CorrelationData.Confirm confirm = correlationData.getFuture().get(5, TimeUnit.SECONDS);
            if (confirm.isAck()) {
                publishAcked.incrementAndGet();
                return "broker ack";
            }
            return "broker nack: " + confirm.getReason();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return "confirm interrupted";
        } catch (TimeoutException ex) {
            return "confirm timeout";
        } catch (Exception ex) {
            return "confirm failed: " + ex.getMessage();
        }
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
        if (messageId == null || messageId.isBlank()) {
            return "MSG-" + UUID.randomUUID().toString().substring(0, 8);
        }
        return messageId;
    }

    private void record(String messageId, String orderId, String status, String detail) {
        RabbitMqConsumedEvent event = new RabbitMqConsumedEvent(messageId, orderId, status, detail, Instant.now());
        synchronized (recentEvents) {
            recentEvents.add(event);
            if (recentEvents.size() > MAX_RECENT_EVENTS) {
                recentEvents.remove(0);
            }
        }
        log.info("RabbitMQ demo message {}: messageId={}, orderId={}, detail={}", status, messageId, orderId, detail);
    }
}
