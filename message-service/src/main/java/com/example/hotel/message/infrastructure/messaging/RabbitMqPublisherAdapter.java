package com.example.hotel.message.infrastructure.messaging;

import com.example.hotel.message.application.port.out.RabbitMqMessagePublisher;
import com.example.hotel.message.application.port.out.RabbitMqOutboxStore;
import com.example.hotel.message.application.result.RabbitMqDemoPublishResult;
import com.example.hotel.message.application.service.RabbitMqDemoMetrics;
import com.example.hotel.message.domain.model.RabbitMqBookingMessage;
import com.example.hotel.message.domain.model.RabbitMqOutboxMessage;
import com.example.hotel.message.domain.model.RabbitMqOutboxPublishTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

@Component
public class RabbitMqPublisherAdapter implements RabbitMqMessagePublisher {

    private static final Logger log = LoggerFactory.getLogger(RabbitMqPublisherAdapter.class);
    private static final int MAX_PUBLISH_ATTEMPTS = 3;
    private static final int RETRY_BATCH_SIZE = 20;
    private static final Duration CONFIRM_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration BASE_RETRY_BACKOFF = Duration.ofSeconds(5);

    private final RabbitTemplate rabbitTemplate;
    private final RabbitMqOutboxStore outboxStore;
    private final RabbitMqDemoMetrics metrics;

    public RabbitMqPublisherAdapter(RabbitTemplate rabbitTemplate,
                                    RabbitMqOutboxStore outboxStore,
                                    RabbitMqDemoMetrics metrics) {
        this.rabbitTemplate = rabbitTemplate;
        this.outboxStore = outboxStore;
        this.metrics = metrics;
        this.rabbitTemplate.setReturnsCallback(returnedMessage -> {
            metrics.returned();
            Object outboxId = returnedMessage.getMessage().getMessageProperties().getHeaders().get("x-outbox-id");
            if (outboxId != null) {
                outboxStore.markReturned(outboxId.toString(),
                        "mandatory return: " + returnedMessage.getReplyCode() + " " + returnedMessage.getReplyText());
            }
            log.warn("RabbitMQ returned message. exchange={}, routingKey={}, replyCode={}, replyText={}",
                    returnedMessage.getExchange(),
                    returnedMessage.getRoutingKey(),
                    returnedMessage.getReplyCode(),
                    returnedMessage.getReplyText());
        });
    }

    @Override
    public RabbitMqDemoPublishResult publish(RabbitMqBookingMessage message,
                                             String exchangeName,
                                             String routingKey,
                                             String note) {
        String outboxId = UUID.randomUUID().toString();
        RabbitMqOutboxPublishTask task = outboxStore.insertNew(
                outboxId, message, exchangeName, routingKey, note, MAX_PUBLISH_ATTEMPTS);
        publishOutboxTask(task);
        RabbitMqOutboxMessage localMessage = outboxStore.findById(outboxId);
        return new RabbitMqDemoPublishResult(
                outboxId,
                message.messageId(),
                message.orderId(),
                exchangeName,
                routingKey,
                true,
                localMessage.status(),
                "async publisher confirm pending, check /rabbitmq/demo/status by outboxId",
                localMessage.attemptCount(),
                note);
    }

    @Override
    public void retryFailures() {
        Instant now = Instant.now();
        outboxStore.findConfirmTimedOut(now.minus(CONFIRM_TIMEOUT), RETRY_BATCH_SIZE)
                .forEach(task -> scheduleRetryOrFail(task.outboxId(), "publisher confirm timeout"));
        outboxStore.findRetryable(now, RETRY_BATCH_SIZE).forEach(this::publishOutboxTask);
    }

    private void publishOutboxTask(RabbitMqOutboxPublishTask task) {
        outboxStore.markWaitConfirm(task.outboxId());
        CorrelationData correlationData = new CorrelationData(task.outboxId());
        registerAsyncConfirm(task.outboxId(), correlationData);
        try {
            rabbitTemplate.convertAndSend(task.exchangeName(), task.routingKey(), task.payload(), amqpMessage -> {
                amqpMessage.getMessageProperties().setMessageId(task.payload().messageId());
                amqpMessage.getMessageProperties().setDeliveryMode(MessageDeliveryMode.PERSISTENT);
                amqpMessage.getMessageProperties().setHeader("x-outbox-id", task.outboxId());
                amqpMessage.getMessageProperties().setHeader("x-demo-feature", task.demoNote());
                return amqpMessage;
            }, correlationData);
            metrics.published();
        } catch (AmqpException exception) {
            scheduleRetryOrFail(task.outboxId(), "send exception: " + exception.getMessage());
        }
    }

    private void registerAsyncConfirm(String outboxId, CorrelationData correlationData) {
        correlationData.getFuture().whenComplete((confirm, throwable) -> {
            if (throwable != null) {
                scheduleRetryOrFail(outboxId, "confirm callback failed: " + throwable.getMessage());
                return;
            }
            if (confirm.isAck()) {
                metrics.publishAcked();
                outboxStore.markSent(outboxId);
                log.info("RabbitMQ publisher confirm ack: outboxId={}", outboxId);
                return;
            }
            scheduleRetryOrFail(outboxId, "broker nack: " + confirm.getReason());
        });
    }

    private void scheduleRetryOrFail(String outboxId, String reason) {
        int attemptCount = outboxStore.attemptCount(outboxId);
        if (attemptCount >= MAX_PUBLISH_ATTEMPTS) {
            outboxStore.markFailed(outboxId, reason + ", max attempts reached");
            log.warn("RabbitMQ publish failed permanently: outboxId={}, attempts={}, reason={}",
                    outboxId, attemptCount, reason);
            return;
        }
        Instant nextRetryAt = Instant.now().plus(BASE_RETRY_BACKOFF.multipliedBy(Math.max(1, attemptCount)));
        outboxStore.markRetrying(outboxId, reason, nextRetryAt);
        log.warn("RabbitMQ publish scheduled for retry: outboxId={}, attempts={}, nextRetryAt={}, reason={}",
                outboxId, attemptCount, nextRetryAt, reason);
    }
}
