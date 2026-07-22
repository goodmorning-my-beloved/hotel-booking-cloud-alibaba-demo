package com.example.hotel.message.application.port.out;

import com.example.hotel.message.domain.model.RabbitMqBookingMessage;
import com.example.hotel.message.domain.model.RabbitMqOutboxMessage;
import com.example.hotel.message.domain.model.RabbitMqOutboxPublishTask;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public interface RabbitMqOutboxStore {

    RabbitMqOutboxPublishTask insertNew(String outboxId,
                                        RabbitMqBookingMessage payload,
                                        String exchangeName,
                                        String routingKey,
                                        String demoNote,
                                        int maxAttempts);

    void markWaitConfirm(String outboxId);

    void markSent(String outboxId);

    void markReturned(String outboxId, String reason);

    void markRetrying(String outboxId, String reason, Instant nextRetryAt);

    void markFailed(String outboxId, String reason);

    int attemptCount(String outboxId);

    List<RabbitMqOutboxPublishTask> findRetryable(Instant now, int limit);

    List<RabbitMqOutboxPublishTask> findConfirmTimedOut(Instant threshold, int limit);

    RabbitMqOutboxMessage findById(String outboxId);

    List<RabbitMqOutboxMessage> recent(int limit);

    Map<String, Long> countByStatus();
}
