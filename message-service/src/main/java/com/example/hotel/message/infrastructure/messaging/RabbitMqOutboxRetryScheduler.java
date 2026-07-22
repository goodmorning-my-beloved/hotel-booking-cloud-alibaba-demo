package com.example.hotel.message.infrastructure.messaging;

import com.example.hotel.message.application.port.out.RabbitMqMessagePublisher;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class RabbitMqOutboxRetryScheduler {

    private final RabbitMqMessagePublisher messagePublisher;

    public RabbitMqOutboxRetryScheduler(RabbitMqMessagePublisher messagePublisher) {
        this.messagePublisher = messagePublisher;
    }

    @Scheduled(fixedDelay = 5_000)
    public void retryPublishFailures() {
        messagePublisher.retryFailures();
    }
}
