package com.example.hotel.message.infrastructure.messaging;

import com.example.hotel.message.application.port.out.KafkaDemoMessagePublisher;
import com.example.hotel.message.application.result.KafkaDemoPublishResult;
import com.example.hotel.message.application.service.KafkaDemoMetrics;
import com.example.hotel.message.domain.model.KafkaDemoMessage;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Component
public class KafkaDemoPublisherAdapter implements KafkaDemoMessagePublisher {

    private final KafkaTemplate<String, KafkaDemoMessage> kafkaTemplate;
    private final KafkaDemoMetrics metrics;

    public KafkaDemoPublisherAdapter(
            @Qualifier("kafkaDemoTemplate") KafkaTemplate<String, KafkaDemoMessage> kafkaTemplate,
            KafkaDemoMetrics metrics) {
        this.kafkaTemplate = kafkaTemplate;
        this.metrics = metrics;
    }

    @Override
    public KafkaDemoPublishResult publish(String topic, KafkaDemoMessage message, String note) {
        try {
            metrics.published();
            CompletableFuture<SendResult<String, KafkaDemoMessage>> future =
                    kafkaTemplate.send(topic, message.businessKey(), message);
            SendResult<String, KafkaDemoMessage> sendResult = future.get(10, TimeUnit.SECONDS);
            RecordMetadata metadata = sendResult.getRecordMetadata();
            metrics.publishAcked();
            return new KafkaDemoPublishResult(
                    message.messageId(),
                    message.orderId(),
                    message.businessKey(),
                    metadata.topic(),
                    metadata.partition(),
                    metadata.offset(),
                    true,
                    note,
                    Instant.now());
        } catch (Exception exception) {
            throw new IllegalStateException("Kafka publish failed: " + exception.getMessage(), exception);
        }
    }
}
