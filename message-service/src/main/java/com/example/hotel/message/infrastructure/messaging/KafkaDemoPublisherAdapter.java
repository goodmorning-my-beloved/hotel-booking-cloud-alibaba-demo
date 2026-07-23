package com.example.hotel.message.infrastructure.messaging;

import com.example.hotel.message.application.port.out.KafkaDemoMessagePublisher;
import com.example.hotel.message.application.result.KafkaDemoPublishResult;
import com.example.hotel.message.application.result.KafkaDemoTransactionResult;
import com.example.hotel.message.application.service.KafkaDemoMetrics;
import com.example.hotel.message.domain.model.KafkaDemoMessage;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Component
public class KafkaDemoPublisherAdapter implements KafkaDemoMessagePublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final KafkaDemoMetrics metrics;

    public KafkaDemoPublisherAdapter(
            @Qualifier("kafkaDemoTemplate") KafkaTemplate<String, Object> kafkaTemplate,
            KafkaDemoMetrics metrics) {
        this.kafkaTemplate = kafkaTemplate;
        this.metrics = metrics;
    }

    @Override
    public KafkaDemoPublishResult publish(String topic, KafkaDemoMessage message, String note) {
        try {
            metrics.published();
            CompletableFuture<SendResult<String, Object>> future =
                    kafkaTemplate.send(topic, message.businessKey(), message);
            SendResult<String, Object> sendResult = future.get(10, TimeUnit.SECONDS);
            metrics.publishAcked();
            return toResult(message, sendResult.getRecordMetadata(), note);
        } catch (Exception exception) {
            throw new IllegalStateException("Kafka publish failed: " + exception.getMessage(), exception);
        }
    }

    @Override
    public List<KafkaDemoPublishResult> publishAsyncBatch(String topic,
                                                          List<KafkaDemoMessage> messages,
                                                          String note) {
        try {
            List<CompletableFuture<SendResult<String, Object>>> futures = new ArrayList<>();
            for (KafkaDemoMessage message : messages) {
                metrics.published();
                futures.add(kafkaTemplate.send(topic, message.businessKey(), message));
            }
            CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                    .get(10, TimeUnit.SECONDS);

            List<KafkaDemoPublishResult> results = new ArrayList<>();
            for (int i = 0; i < futures.size(); i++) {
                SendResult<String, Object> sendResult = futures.get(i).join();
                metrics.publishAcked();
                results.add(toResult(messages.get(i), sendResult.getRecordMetadata(),
                        note + "; batch-index=" + i));
            }
            return List.copyOf(results);
        } catch (Exception exception) {
            throw new IllegalStateException("Kafka async batch publish failed: " + exception.getMessage(), exception);
        }
    }

    @Override
    public KafkaDemoPublishResult publishRaw(String topic,
                                             String messageId,
                                             String businessKey,
                                             byte[] payload,
                                             String note) {
        try {
            metrics.published();
            metrics.poisonPublished();
            SendResult<String, Object> sendResult =
                    kafkaTemplate.send(topic, businessKey, payload).get(10, TimeUnit.SECONDS);
            metrics.publishAcked();
            RecordMetadata metadata = sendResult.getRecordMetadata();
            return new KafkaDemoPublishResult(
                    messageId,
                    null,
                    businessKey,
                    metadata.topic(),
                    metadata.partition(),
                    metadata.offset(),
                    true,
                    note,
                    Instant.now());
        } catch (Exception exception) {
            throw new IllegalStateException("Kafka raw publish failed: " + exception.getMessage(), exception);
        }
    }

    @Override
    public KafkaDemoTransactionResult publishTransaction(String topic,
                                                         List<KafkaDemoMessage> messages,
                                                         boolean failAfterFirst) {
        try {
            List<KafkaDemoPublishResult> committedResults = kafkaTemplate.executeInTransaction(operations -> {
                List<KafkaDemoPublishResult> pendingResults = new ArrayList<>();
                for (int i = 0; i < messages.size(); i++) {
                    KafkaDemoMessage message = messages.get(i);
                    metrics.published();
                    SendResult<String, Object> sendResult;
                    try {
                        sendResult = operations.send(topic, message.businessKey(), message)
                                .get(10, TimeUnit.SECONDS);
                    } catch (Exception exception) {
                        throw new IllegalStateException("Transactional send failed", exception);
                    }
                    pendingResults.add(toResult(
                            message,
                            sendResult.getRecordMetadata(),
                            "transactional send; index=" + i));
                    if (failAfterFirst && i == 0) {
                        throw new IllegalStateException("simulated failure after first transactional send");
                    }
                }
                return pendingResults;
            });

            committedResults.forEach(ignored -> metrics.publishAcked());
            metrics.transactionCommitted();
            return new KafkaDemoTransactionResult(
                    true,
                    messages.size(),
                    committedResults,
                    "transaction committed; read_committed consumers can now see every record",
                    Instant.now());
        } catch (Exception exception) {
            metrics.transactionAborted();
            return new KafkaDemoTransactionResult(
                    false,
                    messages.size(),
                    List.of(),
                    "transaction aborted; read_committed consumers see none of its records: "
                            + rootMessage(exception),
                    Instant.now());
        }
    }

    private KafkaDemoPublishResult toResult(KafkaDemoMessage message,
                                            RecordMetadata metadata,
                                            String note) {
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
    }

    private String rootMessage(Throwable throwable) {
        Throwable current = throwable;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        return current.getMessage() == null
                ? current.getClass().getSimpleName()
                : current.getMessage();
    }
}
