package com.example.hotel.message.application.service;

import com.example.hotel.message.domain.model.KafkaDemoConsumedEvent;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Kafka demo 的内存观测状态。生产和消费适配器写入，查询用例读取快照。
 */
@Component
public class KafkaDemoMetrics {

    private static final int MAX_RECENT_EVENTS = 80;

    private final AtomicLong published = new AtomicLong();
    private final AtomicLong publishAcked = new AtomicLong();
    private final AtomicLong consumed = new AtomicLong();
    private final AtomicLong auditConsumed = new AtomicLong();
    private final AtomicLong duplicated = new AtomicLong();
    private final AtomicLong failedForRetry = new AtomicLong();
    private final AtomicLong deadLettered = new AtomicLong();
    private final AtomicLong poisonPublished = new AtomicLong();
    private final AtomicLong transactionsCommitted = new AtomicLong();
    private final AtomicLong transactionsAborted = new AtomicLong();
    private final List<KafkaDemoConsumedEvent> recentEvents = new ArrayList<>();

    public void published() {
        published.incrementAndGet();
    }

    public void publishAcked() {
        publishAcked.incrementAndGet();
    }

    public void consumed() {
        consumed.incrementAndGet();
    }

    public void auditConsumed() {
        auditConsumed.incrementAndGet();
    }

    public void duplicated() {
        duplicated.incrementAndGet();
    }

    public void failedForRetry() {
        failedForRetry.incrementAndGet();
    }

    public void deadLettered() {
        deadLettered.incrementAndGet();
    }

    public void poisonPublished() {
        poisonPublished.incrementAndGet();
    }

    public void transactionCommitted() {
        transactionsCommitted.incrementAndGet();
    }

    public void transactionAborted() {
        transactionsAborted.incrementAndGet();
    }

    public void record(String messageId,
                       String orderId,
                       String businessKey,
                       String consumerGroup,
                       String consumerName,
                       String topic,
                       int partition,
                       long offset,
                       String status,
                       String detail) {
        KafkaDemoConsumedEvent event = new KafkaDemoConsumedEvent(
                messageId,
                orderId,
                businessKey,
                consumerGroup,
                consumerName,
                topic,
                partition,
                offset,
                status,
                detail,
                Instant.now());
        synchronized (recentEvents) {
            recentEvents.add(event);
            if (recentEvents.size() > MAX_RECENT_EVENTS) {
                recentEvents.remove(0);
            }
        }
    }

    public Snapshot snapshot() {
        List<KafkaDemoConsumedEvent> events;
        synchronized (recentEvents) {
            events = recentEvents.stream()
                    .sorted(Comparator.comparing(KafkaDemoConsumedEvent::receivedAt).reversed())
                    .toList();
        }
        return new Snapshot(
                published.get(),
                publishAcked.get(),
                consumed.get(),
                auditConsumed.get(),
                duplicated.get(),
                failedForRetry.get(),
                deadLettered.get(),
                poisonPublished.get(),
                transactionsCommitted.get(),
                transactionsAborted.get(),
                events);
    }

    public record Snapshot(
            long published,
            long publishAcked,
            long consumed,
            long auditConsumed,
            long duplicated,
            long failedForRetry,
            long deadLettered,
            long poisonPublished,
            long transactionsCommitted,
            long transactionsAborted,
            List<KafkaDemoConsumedEvent> recentEvents
    ) {
    }
}
