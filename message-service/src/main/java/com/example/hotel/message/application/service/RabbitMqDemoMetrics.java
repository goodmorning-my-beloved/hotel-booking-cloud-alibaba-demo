package com.example.hotel.message.application.service;

import com.example.hotel.message.domain.model.RabbitMqConsumedEvent;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Demo 的可观察状态。消息适配器写入，查询用例只读取快照。
 */
@Component
public class RabbitMqDemoMetrics {

    private static final int MAX_RECENT_EVENTS = 50;

    private final AtomicLong published = new AtomicLong();
    private final AtomicLong publishAcked = new AtomicLong();
    private final AtomicLong returned = new AtomicLong();
    private final AtomicLong consumed = new AtomicLong();
    private final AtomicLong duplicated = new AtomicLong();
    private final AtomicLong rejectedToDeadLetter = new AtomicLong();
    private final AtomicLong deadLetterResolved = new AtomicLong();
    private final AtomicLong topicWildcardConsumed = new AtomicLong();
    private final AtomicLong fanoutBroadcastConsumed = new AtomicLong();
    private final Set<String> processedMessageIds = ConcurrentHashMap.newKeySet();
    private final List<RabbitMqConsumedEvent> recentEvents = new ArrayList<>();

    public void published() {
        published.incrementAndGet();
    }

    public void publishAcked() {
        publishAcked.incrementAndGet();
    }

    public void returned() {
        returned.incrementAndGet();
    }

    public void consumed() {
        consumed.incrementAndGet();
    }

    public void duplicated() {
        duplicated.incrementAndGet();
    }

    public void rejectedToDeadLetter() {
        rejectedToDeadLetter.incrementAndGet();
    }

    public void deadLetterResolved() {
        deadLetterResolved.incrementAndGet();
    }

    public void topicWildcardConsumed() {
        topicWildcardConsumed.incrementAndGet();
    }

    public void fanoutBroadcastConsumed() {
        fanoutBroadcastConsumed.incrementAndGet();
    }

    public boolean markProcessed(String messageId) {
        return processedMessageIds.add(messageId);
    }

    public void record(String messageId, String orderId, String status, String detail) {
        RabbitMqConsumedEvent event = new RabbitMqConsumedEvent(messageId, orderId, status, detail, Instant.now());
        synchronized (recentEvents) {
            recentEvents.add(event);
            if (recentEvents.size() > MAX_RECENT_EVENTS) {
                recentEvents.remove(0);
            }
        }
    }

    public Snapshot snapshot() {
        List<RabbitMqConsumedEvent> events;
        synchronized (recentEvents) {
            events = recentEvents.stream()
                    .sorted(Comparator.comparing(RabbitMqConsumedEvent::receivedAt).reversed())
                    .toList();
        }
        return new Snapshot(
                published.get(),
                publishAcked.get(),
                returned.get(),
                consumed.get(),
                duplicated.get(),
                rejectedToDeadLetter.get(),
                deadLetterResolved.get(),
                topicWildcardConsumed.get(),
                fanoutBroadcastConsumed.get(),
                processedMessageIds.stream().sorted().toList(),
                events);
    }

    public record Snapshot(
            long published,
            long publishAcked,
            long returned,
            long consumed,
            long duplicated,
            long rejectedToDeadLetter,
            long deadLetterResolved,
            long topicWildcardConsumed,
            long fanoutBroadcastConsumed,
            List<String> processedMessageIds,
            List<RabbitMqConsumedEvent> recentEvents
    ) {
    }
}
