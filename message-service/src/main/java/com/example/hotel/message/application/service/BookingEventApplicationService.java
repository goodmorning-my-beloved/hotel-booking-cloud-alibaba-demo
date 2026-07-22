package com.example.hotel.message.application.service;

import com.example.hotel.common.dto.BookingCreatedEvent;
import com.example.hotel.message.domain.model.ReceivedBookingEvent;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * 订单业务事件查询用例。
 *
 * <p>demo 为了轻量运行，只保留最近收到的事件在内存中；生产环境通常会把消费结果写入业务库或可观测系统。</p>
 */
@Service
public class BookingEventApplicationService {

    private static final int MAX_RECENT_EVENTS = 50;

    private final List<ReceivedBookingEvent> receivedEvents = new ArrayList<>();

    public void record(String source, BookingCreatedEvent event) {
        synchronized (receivedEvents) {
            receivedEvents.add(new ReceivedBookingEvent(source, event, Instant.now()));
            if (receivedEvents.size() > MAX_RECENT_EVENTS) {
                receivedEvents.remove(0);
            }
        }
    }

    public List<ReceivedBookingEvent> recentEvents() {
        synchronized (receivedEvents) {
            return receivedEvents.stream()
                    .sorted(Comparator.comparing(ReceivedBookingEvent::receivedAt).reversed())
                    .toList();
        }
    }

    public List<ReceivedBookingEvent> recentEvents(String source) {
        return recentEvents().stream()
                .filter(event -> event.source().equalsIgnoreCase(source))
                .toList();
    }
}
