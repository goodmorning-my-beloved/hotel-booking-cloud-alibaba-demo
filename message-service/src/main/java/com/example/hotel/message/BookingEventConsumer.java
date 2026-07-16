package com.example.hotel.message;

import com.example.hotel.common.dto.BookingCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

@Component
public class BookingEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(BookingEventConsumer.class);

    private final List<ReceivedBookingEvent> receivedEvents = Collections.synchronizedList(new ArrayList<>());

    @Bean
    public Consumer<BookingCreatedEvent> bookingCreatedRabbit() {
        return event -> record("rabbitmq", event);
    }

    @Bean
    public Consumer<BookingCreatedEvent> bookingCreatedKafka() {
        return event -> record("kafka", event);
    }

    public List<ReceivedBookingEvent> receivedEvents() {
        synchronized (receivedEvents) {
            return List.copyOf(receivedEvents);
        }
    }

    public List<ReceivedBookingEvent> receivedEvents(String source) {
        return receivedEvents().stream()
                .filter(event -> event.source().equals(source))
                .toList();
    }

    private void record(String source, BookingCreatedEvent event) {
        ReceivedBookingEvent receivedEvent = new ReceivedBookingEvent(source, event, Instant.now());
        receivedEvents.add(receivedEvent);
        log.info("Received {} booking event: {}", source, event);
    }
}
