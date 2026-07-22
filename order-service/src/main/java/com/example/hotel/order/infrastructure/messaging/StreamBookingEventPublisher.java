package com.example.hotel.order.infrastructure.messaging;

import com.example.hotel.common.dto.BookingCreatedEvent;
import com.example.hotel.order.application.port.out.BookingEventPublisher;
import com.example.hotel.order.domain.event.BookingCreated;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.stereotype.Component;

@Component
public class StreamBookingEventPublisher implements BookingEventPublisher {

    private final StreamBridge streamBridge;

    public StreamBookingEventPublisher(StreamBridge streamBridge) {
        this.streamBridge = streamBridge;
    }

    @Override
    public void publish(BookingCreated event) {
        BookingCreatedEvent integrationEvent = new BookingCreatedEvent(
                event.orderId(), event.userId(), event.roomId(), event.amount(), event.occurredAt());
        boolean rabbitSent = streamBridge.send("bookingCreatedRabbit-out-0", integrationEvent);
        boolean kafkaSent = streamBridge.send("bookingCreatedKafka-out-0", integrationEvent);
        if (!rabbitSent || !kafkaSent) {
            throw new IllegalStateException("Booking event could not be sent to every configured binder.");
        }
    }
}
