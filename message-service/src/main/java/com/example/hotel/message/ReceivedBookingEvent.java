package com.example.hotel.message;

import com.example.hotel.common.dto.BookingCreatedEvent;

import java.time.Instant;

public record ReceivedBookingEvent(
        String source,
        BookingCreatedEvent event,
        Instant receivedAt
) {
}
