package com.example.hotel.order.application.port.out;

import com.example.hotel.order.domain.event.BookingCreated;

public interface BookingEventPublisher {

    void publish(BookingCreated event);
}
