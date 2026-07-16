package com.example.hotel.message;

import com.example.hotel.common.api.ApiResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/messages")
public class MessageController {

    private final BookingEventConsumer consumer;

    public MessageController(BookingEventConsumer consumer) {
        this.consumer = consumer;
    }

    @GetMapping("/booking-events")
    public ApiResponse<List<ReceivedBookingEvent>> bookingEvents() {
        return ApiResponse.ok(consumer.receivedEvents());
    }

    @GetMapping("/booking-events/rabbitmq")
    public ApiResponse<List<ReceivedBookingEvent>> rabbitBookingEvents() {
        return ApiResponse.ok(consumer.receivedEvents("rabbitmq"));
    }

    @GetMapping("/booking-events/kafka")
    public ApiResponse<List<ReceivedBookingEvent>> kafkaBookingEvents() {
        return ApiResponse.ok(consumer.receivedEvents("kafka"));
    }
}
