package com.example.hotel.message.application.service;

import com.example.hotel.message.domain.model.BookingCreatedDetails;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class BookingEventApplicationServiceTest {

    private final BookingEventApplicationService service = new BookingEventApplicationService();

    @Test
    void recordsAndFiltersReceivedBookingEvents() {
        BookingCreatedDetails event = new BookingCreatedDetails(
                "ORD-1", 1L, 101L, new BigDecimal("688.00"), Instant.parse("2026-07-22T00:00:00Z"));

        service.record("rabbitmq", event);
        service.record("kafka", event);

        assertThat(service.recentEvents()).hasSize(2);
        assertThat(service.recentEvents("rabbitmq"))
                .singleElement()
                .satisfies(received -> {
                    assertThat(received.source()).isEqualTo("rabbitmq");
                    assertThat(received.event().orderId()).isEqualTo("ORD-1");
                });
    }
}
