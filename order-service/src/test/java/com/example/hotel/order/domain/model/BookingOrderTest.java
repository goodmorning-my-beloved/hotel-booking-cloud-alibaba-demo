package com.example.hotel.order.domain.model;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BookingOrderTest {

    @Test
    void confirmsPaymentThroughAggregateBehavior() {
        BookingOrder order = pendingOrder();

        order.confirmPayment("PAY-1");

        assertThat(order.status()).isEqualTo(BookingStatus.PAID);
        assertThat(order.paymentId()).isEqualTo("PAY-1");
    }

    @Test
    void doesNotAllowPaymentToBeConfirmedTwice() {
        BookingOrder order = pendingOrder();
        order.confirmPayment("PAY-1");

        assertThatThrownBy(() -> order.confirmPayment("PAY-2"))
                .isInstanceOf(IllegalStateException.class);
    }

    private BookingOrder pendingOrder() {
        return BookingOrder.createPending(
                "ORD-1",
                1L,
                101L,
                new StayPeriod(LocalDate.of(2026, 8, 1), LocalDate.of(2026, 8, 3)),
                new BigDecimal("1376.00"),
                Instant.parse("2026-07-22T00:00:00Z"));
    }
}
