package com.example.hotel.order.domain.model;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StayPeriodTest {

    @Test
    void createsValidStayAsValueObject() {
        StayPeriod stay = new StayPeriod(
                LocalDate.of(2026, 8, 1),
                LocalDate.of(2026, 8, 4));

        assertThat(stay.nights()).isEqualTo(3);
    }

    @Test
    void rejectsInvalidLengthWhenObjectIsCreated() {
        assertThatThrownBy(() -> new StayPeriod(
                LocalDate.of(2026, 8, 4),
                LocalDate.of(2026, 8, 4)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("between 1 and 30");
    }
}
