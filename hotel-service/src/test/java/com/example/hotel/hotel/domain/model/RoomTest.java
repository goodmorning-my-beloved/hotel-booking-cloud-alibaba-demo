package com.example.hotel.hotel.domain.model;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RoomTest {

    private final StayPeriod stay = new StayPeriod(
            LocalDate.of(2026, 8, 1), LocalDate.of(2026, 8, 3));

    @Test
    void protectsCapacityAndUsesOrderIdForIdempotency() {
        Room room = new Room(101L, "Test Hotel", "King", new BigDecimal("688.00"), 1);

        room.reserve("ORD-1", stay);
        room.reserve("ORD-1", stay);

        assertThat(room.available()).isZero();
        assertThatThrownBy(() -> room.reserve("ORD-2", stay))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void releasingUnknownOrderCannotIncreaseCapacity() {
        Room room = new Room(101L, "Test Hotel", "King", new BigDecimal("688.00"), 1);

        room.release("ORD-unknown");

        assertThat(room.available()).isEqualTo(1);
    }
}
