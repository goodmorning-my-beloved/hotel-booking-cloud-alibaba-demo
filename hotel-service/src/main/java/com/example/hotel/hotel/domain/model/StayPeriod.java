package com.example.hotel.hotel.domain.model;

import java.time.LocalDate;

public record StayPeriod(LocalDate checkIn, LocalDate checkOut) {

    public StayPeriod {
        if (checkIn == null || checkOut == null || !checkOut.isAfter(checkIn)) {
            throw new IllegalArgumentException("A reservation requires checkOut after checkIn.");
        }
    }

    public boolean overlaps(StayPeriod other) {
        return checkIn.isBefore(other.checkOut) && other.checkIn.isBefore(checkOut);
    }
}
