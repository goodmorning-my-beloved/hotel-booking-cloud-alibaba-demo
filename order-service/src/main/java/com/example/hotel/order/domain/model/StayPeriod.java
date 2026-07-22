package com.example.hotel.order.domain.model;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;

/**
 * 入住日期值对象。对象一旦创建，就保证入住天数是合法的领域状态。
 */
public record StayPeriod(LocalDate checkIn, LocalDate checkOut) {

    private static final long MAX_NIGHTS = 30;

    public StayPeriod {
        if (checkIn == null || checkOut == null) {
            throw new IllegalArgumentException("checkIn and checkOut are required.");
        }
        long nights = ChronoUnit.DAYS.between(checkIn, checkOut);
        if (nights <= 0 || nights > MAX_NIGHTS) {
            throw new IllegalArgumentException("Stay length must be between 1 and 30 nights.");
        }
    }

    public long nights() {
        return ChronoUnit.DAYS.between(checkIn, checkOut);
    }
}
