package com.example.hotel.common.dto;

import java.time.LocalDate;

public record BookingRequest(Long userId, Long roomId, LocalDate checkIn, LocalDate checkOut) {
}
