package com.example.hotel.common.dto;

import java.time.LocalDate;

public record ReserveRoomRequest(String orderId, LocalDate checkIn, LocalDate checkOut) {
}
