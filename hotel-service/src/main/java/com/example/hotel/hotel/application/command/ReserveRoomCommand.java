package com.example.hotel.hotel.application.command;

import java.time.LocalDate;

public record ReserveRoomCommand(Long roomId, String orderId, LocalDate checkIn, LocalDate checkOut) {
}
