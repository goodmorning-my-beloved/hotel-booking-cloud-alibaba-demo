package com.example.hotel.order.application.command;

import java.time.LocalDate;

public record BookRoomCommand(Long userId, Long roomId, LocalDate checkIn, LocalDate checkOut) {
}
