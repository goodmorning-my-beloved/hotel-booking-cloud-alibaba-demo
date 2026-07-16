package com.example.hotel.common.dto;

import java.math.BigDecimal;

public record RoomDto(Long id, String hotelName, String roomType, BigDecimal price, int available) {
}
