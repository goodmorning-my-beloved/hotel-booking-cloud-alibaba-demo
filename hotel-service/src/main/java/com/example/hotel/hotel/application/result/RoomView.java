package com.example.hotel.hotel.application.result;

import java.math.BigDecimal;

public record RoomView(
        Long id,
        String hotelName,
        String roomType,
        BigDecimal pricePerNight,
        int available
) {
}
