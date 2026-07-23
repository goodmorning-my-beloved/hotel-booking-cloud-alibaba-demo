package com.example.hotel.hotel.interfaces.rest.dto;

import java.math.BigDecimal;

/**
 * 酒店库存上下文发布的房型 HTTP 响应协议。
 */
public record RoomResponse(
        Long id,
        String hotelName,
        String roomType,
        BigDecimal price,
        int available
) {
}
