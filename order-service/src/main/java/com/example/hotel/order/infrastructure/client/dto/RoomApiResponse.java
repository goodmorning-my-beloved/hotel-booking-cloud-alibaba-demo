package com.example.hotel.order.infrastructure.client.dto;

import java.math.BigDecimal;

/**
 * 订单上下文眼中的酒店房型 HTTP 响应。
 */
public record RoomApiResponse(
        Long id,
        String hotelName,
        String roomType,
        BigDecimal price,
        int available
) {
}
