package com.example.hotel.common.dto;

import java.math.BigDecimal;

public record BookingResponse(String orderId, Long userId, Long roomId, BigDecimal amount, String status) {
}
