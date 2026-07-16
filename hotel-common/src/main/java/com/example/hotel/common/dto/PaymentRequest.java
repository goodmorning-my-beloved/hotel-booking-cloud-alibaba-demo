package com.example.hotel.common.dto;

import java.math.BigDecimal;

public record PaymentRequest(String orderId, Long userId, BigDecimal amount) {
}
