package com.example.hotel.payment.application.command;

import java.math.BigDecimal;

public record PayOrderCommand(String orderId, Long userId, BigDecimal amount) {
}
