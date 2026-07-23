package com.example.hotel.payment.interfaces.rest.dto;

import java.math.BigDecimal;

/**
 * 支付上下文发布的支付 HTTP 请求协议。
 */
public record PayOrderRequest(String orderId, Long userId, BigDecimal amount) {
}
