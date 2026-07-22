package com.example.hotel.payment.interfaces.rest.dto;

/**
 * 支付上下文发布的 HTTP 响应协议。
 */
public record PaymentResponse(String paymentId, String status) {
}
