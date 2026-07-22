package com.example.hotel.order.infrastructure.client.dto;

/**
 * 订单上下文眼中的支付服务 HTTP 响应。
 */
public record PaymentApiResponse(String paymentId, String status) {
}
