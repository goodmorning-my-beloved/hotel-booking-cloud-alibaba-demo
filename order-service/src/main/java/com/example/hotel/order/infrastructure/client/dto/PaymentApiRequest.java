package com.example.hotel.order.infrastructure.client.dto;

import java.math.BigDecimal;

/**
 * 订单上下文调用支付接口时使用的 HTTP 请求协议。
 */
public record PaymentApiRequest(String orderId, Long userId, BigDecimal amount) {
}
