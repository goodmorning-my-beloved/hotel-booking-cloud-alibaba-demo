package com.example.hotel.order.interfaces.rest.dto;

import java.math.BigDecimal;

/**
 * 订单上下文对外发布的 HTTP 响应协议。
 */
public record BookingResponse(String orderId, Long userId, Long roomId, BigDecimal amount, String status) {
}
