package com.example.hotel.order.infrastructure.client.dto;

import java.time.LocalDate;

/**
 * 订单上下文调用酒店库存接口时使用的 HTTP 请求协议。
 */
public record ReserveRoomApiRequest(String orderId, LocalDate checkIn, LocalDate checkOut) {
}
