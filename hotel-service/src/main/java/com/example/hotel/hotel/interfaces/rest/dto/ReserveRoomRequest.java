package com.example.hotel.hotel.interfaces.rest.dto;

import java.time.LocalDate;

/**
 * 酒店库存上下文发布的预留房间 HTTP 请求协议。
 */
public record ReserveRoomRequest(String orderId, LocalDate checkIn, LocalDate checkOut) {
}
