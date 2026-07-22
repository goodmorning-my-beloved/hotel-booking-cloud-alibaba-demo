package com.example.hotel.order.interfaces.rest.dto;

import java.time.LocalDate;

/**
 * 订单上下文发布的 HTTP 请求协议，只属于 REST 入站适配器。
 */
public record BookRoomRequest(Long userId, Long roomId, LocalDate checkIn, LocalDate checkOut) {
}
