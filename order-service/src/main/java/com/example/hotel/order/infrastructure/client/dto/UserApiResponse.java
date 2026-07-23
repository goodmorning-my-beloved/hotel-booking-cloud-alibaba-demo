package com.example.hotel.order.infrastructure.client.dto;

/**
 * 订单上下文眼中的用户服务 HTTP 响应，仅供 UserClient 适配器使用。
 */
public record UserApiResponse(Long id, String name, String level) {
}
