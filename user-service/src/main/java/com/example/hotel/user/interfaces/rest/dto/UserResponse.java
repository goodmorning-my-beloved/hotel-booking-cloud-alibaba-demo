package com.example.hotel.user.interfaces.rest.dto;

/**
 * 用户上下文发布的 HTTP 响应协议。
 */
public record UserResponse(Long id, String name, String level) {
}
