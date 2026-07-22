package com.example.hotel.message.domain.model;

import com.example.hotel.common.dto.BookingCreatedEvent;

import java.time.Instant;

/**
 * message-service 收到的订单创建业务事件。
 *
 * <p>这是完整酒店下单链路的观测对象，不参与 RabbitMQ outbox demo 的可靠投递状态机。</p>
 *
 * @param source 消息来源，例如 rabbitmq 或 kafka。
 * @param event order-service 发布的订单创建事件。
 * @param receivedAt message-service 实际收到事件的时间。
 */
public record ReceivedBookingEvent(
        String source,
        BookingCreatedEvent event,
        Instant receivedAt
) {
}
