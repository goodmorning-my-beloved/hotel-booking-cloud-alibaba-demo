package com.example.hotel.message;

import java.time.Instant;

/**
 * 一条消费者处理记录，用于 /rabbitmq/demo/status 展示。
 *
 * @param messageId 消息幂等键。
 * @param orderId 业务订单号。
 * @param status consumed、duplicate、dead-lettered 等处理状态。
 * @param detail 对应状态的教学说明。
 * @param receivedAt 消费者记录该事件的时间。
 */
public record RabbitMqConsumedEvent(
        String messageId,
        String orderId,
        String status,
        String detail,
        Instant receivedAt
) {
}
