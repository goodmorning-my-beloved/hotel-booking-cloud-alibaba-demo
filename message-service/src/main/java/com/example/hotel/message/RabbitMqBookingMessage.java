package com.example.hotel.message;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * 发送到 RabbitMQ 的订单创建演示消息。
 *
 * <p>record 会被 Jackson2JsonMessageConverter 转成 JSON，消费者收到后再还原成同一个类型。</p>
 *
 * @param messageId 幂等键；重复投递时保持不变，消费者据此判断是否已经处理过。
 * @param orderId 业务订单号；这里用于观察不同消息是否真的触发了业务处理。
 * @param userId 用户 ID，模拟真实订单事件里的业务字段。
 * @param roomId 房间 ID，模拟真实订单事件里的业务字段。
 * @param amount 订单金额，模拟真实订单事件里的业务字段。
 * @param fail true 表示让消费者模拟失败并把消息打入死信队列。
 * @param createdAt 消息创建时间，方便观察事件产生时间。
 */
public record RabbitMqBookingMessage(
        String messageId,
        String orderId,
        Long userId,
        Long roomId,
        BigDecimal amount,
        boolean fail,
        Instant createdAt
) {
}
