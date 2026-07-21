package com.example.hotel.message;

/**
 * 发布接口返回给调用方的结果。
 *
 * <p>这些字段不是 RabbitMQ 必需字段，而是为了让初学者在 curl 返回值里看到本次消息去了哪里。</p>
 *
 * @param messageId 本次消息的幂等键。
 * @param orderId 本次消息模拟的订单号。
 * @param exchange 消息发送到哪个 exchange。
 * @param routingKey 本次发送使用的 routing key。
 * @param persistentMessage 是否把消息设置成持久化投递模式。
 * @param confirm publisher confirm 的结果，表示 Broker 是否确认收到消息。
 * @param note 本次接口要演示的 MQ 知识点。
 */
public record RabbitMqDemoPublishResult(
        String messageId,
        String orderId,
        String exchange,
        String routingKey,
        boolean persistentMessage,
        String confirm,
        String note
) {
}
