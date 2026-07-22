package com.example.hotel.message.application.result;

/**
 * 发布接口返回给调用方的结果。
 *
 * <p>这些字段不是 RabbitMQ 必需字段，而是为了让初学者在 curl 返回值里看到本次消息去了哪里。</p>
 *
 * @param outboxId 本地消息表主键；也是异步 publisher confirm 用来回写状态的关联 ID。
 * @param messageId 本次消息的幂等键。
 * @param orderId 本次消息模拟的订单号。
 * @param exchange 消息发送到哪个 exchange。
 * @param routingKey 本次发送使用的 routing key。
 * @param persistentMessage 是否把消息设置成持久化投递模式。
 * @param localMessageStatus 本地消息表当前状态；刚返回时通常是 WAIT_CONFIRM。
 * @param confirm publisher confirm 是异步回调，最终结果请看 /rabbitmq/demo/status 的本地消息表状态。
 * @param attemptCount 当前已经尝试投递的次数。
 * @param note 本次接口要演示的 MQ 知识点。
 */
public record RabbitMqDemoPublishResult(
        String outboxId,
        String messageId,
        String orderId,
        String exchange,
        String routingKey,
        boolean persistentMessage,
        String localMessageStatus,
        String confirm,
        int attemptCount,
        String note
) {
}
