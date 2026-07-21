package com.example.hotel.message.domain.model;

/**
 * 重试任务需要的完整发送信息。
 *
 * <p>这个 record 不直接暴露给接口，因为 payload 可能较大。它只在生产者发送和定时重试时使用。</p>
 *
 * @param outboxId 本地消息表主键，同时作为 CorrelationData id。
 * @param exchangeName 目标 exchange。
 * @param routingKey 目标 routing key。
 * @param payload 重新投递时要恢复出来的业务消息体。
 * @param demoNote 写入 AMQP header 的教学说明。
 */
public record RabbitMqOutboxPublishTask(
        String outboxId,
        String exchangeName,
        String routingKey,
        RabbitMqBookingMessage payload,
        String demoNote
) {
}
