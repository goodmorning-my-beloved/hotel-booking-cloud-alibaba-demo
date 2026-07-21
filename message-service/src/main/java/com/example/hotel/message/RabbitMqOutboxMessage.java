package com.example.hotel.message;

import java.time.Instant;

/**
 * 本地消息表里一条消息的展示模型。
 *
 * <p>面试里常说的“本地消息表 / Outbox”就是这类记录：业务先把要发的消息落到本地库，
 * 再异步发送到 MQ，最后根据 publisher confirm 或重试任务更新发送状态。</p>
 *
 * @param outboxId 本地消息表主键，也是 publisher confirm 的 CorrelationData id。
 * @param messageId 业务幂等键，消费者用它去重；同一个 messageId 可以有多条投递记录。
 * @param orderId 演示订单号。
 * @param exchangeName 目标 exchange。
 * @param routingKey 目标 routing key。
 * @param status 当前投递状态：PENDING、WAIT_CONFIRM、SENT、RETRYING、RETURNED、FAILED。
 * @param attemptCount 已经尝试发送的次数，包含第一次发送。
 * @param maxAttempts 最大尝试次数。
 * @param nextRetryAt 下一次允许重试的时间；不可重试状态为空。
 * @param lastError 最近一次失败、nack 或 mandatory return 的原因。
 * @param createdAt 本地消息表记录创建时间。
 * @param updatedAt 最近一次状态更新时间。
 */
public record RabbitMqOutboxMessage(
        String outboxId,
        String messageId,
        String orderId,
        String exchangeName,
        String routingKey,
        String status,
        int attemptCount,
        int maxAttempts,
        Instant nextRetryAt,
        String lastError,
        Instant createdAt,
        Instant updatedAt
) {
}
