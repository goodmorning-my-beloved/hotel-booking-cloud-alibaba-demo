package com.example.hotel.message;

import java.time.Instant;

/**
 * 死信事故/补偿任务的展示模型。
 *
 * <p>生产里不建议让人工长期盯着 RabbitMQ DLQ 手工拿消息。更常见的做法是：
 * DLQ Listener 先把死信消息转存到业务库的“事故表/补偿任务表”，状态初始为 PENDING；
 * 运维或业务同学排障后，再按 incidentId 触发补偿。</p>
 *
 * @param incidentId 补偿任务 ID，人工处理接口按它精确处理指定任务。
 * @param sourceKey 死信来源去重键，避免 DLQ Listener 落库成功但 ACK 前宕机导致重复建任务。
 * @param queueName 死信来自哪个 DLQ。
 * @param messageId 原业务消息的幂等键。
 * @param orderId 演示订单号。
 * @param deadLetterReason RabbitMQ header 中记录的死信原因，例如 rejected。
 * @param status 当前任务状态：PENDING、RESOLVING、RESOLVED、FAILED。
 * @param compensationNote 人工排障或补偿说明。
 * @param lastError 最近一次补偿失败原因。
 * @param createdAt DLQ Listener 创建任务的时间。
 * @param updatedAt 最近一次任务状态更新时间。
 * @param resolvedAt 补偿成功时间。
 */
public record RabbitMqDlqIncident(
        String incidentId,
        String sourceKey,
        String queueName,
        String messageId,
        String orderId,
        String deadLetterReason,
        String status,
        String compensationNote,
        String lastError,
        Instant createdAt,
        Instant updatedAt,
        Instant resolvedAt
) {
}
