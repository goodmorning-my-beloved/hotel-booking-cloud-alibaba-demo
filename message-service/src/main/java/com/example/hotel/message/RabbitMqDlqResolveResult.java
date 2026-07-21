package com.example.hotel.message;

import java.time.Instant;

/**
 * DLQ 事故任务人工补偿接口的返回值。
 *
 * <p>当前 demo 的处理链路是：DLQ Listener 先把死信消息登记成 PENDING 任务并 ACK 掉队列消息；
 * 人工排障或补偿完成后，再调用 resolve 接口处理指定 incidentId。这样 RabbitMQ 不需要承担工单系统职责，
 * 人工侧也能按任务 ID 精确处理、审计和重试。</p>
 *
 * @param found 是否找到了指定 incidentId。
 * @param incidentId 本次处理的 DLQ 事故任务 ID。
 * @param queue 事故来源 DLQ。
 * @param messageId 原消息幂等键，用来保证补偿动作可以重复执行但业务副作用只发生一次。
 * @param orderId 演示订单号，方便和 RabbitMQ 控制台里的消息内容对应。
 * @param deadLetterReason RabbitMQ 记录的首次死信原因，例如 rejected、expired、maxlen。
 * @param previousStatus 接口处理前任务状态。
 * @param currentStatus 接口处理后任务状态。
 * @param action 本次对事故任务执行的动作。
 * @param compensationNote 人工排障或补偿动作说明，演示时由接口参数传入。
 * @param detail 教学说明，解释为什么这一步不再直接从 DLQ 删除消息。
 * @param resolvedAt 本次处理时间。
 */
public record RabbitMqDlqResolveResult(
        boolean found,
        String incidentId,
        String queue,
        String messageId,
        String orderId,
        String deadLetterReason,
        String previousStatus,
        String currentStatus,
        String action,
        String compensationNote,
        String detail,
        Instant resolvedAt
) {
}
