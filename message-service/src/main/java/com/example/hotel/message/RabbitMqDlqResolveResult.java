package com.example.hotel.message;

import java.time.Instant;

/**
 * DLQ 人工补偿演示接口的返回值。
 *
 * <p>生产里不要把“删除死信消息”理解成直接删队列数据。更标准的做法是：
 * 修复原因或完成补偿后，用一个受控的 DLQ 处理器取出消息，补偿成功才 basicAck。
 * RabbitMQ 收到 ACK 后，才会把这条 DLQ 消息从队列里移除。</p>
 *
 * @param found DLQ 当前是否取到了消息；false 表示队列里没有 ready 消息可处理。
 * @param queue 本次处理的死信队列名称。
 * @param messageId 消息幂等键，用来保证补偿动作可以重复执行但业务副作用只发生一次。
 * @param orderId 演示订单号，方便和 RabbitMQ 控制台里的消息内容对应。
 * @param deadLetterReason RabbitMQ 记录的首次死信原因，例如 rejected、expired、maxlen。
 * @param action 本次对 DLQ 消息执行的动作；补偿成功后是 basicAck-delete。
 * @param compensationNote 人工排障或补偿动作说明，演示时由接口参数传入。
 * @param detail 教学说明，解释为什么 ACK 后消息会从 DLQ 消失。
 * @param resolvedAt 本次处理时间。
 */
public record RabbitMqDlqResolveResult(
        boolean found,
        String queue,
        String messageId,
        String orderId,
        String deadLetterReason,
        String action,
        String compensationNote,
        String detail,
        Instant resolvedAt
) {
}
