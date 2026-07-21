package com.example.hotel.message.interfaces.rest.dto;

import com.example.hotel.message.domain.model.RabbitMqConsumedEvent;
import com.example.hotel.message.domain.model.RabbitMqDlqIncident;
import com.example.hotel.message.domain.model.RabbitMqOutboxMessage;

import java.util.List;
import java.util.Map;

/**
 * RabbitMQ demo 的内存状态快照。
 *
 * <p>它帮助你把接口调用结果、消费者日志和 RabbitMQ 控制台里的队列变化对应起来。</p>
 *
 * @param exchange 正常业务 exchange 名称。
 * @param queue 正常消费 queue 名称。
 * @param deadLetterExchange 死信 exchange 名称。
 * @param deadLetterQueue 死信 queue 名称。
 * @param topicExchange topic 通配符 exchange 名称。
 * @param topicBindings topic binding key 到队列名的映射，用于直观看出 * 和 # 的匹配差异。
 * @param fanoutExchange fanout 广播 exchange 名称。
 * @param fanoutQueues fanout exchange 当前绑定的演示队列；广播消息会复制到这些队列各一份。
 * @param published 生产者已经调用发送的消息数量。
 * @param publishAcked Broker 通过 publisher confirm 确认收到的数量。
 * @param returned mandatory return 捕获到的不可路由消息数量。
 * @param consumed 业务成功消费并 ack 的数量。
 * @param duplicated 被幂等逻辑识别为重复并直接 ack 的数量。
 * @param rejectedToDeadLetter 被 nack(requeue=false) 打入死信队列的数量。
 * @param deadLetterResolved 按 incidentId 补偿成功并标记 RESOLVED 的数量。
 * @param topicWildcardConsumed topic 通配符队列消费到的消息副本数量。
 * @param fanoutBroadcastConsumed fanout 广播队列消费到的消息副本数量。
 * @param outboxStatusCounts 本地消息表按状态聚合的数量。
 * @param recentOutboxMessages 最近的本地消息表记录，用于观察异步 confirm 和重试结果。
 * @param dlqIncidentStatusCounts DLQ 补偿任务按状态聚合的数量。
 * @param recentDlqIncidents 最近的 DLQ 补偿任务，用于找到要人工处理的 incidentId。
 * @param processedMessageIds 已经成功处理过的 messageId 集合。
 * @param recentEvents 最近的消费事件，便于观察每条消息的处理结果。
 */
public record RabbitMqDemoStatus(
        String exchange,
        String queue,
        String deadLetterExchange,
        String deadLetterQueue,
        String topicExchange,
        Map<String, String> topicBindings,
        String fanoutExchange,
        List<String> fanoutQueues,
        long published,
        long publishAcked,
        long returned,
        long consumed,
        long duplicated,
        long rejectedToDeadLetter,
        long deadLetterResolved,
        long topicWildcardConsumed,
        long fanoutBroadcastConsumed,
        Map<String, Long> outboxStatusCounts,
        List<RabbitMqOutboxMessage> recentOutboxMessages,
        Map<String, Long> dlqIncidentStatusCounts,
        List<RabbitMqDlqIncident> recentDlqIncidents,
        List<String> processedMessageIds,
        List<RabbitMqConsumedEvent> recentEvents
) {
}
