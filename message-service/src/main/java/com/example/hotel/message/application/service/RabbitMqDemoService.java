package com.example.hotel.message.application.service;

import com.example.hotel.message.domain.model.RabbitMqBookingMessage;
import com.example.hotel.message.domain.model.RabbitMqConsumedEvent;
import com.example.hotel.message.domain.model.RabbitMqDlqIncident;
import com.example.hotel.message.domain.model.RabbitMqOutboxMessage;
import com.example.hotel.message.domain.model.RabbitMqOutboxPublishTask;
import com.example.hotel.message.infrastructure.config.RabbitMqDemoConfig;
import com.example.hotel.message.infrastructure.persistence.RabbitMqDlqIncidentRepository;
import com.example.hotel.message.infrastructure.persistence.RabbitMqOutboxRepository;
import com.example.hotel.message.interfaces.rest.dto.RabbitMqDemoPublishResult;
import com.example.hotel.message.interfaces.rest.dto.RabbitMqDemoStatus;
import com.example.hotel.message.interfaces.rest.dto.RabbitMqDlqResolveResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class RabbitMqDemoService {

    private static final Logger log = LoggerFactory.getLogger(RabbitMqDemoService.class);
    // 只保留最近的消费事件，避免 /status 接口返回无限增长的数据。
    private static final int MAX_RECENT_EVENTS = 50;
    // 本地消息表最多保留给状态接口展示的最近记录数。
    private static final int MAX_RECENT_OUTBOX_MESSAGES = 20;
    // DLQ 补偿任务最多保留给状态接口展示的最近记录数。
    private static final int MAX_RECENT_DLQ_INCIDENTS = 20;
    // demo 里最多尝试 3 次投递。生产环境通常会把最大次数、退避时间配置化。
    private static final int MAX_PUBLISH_ATTEMPTS = 3;
    private static final int RETRY_BATCH_SIZE = 20;
    // confirm 长时间没回来时，重试任务会把它当作“结果未知”处理并重新投递。
    // 这可能造成重复消息，所以消费端幂等是可靠投递方案的必要组成部分。
    private static final Duration CONFIRM_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration BASE_RETRY_BACKOFF = Duration.ofSeconds(5);

    // RabbitTemplate 是 Spring AMQP 的生产者工具类，负责把 Java 对象发送到 RabbitMQ。
    private final RabbitTemplate rabbitTemplate;
    private final ObjectMapper objectMapper;
    // 本地消息表负责记录“要发什么、发到哪里、当前是否成功、失败后何时重试”。
    private final RabbitMqOutboxRepository outboxRepository;
    // 死信事故表负责把 DLQ 消息变成可查询、可审计、可按 ID 处理的补偿任务。
    private final RabbitMqDlqIncidentRepository dlqIncidentRepository;
    // 下面这些计数器只是 demo 状态展示，帮助你把接口调用和 RabbitMQ 控制台变化对应起来。
    private final AtomicLong published = new AtomicLong();
    private final AtomicLong publishAcked = new AtomicLong();
    private final AtomicLong returned = new AtomicLong();
    private final AtomicLong consumed = new AtomicLong();
    private final AtomicLong duplicated = new AtomicLong();
    private final AtomicLong rejectedToDeadLetter = new AtomicLong();
    private final AtomicLong deadLetterResolved = new AtomicLong();
    private final AtomicLong topicWildcardConsumed = new AtomicLong();
    private final AtomicLong fanoutBroadcastConsumed = new AtomicLong();
    // 幂等演示：用 messageId 记录已处理消息。生产环境通常换成数据库唯一索引或 Redis SETNX。
    private final Set<String> processedMessageIds = ConcurrentHashMap.newKeySet();
    // ArrayList 不是线程安全的，读写时在 record/status 里用 synchronized 保护。
    private final List<RabbitMqConsumedEvent> recentEvents = new ArrayList<>();

    public RabbitMqDemoService(RabbitTemplate rabbitTemplate,
                               ObjectMapper objectMapper,
                               RabbitMqOutboxRepository outboxRepository,
                               RabbitMqDlqIncidentRepository dlqIncidentRepository) {
        this.rabbitTemplate = rabbitTemplate;
        this.objectMapper = objectMapper;
        this.outboxRepository = outboxRepository;
        this.dlqIncidentRepository = dlqIncidentRepository;
        // mandatory=true 且消息到达 exchange 但没有匹配队列时，会触发 returns callback。
        // 这能演示“消息到了交换机，却没有进任何队列”的不可路由问题。
        this.rabbitTemplate.setReturnsCallback(returnedMessage -> {
            returned.incrementAndGet();
            Object outboxId = returnedMessage.getMessage().getMessageProperties().getHeaders().get("x-outbox-id");
            if (outboxId != null) {
                outboxRepository.markReturned(outboxId.toString(),
                        "mandatory return: " + returnedMessage.getReplyCode() + " " + returnedMessage.getReplyText());
            }
            log.warn("RabbitMQ returned message. exchange={}, routingKey={}, replyCode={}, replyText={}",
                    returnedMessage.getExchange(),
                    returnedMessage.getRoutingKey(),
                    returnedMessage.getReplyCode(),
                    returnedMessage.getReplyText());
        });
    }

    public RabbitMqDemoPublishResult publishNormal(String messageId) {
        // 正常消息：发送到正确 routing key，消费者处理成功后 basicAck。
        RabbitMqBookingMessage message = newMessage(messageId, false);
        return publish(message, RabbitMqDemoConfig.BOOKING_ROUTING_KEY,
                "normal persistent message, consumer will ack after successful processing");
    }

    public List<RabbitMqDemoPublishResult> publishDuplicate(String messageId) {
        // 重复消息：两条消息使用同一个 messageId，模拟 RabbitMQ “至少一次投递”带来的重复消费。
        String duplicateMessageId = normalizeMessageId(messageId);
        RabbitMqBookingMessage first = newMessage(duplicateMessageId, false);
        // 第二条消息故意换 orderId 但保持同一个 messageId，方便观察消费者按 messageId 去重。
        RabbitMqBookingMessage second = new RabbitMqBookingMessage(
                duplicateMessageId,
                "ORD-" + UUID.randomUUID().toString().substring(0, 8),
                1L,
                101L,
                new BigDecimal("399.00"),
                false,
                Instant.now());
        return List.of(
                publish(first, RabbitMqDemoConfig.BOOKING_ROUTING_KEY, "first message for idempotency demo"),
                publish(second, RabbitMqDemoConfig.BOOKING_ROUTING_KEY, "same messageId, consumer should ack and ignore")
        );
    }

    public RabbitMqDemoPublishResult publishDeadLetter(String messageId) {
        // fail=true 让消费者主动 basicNack(requeue=false)，消息会走正常队列配置的 DLX/DLQ。
        RabbitMqBookingMessage message = newMessage(messageId, true);
        return publish(message, RabbitMqDemoConfig.BOOKING_ROUTING_KEY,
                "consumer will nack without requeue, RabbitMQ routes it to the DLQ");
    }

    public RabbitMqDemoPublishResult publishUnroutable(String messageId) {
        // 错误 routing key：exchange 存在，但没有 binding 匹配，mandatory return 会记录 returned+1。
        RabbitMqBookingMessage message = newMessage(messageId, false);
        return publish(message, "hotel.booking.unroutable",
                "mandatory publish with no matching binding, return callback records the problem");
    }

    public RabbitMqDemoPublishResult publishTopic(String messageId, String routingKey) {
        // topic exchange 演示重点不是可靠投递链路本身，而是“一个 routing key 可以匹配多个通配绑定”。
        // 默认 hotel.booking.created 会同时匹配 hotel.booking.* 和 hotel.booking.# 两个队列。
        String normalizedRoutingKey = normalizeTopicRoutingKey(routingKey);
        RabbitMqBookingMessage message = newMessage(messageId, false);
        return publish(message,
                RabbitMqDemoConfig.BOOKING_TOPIC_EXCHANGE,
                normalizedRoutingKey,
                "topic wildcard demo: hotel.booking.* matches one word, hotel.booking.# matches multiple words");
    }

    public RabbitMqDemoPublishResult publishFanout(String messageId) {
        // fanout exchange 会忽略 routing key；这里保留空字符串，避免初学者误以为广播还靠 key 匹配。
        RabbitMqBookingMessage message = newMessage(messageId, false);
        return publish(message,
                RabbitMqDemoConfig.BOOKING_FANOUT_EXCHANGE,
                "",
                "fanout broadcast demo: same message is copied to every bound queue");
    }

    public RabbitMqDemoStatus status() {
        // /status 把 demo 内存计数返回给前端或 curl，便于和 RabbitMQ 控制台一起观察。
        List<String> ids = processedMessageIds.stream()
                .sorted()
                .toList();
        List<RabbitMqConsumedEvent> events;
        synchronized (recentEvents) {
            events = recentEvents.stream()
                    .sorted(Comparator.comparing(RabbitMqConsumedEvent::receivedAt).reversed())
                    .toList();
        }
        return new RabbitMqDemoStatus(
                RabbitMqDemoConfig.BOOKING_EXCHANGE,
                RabbitMqDemoConfig.BOOKING_QUEUE,
                RabbitMqDemoConfig.BOOKING_DLX,
                RabbitMqDemoConfig.BOOKING_DLQ,
                RabbitMqDemoConfig.BOOKING_TOPIC_EXCHANGE,
                Map.of(
                        RabbitMqDemoConfig.BOOKING_TOPIC_SINGLE_WORD_PATTERN,
                        RabbitMqDemoConfig.BOOKING_TOPIC_SINGLE_WORD_QUEUE,
                        RabbitMqDemoConfig.BOOKING_TOPIC_MULTI_WORD_PATTERN,
                        RabbitMqDemoConfig.BOOKING_TOPIC_MULTI_WORD_QUEUE),
                RabbitMqDemoConfig.BOOKING_FANOUT_EXCHANGE,
                List.of(
                        RabbitMqDemoConfig.BOOKING_FANOUT_SMS_QUEUE,
                        RabbitMqDemoConfig.BOOKING_FANOUT_POINTS_QUEUE),
                published.get(),
                publishAcked.get(),
                returned.get(),
                consumed.get(),
                duplicated.get(),
                rejectedToDeadLetter.get(),
                deadLetterResolved.get(),
                topicWildcardConsumed.get(),
                fanoutBroadcastConsumed.get(),
                outboxRepository.countByStatus(),
                outboxRepository.recent(MAX_RECENT_OUTBOX_MESSAGES),
                dlqIncidentRepository.countByStatus(),
                dlqIncidentRepository.recent(MAX_RECENT_DLQ_INCIDENTS),
                ids,
                events);
    }

    public List<RabbitMqDlqIncident> dlqIncidents(String status) {
        // 人工处理前通常先查 PENDING 任务，拿到 incidentId 后再调用 resolve。
        if (status == null || status.isBlank()) {
            return dlqIncidentRepository.recent(MAX_RECENT_DLQ_INCIDENTS);
        }
        return dlqIncidentRepository.findByStatus(status, MAX_RECENT_DLQ_INCIDENTS);
    }

    public RabbitMqDlqResolveResult resolveDlqIncident(String incidentId, String compensationNote) {
        // 现在不再从 RabbitMQ DLQ 里“取下一条”处理，而是按补偿任务 ID 精确处理。
        // 这更符合生产实践：DLQ Listener 已经把消息转存到本地表，人工操作面对的是可审计的业务任务。
        RabbitMqDlqIncident incident = dlqIncidentRepository.findById(incidentId);
        String note = normalizeCompensationNote(compensationNote);
        if (incident == null) {
            return new RabbitMqDlqResolveResult(
                    false,
                    incidentId,
                    RabbitMqDemoConfig.BOOKING_DLQ,
                    null,
                    null,
                    null,
                    null,
                    null,
                    "not-found",
                    note,
                    "no DLQ incident found by incidentId",
                    Instant.now());
        }

        if (RabbitMqDlqIncidentRepository.STATUS_RESOLVED.equals(incident.status())) {
            return new RabbitMqDlqResolveResult(
                    true,
                    incident.incidentId(),
                    incident.queueName(),
                    incident.messageId(),
                    incident.orderId(),
                    incident.deadLetterReason(),
                    incident.status(),
                    incident.status(),
                    "already-resolved",
                    incident.compensationNote(),
                    "incident was already resolved, do not run compensation again",
                    incident.resolvedAt());
        }

        if (!dlqIncidentRepository.markResolving(incidentId)) {
            RabbitMqDlqIncident latest = dlqIncidentRepository.findById(incidentId);
            return new RabbitMqDlqResolveResult(
                    true,
                    latest.incidentId(),
                    latest.queueName(),
                    latest.messageId(),
                    latest.orderId(),
                    latest.deadLetterReason(),
                    incident.status(),
                    latest.status(),
                    "skip",
                    latest.compensationNote(),
                    "incident is not PENDING or FAILED, another operator may be processing it",
                    Instant.now());
        }

        try {
            // 这里用 processedMessageIds 模拟“补偿动作已经落库”的幂等记录。
            // 真实生产系统应写补偿流水表、订单状态表或人工工单表，并用唯一键防止重复补偿。
            String idempotencyKey = incident.messageId() == null ? incident.incidentId() : incident.messageId();
            boolean firstCompensation = processedMessageIds.add(idempotencyKey);
            String compensationDetail = firstCompensation
                    ? "manual compensation task finished by incidentId"
                    : "business message was already compensated before, mark incident resolved without side effects";
            dlqIncidentRepository.markResolved(incidentId, note);
            deadLetterResolved.incrementAndGet();
            record(idempotencyKey, incident.orderId(), "dlq-incident-resolved",
                    compensationDetail + "; incidentId=" + incidentId + "; note=" + note);

            RabbitMqDlqIncident resolved = dlqIncidentRepository.findById(incidentId);
            return new RabbitMqDlqResolveResult(
                    true,
                    resolved.incidentId(),
                    resolved.queueName(),
                    resolved.messageId(),
                    resolved.orderId(),
                    resolved.deadLetterReason(),
                    incident.status(),
                    resolved.status(),
                    "mark-incident-resolved",
                    note,
                    "DLQ message was already ACKed after creating the PENDING incident; this call resolves the stored compensation task",
                    resolved.resolvedAt());
        } catch (RuntimeException ex) {
            dlqIncidentRepository.markFailed(incidentId, ex.getMessage());
            throw ex;
        }
    }

    @Scheduled(fixedDelay = 5_000)
    public void retryPublishFailures() {
        // 定时任务只处理生产端投递失败：例如 RabbitMQ 暂时不可用、Broker 返回 nack、confirm 超时。
        // 不可路由 RETURNED 通常说明 exchange/binding/routing key 配错，盲目重试同一条路由没有意义。
        Instant now = Instant.now();
        outboxRepository.findConfirmTimedOut(now.minus(CONFIRM_TIMEOUT), RETRY_BATCH_SIZE)
                .forEach(task -> scheduleRetryOrFail(task.outboxId(), "publisher confirm timeout"));
        outboxRepository.findRetryable(now, RETRY_BATCH_SIZE)
                .forEach(this::publishOutboxTask);
    }

    @RabbitListener(queues = RabbitMqDemoConfig.BOOKING_QUEUE, ackMode = "MANUAL")
    public void consume(RabbitMqBookingMessage payload, Message rawMessage, Channel channel) throws IOException {
        // deliveryTag 是 RabbitMQ 给当前 channel 内消息的编号，ack/nack 时必须带上它。
        long deliveryTag = rawMessage.getMessageProperties().getDeliveryTag();
        // messageId 优先从 AMQP 属性取；如果没有，再用消息体里的 messageId，保证幂等键稳定。
        String messageId = rawMessage.getMessageProperties().getMessageId();
        if (messageId == null || messageId.isBlank()) {
            messageId = payload.messageId();
        }

        if (payload.fail()) {
            rejectedToDeadLetter.incrementAndGet();
            record(messageId, payload.orderId(), "dead-lettered", "simulated failure, basicNack requeue=false");
            // multiple=false 表示只拒绝当前这条消息；requeue=false 表示不要放回原队列，而是进入 DLX/DLQ。
            channel.basicNack(deliveryTag, false, false);
            return;
        }

        if (!processedMessageIds.add(messageId)) {
            duplicated.incrementAndGet();
            record(messageId, payload.orderId(), "duplicate", "messageId already processed, ack without side effects");
            // 重复消息也要 ack，否则 RabbitMQ 会继续重投；区别是这里不再执行业务副作用。
            channel.basicAck(deliveryTag, false);
            return;
        }

        consumed.incrementAndGet();
        record(messageId, payload.orderId(), "consumed", "business action finished, manual ack sent");
        // 手动 ack 的核心原则：业务真正成功之后再 ack，避免“消息已确认但业务失败”导致丢消息。
        channel.basicAck(deliveryTag, false);
    }

    @RabbitListener(queues = RabbitMqDemoConfig.BOOKING_DLQ, ackMode = "MANUAL")
    public void captureDeadLetterIncident(Message rawMessage, Channel channel) throws IOException {
        // 这个监听器专门监听 DLQ。它不做真正业务补偿，只把死信登记为 PENDING 事故任务。
        // 好处是 RabbitMQ 不需要长期堆积失败现场，人工系统可以按 incidentId 查询、分派、审计和重试。
        long deliveryTag = rawMessage.getMessageProperties().getDeliveryTag();
        try {
            String payloadJson = new String(rawMessage.getBody(), StandardCharsets.UTF_8);
            RabbitMqBookingMessage payload = readBookingMessageOrNull(payloadJson);
            String messageId = rawMessage.getMessageProperties().getMessageId();
            if ((messageId == null || messageId.isBlank()) && payload != null) {
                messageId = payload.messageId();
            }
            String orderId = payload == null ? null : payload.orderId();
            String headersJson = headersJson(rawMessage.getMessageProperties().getHeaders());
            String sourceKey = dlqSourceKey(rawMessage, payloadJson, messageId);
            RabbitMqDlqIncident incident = dlqIncidentRepository.insertPendingIfAbsent(
                    "INC-" + UUID.randomUUID().toString().substring(0, 8),
                    sourceKey,
                    RabbitMqDemoConfig.BOOKING_DLQ,
                    messageId,
                    orderId,
                    firstDeathReason(rawMessage.getMessageProperties().getHeaders()),
                    payloadJson,
                    headersJson);

            // 只有死信已经成功转存到 mq_dlq_incident 后才 ACK。ACK 后 RabbitMQ 会删除 DLQ 原消息；
            // 后续人工处理不再依赖队列顺序，而是依赖本地表里的 PENDING 任务。
            channel.basicAck(deliveryTag, false);
            record(messageId == null ? incident.incidentId() : messageId,
                    orderId,
                    "dlq-incident-pending",
                    "DLQ listener stored PENDING incidentId=" + incident.incidentId());
        } catch (Exception ex) {
            // 事故任务都没落库时不能 ACK，否则 DLQ 消息会丢。这里 requeue=true 让它回到 DLQ 等待下次登记。
            channel.basicNack(deliveryTag, false, true);
            throw new IllegalStateException("Failed to store DLQ incident, message has been requeued", ex);
        }
    }

    @RabbitListener(queues = RabbitMqDemoConfig.BOOKING_TOPIC_SINGLE_WORD_QUEUE, ackMode = "MANUAL")
    public void consumeTopicSingleWord(RabbitMqBookingMessage payload, Message rawMessage, Channel channel) throws IOException {
        // 这个消费者只会收到匹配 hotel.booking.* 的消息。
        // 例如 hotel.booking.created 能进来，但 hotel.booking.payment.timeout 因为多了一个单词层级，不会进来。
        long deliveryTag = rawMessage.getMessageProperties().getDeliveryTag();
        String routingKey = rawMessage.getMessageProperties().getReceivedRoutingKey();
        topicWildcardConsumed.incrementAndGet();
        record(payload.messageId(), payload.orderId(), "topic-single-word-consumed",
                "queue=" + RabbitMqDemoConfig.BOOKING_TOPIC_SINGLE_WORD_QUEUE
                        + "; binding=" + RabbitMqDemoConfig.BOOKING_TOPIC_SINGLE_WORD_PATTERN
                        + "; routingKey=" + routingKey);
        channel.basicAck(deliveryTag, false);
    }

    @RabbitListener(queues = RabbitMqDemoConfig.BOOKING_TOPIC_MULTI_WORD_QUEUE, ackMode = "MANUAL")
    public void consumeTopicMultiWord(RabbitMqBookingMessage payload, Message rawMessage, Channel channel) throws IOException {
        // 这个消费者会收到匹配 hotel.booking.# 的消息。
        // # 能覆盖更宽的事件范围，常用于“订单域所有事件”这种订阅。
        long deliveryTag = rawMessage.getMessageProperties().getDeliveryTag();
        String routingKey = rawMessage.getMessageProperties().getReceivedRoutingKey();
        topicWildcardConsumed.incrementAndGet();
        record(payload.messageId(), payload.orderId(), "topic-multi-word-consumed",
                "queue=" + RabbitMqDemoConfig.BOOKING_TOPIC_MULTI_WORD_QUEUE
                        + "; binding=" + RabbitMqDemoConfig.BOOKING_TOPIC_MULTI_WORD_PATTERN
                        + "; routingKey=" + routingKey);
        channel.basicAck(deliveryTag, false);
    }

    @RabbitListener(queues = RabbitMqDemoConfig.BOOKING_FANOUT_SMS_QUEUE, ackMode = "MANUAL")
    public void consumeFanoutSms(RabbitMqBookingMessage payload, Message rawMessage, Channel channel) throws IOException {
        // fanout 场景下，每个下游队列都有一份独立消息。这里模拟“短信服务”收到订单创建广播。
        long deliveryTag = rawMessage.getMessageProperties().getDeliveryTag();
        fanoutBroadcastConsumed.incrementAndGet();
        record(payload.messageId(), payload.orderId(), "fanout-sms-consumed",
                "queue=" + RabbitMqDemoConfig.BOOKING_FANOUT_SMS_QUEUE + "; routing key ignored by fanout exchange");
        channel.basicAck(deliveryTag, false);
    }

    @RabbitListener(queues = RabbitMqDemoConfig.BOOKING_FANOUT_POINTS_QUEUE, ackMode = "MANUAL")
    public void consumeFanoutPoints(RabbitMqBookingMessage payload, Message rawMessage, Channel channel) throws IOException {
        // 同一条 fanout 消息还会到积分队列，说明广播不是负载均衡，而是复制投递给每个绑定队列。
        long deliveryTag = rawMessage.getMessageProperties().getDeliveryTag();
        fanoutBroadcastConsumed.incrementAndGet();
        record(payload.messageId(), payload.orderId(), "fanout-points-consumed",
                "queue=" + RabbitMqDemoConfig.BOOKING_FANOUT_POINTS_QUEUE + "; same broadcast message copy");
        channel.basicAck(deliveryTag, false);
    }

    private RabbitMqDemoPublishResult publish(RabbitMqBookingMessage message, String routingKey, String note) {
        return publish(message, RabbitMqDemoConfig.BOOKING_EXCHANGE, routingKey, note);
    }

    private RabbitMqDemoPublishResult publish(RabbitMqBookingMessage message,
                                              String exchangeName,
                                              String routingKey,
                                              String note) {
        // 标准可靠投递流程第一步：先写本地消息表，再发 MQ。
        // 这里的 outboxId 是“本次投递记录”的 ID，messageId 是“业务幂等”的 ID，两者不要混用。
        String outboxId = UUID.randomUUID().toString();
        RabbitMqOutboxPublishTask task = outboxRepository.insertNew(
                outboxId,
                message,
                exchangeName,
                routingKey,
                note,
                MAX_PUBLISH_ATTEMPTS);
        publishOutboxTask(task);
        RabbitMqOutboxMessage localMessage = outboxRepository.findById(outboxId);
        return new RabbitMqDemoPublishResult(
                outboxId,
                message.messageId(),
                message.orderId(),
                exchangeName,
                routingKey,
                true,
                localMessage.status(),
                "async publisher confirm pending, check /rabbitmq/demo/status by outboxId",
                localMessage.attemptCount(),
                note);
    }

    private void publishOutboxTask(RabbitMqOutboxPublishTask task) {
        // 每次真正调用 RabbitTemplate 前先把状态改成 WAIT_CONFIRM，并递增 attempt_count。
        // 如果 JVM 在这里之后宕机，重启后的超时扫描可以发现 WAIT_CONFIRM 并补偿重试。
        outboxRepository.markWaitConfirm(task.outboxId());
        CorrelationData correlationData = new CorrelationData(task.outboxId());
        registerAsyncConfirm(task.outboxId(), correlationData);
        try {
            rabbitTemplate.convertAndSend(task.exchangeName(), task.routingKey(), task.payload(), amqpMessage -> {
                // messageId 放在 AMQP 标准属性里，消费者可直接读取，用来做幂等判断。
                amqpMessage.getMessageProperties().setMessageId(task.payload().messageId());
                // PERSISTENT 表示消息持久化；它要和 durable exchange/queue、publisher confirm 配合使用。
                amqpMessage.getMessageProperties().setDeliveryMode(MessageDeliveryMode.PERSISTENT);
                // outboxId 放到 header，mandatory return 回调才能知道要更新本地消息表的哪一行。
                amqpMessage.getMessageProperties().setHeader("x-outbox-id", task.outboxId());
                // 自定义 header 只是教学说明，RabbitMQ 控制台查看消息时能看到本次演示点。
                amqpMessage.getMessageProperties().setHeader("x-demo-feature", task.demoNote());
                return amqpMessage;
            }, correlationData);
            published.incrementAndGet();
        } catch (AmqpException ex) {
            // RabbitMQ 暂时不可用、连接失败等异常不会让接口丢失消息，因为本地消息表已经有记录。
            // 定时任务会根据 attempt_count 和 next_retry_at 继续补偿。
            scheduleRetryOrFail(task.outboxId(), "send exception: " + ex.getMessage());
        }
    }

    private void registerAsyncConfirm(String outboxId, CorrelationData correlationData) {
        // publisher confirm 本身就是异步模型：Broker 处理完后回调 ack/nack。
        // 生产端不能靠接口线程同步阻塞等待，否则吞吐会很差，也不符合常见面试标准方案。
        correlationData.getFuture().whenComplete((confirm, throwable) -> {
            if (throwable != null) {
                scheduleRetryOrFail(outboxId, "confirm callback failed: " + throwable.getMessage());
                return;
            }
            if (confirm.isAck()) {
                publishAcked.incrementAndGet();
                outboxRepository.markSent(outboxId);
                log.info("RabbitMQ publisher confirm ack: outboxId={}", outboxId);
                return;
            }
            scheduleRetryOrFail(outboxId, "broker nack: " + confirm.getReason());
        });
    }

    private void scheduleRetryOrFail(String outboxId, String reason) {
        int attemptCount = outboxRepository.attemptCount(outboxId);
        if (attemptCount >= MAX_PUBLISH_ATTEMPTS) {
            outboxRepository.markFailed(outboxId, reason + ", max attempts reached");
            log.warn("RabbitMQ publish failed permanently: outboxId={}, attempts={}, reason={}",
                    outboxId, attemptCount, reason);
            return;
        }
        // 简单递增退避：第 1 次失败 5 秒后重试，第 2 次失败 10 秒后重试。
        Instant nextRetryAt = Instant.now().plus(BASE_RETRY_BACKOFF.multipliedBy(Math.max(1, attemptCount)));
        outboxRepository.markRetrying(outboxId, reason, nextRetryAt);
        log.warn("RabbitMQ publish scheduled for retry: outboxId={}, attempts={}, nextRetryAt={}, reason={}",
                outboxId, attemptCount, nextRetryAt, reason);
    }

    private RabbitMqBookingMessage newMessage(String messageId, boolean fail) {
        // demo 消息模拟“订单已创建事件”，金额、用户、房间固定，便于反复演示 MQ 行为。
        return new RabbitMqBookingMessage(
                normalizeMessageId(messageId),
                "ORD-" + UUID.randomUUID().toString().substring(0, 8),
                1L,
                101L,
                new BigDecimal("399.00"),
                fail,
                Instant.now());
    }

    private String normalizeMessageId(String messageId) {
        // 调接口不传 messageId 时自动生成；传入固定 messageId 时可稳定复现重复消费场景。
        if (messageId == null || messageId.isBlank()) {
            return "MSG-" + UUID.randomUUID().toString().substring(0, 8);
        }
        return messageId;
    }

    private String normalizeTopicRoutingKey(String routingKey) {
        // routing key 的每一段用英文点号分隔。默认值会同时命中 * 和 # 两个演示队列。
        if (routingKey == null || routingKey.isBlank()) {
            return RabbitMqDemoConfig.BOOKING_ROUTING_KEY;
        }
        return routingKey;
    }

    private String normalizeCompensationNote(String compensationNote) {
        if (compensationNote == null || compensationNote.isBlank()) {
            return "manual troubleshooting or compensation has been completed";
        }
        return compensationNote;
    }

    private RabbitMqBookingMessage readBookingMessageOrNull(String payloadJson) {
        try {
            return objectMapper.readValue(payloadJson, RabbitMqBookingMessage.class);
        } catch (IOException ex) {
            // 生产里即使 payload 反序列化失败，也应该把原始消息登记成事故，方便人工查看原文后修复。
            return null;
        }
    }

    private String headersJson(Map<String, Object> headers) throws IOException {
        if (headers == null || headers.isEmpty()) {
            return "{}";
        }
        Map<String, String> simpleHeaders = new java.util.LinkedHashMap<>();
        headers.forEach((key, value) -> simpleHeaders.put(key, value == null ? null : value.toString()));
        return objectMapper.writeValueAsString(simpleHeaders);
    }

    private String dlqSourceKey(Message rawMessage, String payloadJson, String messageId) {
        // 优先使用 outboxId，因为它代表一次生产端投递记录；没有 outboxId 时退回到 messageId；
        // 两者都没有时用 payload 哈希，保证同一条 DLQ 消息重投时仍然落到同一个 incident。
        Object outboxId = rawMessage.getMessageProperties().getHeaders().get("x-outbox-id");
        String stableId = outboxId == null ? null : outboxId.toString();
        if (stableId == null || stableId.isBlank()) {
            stableId = messageId;
        }
        if (stableId == null || stableId.isBlank()) {
            stableId = sha256(payloadJson);
        }
        return RabbitMqDemoConfig.BOOKING_DLQ + ":" + stableId;
    }

    private String sha256(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 is not available", ex);
        }
    }

    private String firstDeathReason(Map<String, Object> headers) {
        // RabbitMQ 会在死信消息 header 里写入 x-first-death-reason/x-death，帮助定位死信来源。
        // 本 demo 只取最容易读懂的首次死信原因给接口返回。
        if (headers == null) {
            return null;
        }
        Object reason = headers.get("x-first-death-reason");
        return reason == null ? null : reason.toString();
    }

    private void record(String messageId, String orderId, String status, String detail) {
        RabbitMqConsumedEvent event = new RabbitMqConsumedEvent(messageId, orderId, status, detail, Instant.now());
        synchronized (recentEvents) {
            recentEvents.add(event);
            if (recentEvents.size() > MAX_RECENT_EVENTS) {
                // 超过上限就丢弃最老事件，保持状态接口轻量。
                recentEvents.remove(0);
            }
        }
        log.info("RabbitMQ demo message {}: messageId={}, orderId={}, detail={}", status, messageId, orderId, detail);
    }
}
