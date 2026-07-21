package com.example.hotel.message;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.GetResponse;
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
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
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
    // 下面这些计数器只是 demo 状态展示，帮助你把接口调用和 RabbitMQ 控制台变化对应起来。
    private final AtomicLong published = new AtomicLong();
    private final AtomicLong publishAcked = new AtomicLong();
    private final AtomicLong returned = new AtomicLong();
    private final AtomicLong consumed = new AtomicLong();
    private final AtomicLong duplicated = new AtomicLong();
    private final AtomicLong rejectedToDeadLetter = new AtomicLong();
    private final AtomicLong deadLetterResolved = new AtomicLong();
    // 幂等演示：用 messageId 记录已处理消息。生产环境通常换成数据库唯一索引或 Redis SETNX。
    private final Set<String> processedMessageIds = ConcurrentHashMap.newKeySet();
    // ArrayList 不是线程安全的，读写时在 record/status 里用 synchronized 保护。
    private final List<RabbitMqConsumedEvent> recentEvents = new ArrayList<>();

    public RabbitMqDemoService(RabbitTemplate rabbitTemplate,
                               ObjectMapper objectMapper,
                               RabbitMqOutboxRepository outboxRepository) {
        this.rabbitTemplate = rabbitTemplate;
        this.objectMapper = objectMapper;
        this.outboxRepository = outboxRepository;
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
                published.get(),
                publishAcked.get(),
                returned.get(),
                consumed.get(),
                duplicated.get(),
                rejectedToDeadLetter.get(),
                deadLetterResolved.get(),
                outboxRepository.countByStatus(),
                outboxRepository.recent(MAX_RECENT_OUTBOX_MESSAGES),
                ids,
                events);
    }

    public RabbitMqDlqResolveResult resolveNextDeadLetter(String compensationNote) {
        // RabbitMQ 的 queue 不是数据库表，不能按 id 随机删除某一行。
        // 生产里通常用“DLQ 修复/补偿工具”顺序取消息：先排障或补偿，成功后 ACK；失败则不要 ACK。
        return rabbitTemplate.execute(channel -> {
            // autoAck=false 是关键：取出 DLQ 消息后先保持 unacked，直到补偿成功才 basicAck 删除。
            GetResponse response = channel.basicGet(RabbitMqDemoConfig.BOOKING_DLQ, false);
            if (response == null) {
                return new RabbitMqDlqResolveResult(
                        false,
                        RabbitMqDemoConfig.BOOKING_DLQ,
                        null,
                        null,
                        null,
                        "no-message",
                        normalizeCompensationNote(compensationNote),
                        "DLQ has no ready message to resolve",
                        Instant.now());
            }

            long deliveryTag = response.getEnvelope().getDeliveryTag();
            try {
                RabbitMqBookingMessage payload = objectMapper.readValue(response.getBody(), RabbitMqBookingMessage.class);
                String messageId = response.getProps().getMessageId();
                if (messageId == null || messageId.isBlank()) {
                    messageId = payload.messageId();
                }
                String note = normalizeCompensationNote(compensationNote);

                // 这里用 processedMessageIds 模拟“补偿动作已经落库”的幂等记录。
                // 真实生产系统应写补偿流水表、订单状态表或人工工单表，并用唯一键防止重复补偿。
                boolean firstCompensation = processedMessageIds.add(messageId);
                String compensationDetail = firstCompensation
                        ? "manual compensation finished, then ACK DLQ message"
                        : "messageId already compensated before, ACK duplicate DLQ message without side effects";

                // 对 DLQ 消息执行 basicAck 后，RabbitMQ 会把这条消息从 hotel.booking.created.dlq 删除。
                // 如果补偿失败，应该 basicNack(requeue=true) 或转存到 parking-lot 队列，不能 ACK 掉。
                channel.basicAck(deliveryTag, false);
                deadLetterResolved.incrementAndGet();
                record(messageId, payload.orderId(), "dlq-resolved", compensationDetail + "; note=" + note);

                return new RabbitMqDlqResolveResult(
                        true,
                        RabbitMqDemoConfig.BOOKING_DLQ,
                        messageId,
                        payload.orderId(),
                        firstDeathReason(response.getProps().getHeaders()),
                        "basicAck-delete",
                        note,
                        "compensation succeeded before ACK, so RabbitMQ removes the message from the DLQ",
                        Instant.now());
            } catch (Exception ex) {
                // 只要补偿或反序列化过程失败，就不要 ACK；这里 requeue=true 让消息回到 DLQ，方便继续排查。
                channel.basicNack(deliveryTag, false, true);
                throw new IllegalStateException("Failed to resolve DLQ message, message has been requeued", ex);
            }
        });
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

    private RabbitMqDemoPublishResult publish(RabbitMqBookingMessage message, String routingKey, String note) {
        // 标准可靠投递流程第一步：先写本地消息表，再发 MQ。
        // 这里的 outboxId 是“本次投递记录”的 ID，messageId 是“业务幂等”的 ID，两者不要混用。
        String outboxId = UUID.randomUUID().toString();
        RabbitMqOutboxPublishTask task = outboxRepository.insertNew(
                outboxId,
                message,
                RabbitMqDemoConfig.BOOKING_EXCHANGE,
                routingKey,
                note,
                MAX_PUBLISH_ATTEMPTS);
        publishOutboxTask(task);
        RabbitMqOutboxMessage localMessage = outboxRepository.findById(outboxId);
        return new RabbitMqDemoPublishResult(
                outboxId,
                message.messageId(),
                message.orderId(),
                RabbitMqDemoConfig.BOOKING_EXCHANGE,
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

    private String normalizeCompensationNote(String compensationNote) {
        if (compensationNote == null || compensationNote.isBlank()) {
            return "manual troubleshooting or compensation has been completed";
        }
        return compensationNote;
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
