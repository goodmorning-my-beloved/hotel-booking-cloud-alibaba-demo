package com.example.hotel.message;

import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class RabbitMqDemoService {

    private static final Logger log = LoggerFactory.getLogger(RabbitMqDemoService.class);
    // 只保留最近的消费事件，避免 /status 接口返回无限增长的数据。
    private static final int MAX_RECENT_EVENTS = 50;

    // RabbitTemplate 是 Spring AMQP 的生产者工具类，负责把 Java 对象发送到 RabbitMQ。
    private final RabbitTemplate rabbitTemplate;
    // 下面这些计数器只是 demo 状态展示，帮助你把接口调用和 RabbitMQ 控制台变化对应起来。
    private final AtomicLong published = new AtomicLong();
    private final AtomicLong publishAcked = new AtomicLong();
    private final AtomicLong returned = new AtomicLong();
    private final AtomicLong consumed = new AtomicLong();
    private final AtomicLong duplicated = new AtomicLong();
    private final AtomicLong rejectedToDeadLetter = new AtomicLong();
    // 幂等演示：用 messageId 记录已处理消息。生产环境通常换成数据库唯一索引或 Redis SETNX。
    private final Set<String> processedMessageIds = ConcurrentHashMap.newKeySet();
    // ArrayList 不是线程安全的，读写时在 record/status 里用 synchronized 保护。
    private final List<RabbitMqConsumedEvent> recentEvents = new ArrayList<>();

    public RabbitMqDemoService(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
        // mandatory=true 且消息到达 exchange 但没有匹配队列时，会触发 returns callback。
        // 这能演示“消息到了交换机，却没有进任何队列”的不可路由问题。
        this.rabbitTemplate.setReturnsCallback(returnedMessage -> {
            returned.incrementAndGet();
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
                ids,
                events);
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
        // CorrelationData 用 messageId 关联本次发送和 Broker confirm 回调结果。
        CorrelationData correlationData = new CorrelationData(message.messageId());
        try {
            rabbitTemplate.convertAndSend(RabbitMqDemoConfig.BOOKING_EXCHANGE, routingKey, message, amqpMessage -> {
                // messageId 放在 AMQP 标准属性里，消费者可直接读取，用来做幂等判断。
                amqpMessage.getMessageProperties().setMessageId(message.messageId());
                // PERSISTENT 表示消息持久化；它要和 durable exchange/queue、publisher confirm 配合使用。
                amqpMessage.getMessageProperties().setDeliveryMode(MessageDeliveryMode.PERSISTENT);
                // 自定义 header 只是教学说明，RabbitMQ 控制台查看消息时能看到本次演示点。
                amqpMessage.getMessageProperties().setHeader("x-demo-feature", note);
                return amqpMessage;
            }, correlationData);
            published.incrementAndGet();
            // confirm 只说明 Broker 是否接收了消息；能否路由到队列还要看 mandatory return。
            String confirm = waitForConfirm(correlationData);
            return new RabbitMqDemoPublishResult(
                    message.messageId(),
                    message.orderId(),
                    RabbitMqDemoConfig.BOOKING_EXCHANGE,
                    routingKey,
                    true,
                    confirm,
                    note);
        } catch (AmqpException ex) {
            throw new IllegalStateException("RabbitMQ publish failed: " + ex.getMessage(), ex);
        }
    }

    private String waitForConfirm(CorrelationData correlationData) {
        try {
            // publisher confirm 是异步结果；这里最多等 5 秒，方便接口响应里直接展示 ack/nack。
            CorrelationData.Confirm confirm = correlationData.getFuture().get(5, TimeUnit.SECONDS);
            if (confirm.isAck()) {
                publishAcked.incrementAndGet();
                return "broker ack";
            }
            return "broker nack: " + confirm.getReason();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return "confirm interrupted";
        } catch (TimeoutException ex) {
            return "confirm timeout";
        } catch (Exception ex) {
            return "confirm failed: " + ex.getMessage();
        }
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
