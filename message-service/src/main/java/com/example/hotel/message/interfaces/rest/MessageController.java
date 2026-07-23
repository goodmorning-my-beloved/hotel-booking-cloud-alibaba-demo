package com.example.hotel.message.interfaces.rest;

import com.example.hotel.common.api.ApiResponse;
import com.example.hotel.message.application.service.BookingEventApplicationService;
import com.example.hotel.message.application.service.KafkaDemoApplicationService;
import com.example.hotel.message.application.result.KafkaDemoPublishResult;
import com.example.hotel.message.application.result.KafkaDemoStatus;
import com.example.hotel.message.application.result.KafkaDemoTransactionResult;
import com.example.hotel.message.application.service.RabbitMqDemoApplicationService;
import com.example.hotel.message.application.result.RabbitMqDemoPublishResult;
import com.example.hotel.message.application.result.RabbitMqDemoStatus;
import com.example.hotel.message.application.result.RabbitMqDlqResolveResult;
import com.example.hotel.message.domain.model.RabbitMqDlqIncident;
import com.example.hotel.message.domain.model.ReceivedBookingEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/messages")
public class MessageController {

    private final RabbitMqDemoApplicationService rabbitMqDemoService;
    private final BookingEventApplicationService bookingEventService;
    private final KafkaDemoApplicationService kafkaDemoService;

    public MessageController(RabbitMqDemoApplicationService rabbitMqDemoService,
                             BookingEventApplicationService bookingEventService,
                             KafkaDemoApplicationService kafkaDemoService) {
        this.rabbitMqDemoService = rabbitMqDemoService;
        this.bookingEventService = bookingEventService;
        this.kafkaDemoService = kafkaDemoService;
    }

    @GetMapping("/booking-events")
    public ApiResponse<List<ReceivedBookingEvent>> bookingEvents() {
        // 完整酒店下单链路观测接口：order-service 创建订单后会发布 BookingCreatedEvent。
        return ApiResponse.ok(bookingEventService.recentEvents());
    }

    @GetMapping("/booking-events/rabbitmq")
    public ApiResponse<List<ReceivedBookingEvent>> rabbitBookingEvents() {
        // 只看 RabbitMQ binder 收到的订单事件，便于和 Kafka binder 对比。
        return ApiResponse.ok(bookingEventService.recentEvents("rabbitmq"));
    }

    @GetMapping("/booking-events/kafka")
    public ApiResponse<List<ReceivedBookingEvent>> kafkaBookingEvents() {
        // 只看 Kafka binder 收到的订单事件，证明同一业务事件也进入了 Kafka topic。
        return ApiResponse.ok(bookingEventService.recentEvents("kafka"));
    }

    @PostMapping("/rabbitmq/demo/normal")
    public ApiResponse<RabbitMqDemoPublishResult> publishNormal(
            @RequestParam(value = "messageId", required = false) String messageId) {
        // 普通可靠投递演示：persistent message + publisher confirm + consumer manual ack。
        return ApiResponse.ok(rabbitMqDemoService.publishNormal(messageId));
    }

    @PostMapping("/rabbitmq/demo/duplicate")
    public ApiResponse<List<RabbitMqDemoPublishResult>> publishDuplicate(
            @RequestParam(value = "messageId", required = false) String messageId) {
        // 幂等演示：连续发送两条相同 messageId 的消息，消费者只执行业务一次。
        return ApiResponse.ok(rabbitMqDemoService.publishDuplicate(messageId));
    }

    @PostMapping("/rabbitmq/demo/dead-letter")
    public ApiResponse<RabbitMqDemoPublishResult> publishDeadLetter(
            @RequestParam(value = "messageId", required = false) String messageId) {
        // 死信演示：消费者模拟失败并 nack(requeue=false)，消息进入 DLQ；
        // 随后 DLQ Listener 会自动把这条死信登记为 PENDING 补偿任务。
        return ApiResponse.ok(rabbitMqDemoService.publishDeadLetter(messageId));
    }

    @GetMapping("/dlq/incidents")
    public ApiResponse<List<RabbitMqDlqIncident>> dlqIncidents(
            @RequestParam(value = "status", required = false) String status) {
        // 人工处理前先查事故任务列表；常用 status=PENDING 找到还没补偿的 incidentId。
        return ApiResponse.ok(rabbitMqDemoService.dlqIncidents(status));
    }

    @PostMapping("/dlq/incidents/{incidentId}/resolve")
    public ApiResponse<RabbitMqDlqResolveResult> resolveDlqIncident(
            @PathVariable("incidentId") String incidentId,
            @RequestParam(value = "compensationNote", required = false) String compensationNote) {
        // DLQ 补偿演示：按指定 incidentId 处理 PENDING 任务，而不是从队列里随便取下一条。
        return ApiResponse.ok(rabbitMqDemoService.resolveDlqIncident(incidentId, compensationNote));
    }

    @PostMapping("/rabbitmq/demo/unroutable")
    public ApiResponse<RabbitMqDemoPublishResult> publishUnroutable(
            @RequestParam(value = "messageId", required = false) String messageId) {
        // 不可路由演示：routing key 没有绑定队列，mandatory return 会把问题反馈给生产者。
        return ApiResponse.ok(rabbitMqDemoService.publishUnroutable(messageId));
    }

    @PostMapping("/rabbitmq/demo/topic")
    public ApiResponse<RabbitMqDemoPublishResult> publishTopic(
            @RequestParam(value = "messageId", required = false) String messageId,
            @RequestParam(value = "routingKey", required = false) String routingKey) {
        // topic 通配演示：默认 hotel.booking.created 同时匹配 hotel.booking.* 和 hotel.booking.#。
        // 可以传 routingKey=hotel.booking.payment.timeout，观察 * 队列不匹配、# 队列匹配。
        return ApiResponse.ok(rabbitMqDemoService.publishTopic(messageId, routingKey));
    }

    @PostMapping("/rabbitmq/demo/fanout")
    public ApiResponse<RabbitMqDemoPublishResult> publishFanout(
            @RequestParam(value = "messageId", required = false) String messageId) {
        // fanout 广播演示：exchange 忽略 routing key，同一条消息复制到每个绑定队列。
        return ApiResponse.ok(rabbitMqDemoService.publishFanout(messageId));
    }

    @GetMapping("/rabbitmq/demo/status")
    public ApiResponse<RabbitMqDemoStatus> rabbitMqDemoStatus() {
        // 状态接口用于把发布、确认、退回、消费、重复、死信计数串起来观察。
        return ApiResponse.ok(rabbitMqDemoService.status());
    }

    @PostMapping("/kafka/demo/normal")
    public ApiResponse<KafkaDemoPublishResult> publishKafkaNormal(
            @RequestParam(value = "messageId", required = false) String messageId,
            @RequestParam(value = "key", required = false) String key) {
        // 普通生产演示：key 决定 partition，Broker ACK 后返回 topic/partition/offset。
        return ApiResponse.ok(kafkaDemoService.publishNormal(messageId, key));
    }

    @PostMapping("/kafka/demo/key-order")
    public ApiResponse<List<KafkaDemoPublishResult>> publishKafkaKeyOrder(
            @RequestParam(value = "key", required = false) String key,
            @RequestParam(value = "count", defaultValue = "5") int count) {
        // 同 key 有序演示：同一个 key 进入同一个 partition，在该 partition 内按 offset 有序。
        return ApiResponse.ok(kafkaDemoService.publishKeyOrder(key, count));
    }

    @PostMapping("/kafka/demo/group")
    public ApiResponse<List<KafkaDemoPublishResult>> publishKafkaConsumerGroupBatch(
            @RequestParam(value = "count", defaultValue = "9") int count) {
        // 消费者组演示：同组内多个消费者分摊 partition，不同 group 都会收到自己的消息副本。
        return ApiResponse.ok(kafkaDemoService.publishConsumerGroupBatch(count));
    }

    @PostMapping("/kafka/demo/async-batch")
    public ApiResponse<List<KafkaDemoPublishResult>> publishKafkaAsyncBatch(
            @RequestParam(value = "count", defaultValue = "20") int count) {
        return ApiResponse.ok(kafkaDemoService.publishAsyncBatch(count));
    }

    @PostMapping("/kafka/demo/duplicate")
    public ApiResponse<List<KafkaDemoPublishResult>> publishKafkaDuplicate(
            @RequestParam(value = "messageId", required = false) String messageId) {
        // 幂等演示：发送两条相同 messageId 的消息，消费者只执行业务一次，但 offset 都会推进。
        return ApiResponse.ok(kafkaDemoService.publishDuplicate(messageId));
    }

    @PostMapping("/kafka/demo/dead-letter")
    public ApiResponse<KafkaDemoPublishResult> publishKafkaDeadLetter(
            @RequestParam(value = "messageId", required = false) String messageId,
            @RequestParam(value = "key", required = false) String key) {
        // 失败重试和 DLT 演示：消费者抛异常，重试耗尽后进入 hotel.kafka.demo.orders.DLT。
        return ApiResponse.ok(kafkaDemoService.publishDeadLetter(messageId, key));
    }

    @PostMapping("/kafka/demo/poison")
    public ApiResponse<KafkaDemoPublishResult> publishKafkaPoison(
            @RequestParam(value = "key", required = false) String key) {
        return ApiResponse.ok(kafkaDemoService.publishPoison(key));
    }

    @PostMapping("/kafka/demo/transaction")
    public ApiResponse<KafkaDemoTransactionResult> publishKafkaTransaction(
            @RequestParam(value = "failAfterFirst", defaultValue = "false") boolean failAfterFirst) {
        return ApiResponse.ok(kafkaDemoService.publishTransaction(failAfterFirst));
    }

    @PostMapping("/kafka/demo/consumer/pause")
    public ApiResponse<KafkaDemoStatus> pauseKafkaPrimaryConsumer() {
        return ApiResponse.ok(kafkaDemoService.pausePrimaryConsumer());
    }

    @PostMapping("/kafka/demo/consumer/resume")
    public ApiResponse<KafkaDemoStatus> resumeKafkaPrimaryConsumer() {
        return ApiResponse.ok(kafkaDemoService.resumePrimaryConsumer());
    }

    @PostMapping("/kafka/demo/dlt/incidents/{incidentId}/resolve")
    public ApiResponse<KafkaDemoStatus> resolveKafkaDltIncident(
            @PathVariable("incidentId") String incidentId,
            @RequestParam(value = "resolutionNote", required = false) String resolutionNote) {
        return ApiResponse.ok(kafkaDemoService.resolveDltIncident(incidentId, resolutionNote));
    }

    @GetMapping("/kafka/demo/status")
    public ApiResponse<KafkaDemoStatus> kafkaDemoStatus() {
        // 状态接口展示 topic、partition、offset、consumer group lag、重复消息和 DLT 处理结果。
        return ApiResponse.ok(kafkaDemoService.status());
    }
}
