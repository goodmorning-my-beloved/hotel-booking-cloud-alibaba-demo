package com.example.hotel.mqdemo.bff.infrastructure.client;

import com.example.hotel.common.api.ApiResponse;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

@FeignClient(name = "message-service")
public interface MessageServiceClient {

    @GetMapping("/messages/booking-events")
    ApiResponse<Object> bookingEvents();

    @GetMapping("/messages/booking-events/rabbitmq")
    ApiResponse<Object> rabbitBookingEvents();

    @GetMapping("/messages/booking-events/kafka")
    ApiResponse<Object> kafkaBookingEvents();

    @PostMapping("/messages/rabbitmq/demo/normal")
    ApiResponse<Object> publishRabbitNormal(@RequestParam(value = "messageId", required = false) String messageId);

    @PostMapping("/messages/rabbitmq/demo/duplicate")
    ApiResponse<Object> publishRabbitDuplicate(@RequestParam(value = "messageId", required = false) String messageId);

    @PostMapping("/messages/rabbitmq/demo/dead-letter")
    ApiResponse<Object> publishRabbitDeadLetter(@RequestParam(value = "messageId", required = false) String messageId);

    @GetMapping("/messages/dlq/incidents")
    ApiResponse<Object> rabbitDlqIncidents(@RequestParam(value = "status", required = false) String status);

    @PostMapping("/messages/dlq/incidents/{incidentId}/resolve")
    ApiResponse<Object> resolveRabbitDlqIncident(
            @PathVariable("incidentId") String incidentId,
            @RequestParam(value = "compensationNote", required = false) String compensationNote);

    @PostMapping("/messages/rabbitmq/demo/unroutable")
    ApiResponse<Object> publishRabbitUnroutable(@RequestParam(value = "messageId", required = false) String messageId);

    @PostMapping("/messages/rabbitmq/demo/topic")
    ApiResponse<Object> publishRabbitTopic(
            @RequestParam(value = "messageId", required = false) String messageId,
            @RequestParam(value = "routingKey", required = false) String routingKey);

    @PostMapping("/messages/rabbitmq/demo/fanout")
    ApiResponse<Object> publishRabbitFanout(@RequestParam(value = "messageId", required = false) String messageId);

    @GetMapping("/messages/rabbitmq/demo/status")
    ApiResponse<Object> rabbitStatus();

    @PostMapping("/messages/kafka/demo/normal")
    ApiResponse<Object> publishKafkaNormal(
            @RequestParam(value = "messageId", required = false) String messageId,
            @RequestParam(value = "key", required = false) String key);

    @PostMapping("/messages/kafka/demo/key-order")
    ApiResponse<Object> publishKafkaKeyOrder(
            @RequestParam(value = "key", required = false) String key,
            @RequestParam(value = "count", defaultValue = "5") int count);

    @PostMapping("/messages/kafka/demo/group")
    ApiResponse<Object> publishKafkaConsumerGroupBatch(@RequestParam(value = "count", defaultValue = "9") int count);

    @PostMapping("/messages/kafka/demo/async-batch")
    ApiResponse<Object> publishKafkaAsyncBatch(@RequestParam(value = "count", defaultValue = "20") int count);

    @PostMapping("/messages/kafka/demo/duplicate")
    ApiResponse<Object> publishKafkaDuplicate(@RequestParam(value = "messageId", required = false) String messageId);

    @PostMapping("/messages/kafka/demo/dead-letter")
    ApiResponse<Object> publishKafkaDeadLetter(
            @RequestParam(value = "messageId", required = false) String messageId,
            @RequestParam(value = "key", required = false) String key);

    @PostMapping("/messages/kafka/demo/poison")
    ApiResponse<Object> publishKafkaPoison(@RequestParam(value = "key", required = false) String key);

    @PostMapping("/messages/kafka/demo/transaction")
    ApiResponse<Object> publishKafkaTransaction(
            @RequestParam(value = "failAfterFirst", defaultValue = "false") boolean failAfterFirst);

    @PostMapping("/messages/kafka/demo/consumer/pause")
    ApiResponse<Object> pauseKafkaPrimaryConsumer();

    @PostMapping("/messages/kafka/demo/consumer/resume")
    ApiResponse<Object> resumeKafkaPrimaryConsumer();

    @PostMapping("/messages/kafka/demo/dlt/incidents/{incidentId}/resolve")
    ApiResponse<Object> resolveKafkaDltIncident(
            @org.springframework.web.bind.annotation.PathVariable("incidentId") String incidentId,
            @RequestParam(value = "resolutionNote", required = false) String resolutionNote);

    @GetMapping("/messages/kafka/demo/status")
    ApiResponse<Object> kafkaStatus();
}
