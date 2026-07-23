package com.example.hotel.mqdemo.bff.interfaces.rest;

import com.example.hotel.common.api.ApiResponse;
import com.example.hotel.mqdemo.bff.application.service.MqDemoFacadeService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/mq-demo")
public class MqDemoController {

    private final MqDemoFacadeService facadeService;

    public MqDemoController(MqDemoFacadeService facadeService) {
        this.facadeService = facadeService;
    }

    @GetMapping("/booking-events")
    public ApiResponse<Object> bookingEvents() {
        return facadeService.bookingEvents();
    }

    @GetMapping("/booking-events/rabbitmq")
    public ApiResponse<Object> rabbitBookingEvents() {
        return facadeService.rabbitBookingEvents();
    }

    @GetMapping("/booking-events/kafka")
    public ApiResponse<Object> kafkaBookingEvents() {
        return facadeService.kafkaBookingEvents();
    }

    @PostMapping("/rabbitmq/normal")
    public ApiResponse<Object> publishRabbitNormal(
            @RequestParam(value = "messageId", required = false) String messageId) {
        return facadeService.publishRabbitNormal(messageId);
    }

    @PostMapping("/rabbitmq/duplicate")
    public ApiResponse<Object> publishRabbitDuplicate(
            @RequestParam(value = "messageId", required = false) String messageId) {
        return facadeService.publishRabbitDuplicate(messageId);
    }

    @PostMapping("/rabbitmq/dead-letter")
    public ApiResponse<Object> publishRabbitDeadLetter(
            @RequestParam(value = "messageId", required = false) String messageId) {
        return facadeService.publishRabbitDeadLetter(messageId);
    }

    @GetMapping("/rabbitmq/dlq/incidents")
    public ApiResponse<Object> rabbitDlqIncidents(
            @RequestParam(value = "status", required = false) String status) {
        return facadeService.rabbitDlqIncidents(status);
    }

    @PostMapping("/rabbitmq/dlq/incidents/{incidentId}/resolve")
    public ApiResponse<Object> resolveRabbitDlqIncident(
            @PathVariable("incidentId") String incidentId,
            @RequestParam(value = "compensationNote", required = false) String compensationNote) {
        return facadeService.resolveRabbitDlqIncident(incidentId, compensationNote);
    }

    @PostMapping("/rabbitmq/unroutable")
    public ApiResponse<Object> publishRabbitUnroutable(
            @RequestParam(value = "messageId", required = false) String messageId) {
        return facadeService.publishRabbitUnroutable(messageId);
    }

    @PostMapping("/rabbitmq/topic")
    public ApiResponse<Object> publishRabbitTopic(
            @RequestParam(value = "messageId", required = false) String messageId,
            @RequestParam(value = "routingKey", required = false) String routingKey) {
        return facadeService.publishRabbitTopic(messageId, routingKey);
    }

    @PostMapping("/rabbitmq/fanout")
    public ApiResponse<Object> publishRabbitFanout(
            @RequestParam(value = "messageId", required = false) String messageId) {
        return facadeService.publishRabbitFanout(messageId);
    }

    @GetMapping("/rabbitmq/status")
    public ApiResponse<Object> rabbitStatus() {
        return facadeService.rabbitStatus();
    }

    @PostMapping("/kafka/normal")
    public ApiResponse<Object> publishKafkaNormal(
            @RequestParam(value = "messageId", required = false) String messageId,
            @RequestParam(value = "key", required = false) String key) {
        return facadeService.publishKafkaNormal(messageId, key);
    }

    @PostMapping("/kafka/key-order")
    public ApiResponse<Object> publishKafkaKeyOrder(
            @RequestParam(value = "key", required = false) String key,
            @RequestParam(value = "count", defaultValue = "5") int count) {
        return facadeService.publishKafkaKeyOrder(key, count);
    }

    @PostMapping("/kafka/group")
    public ApiResponse<Object> publishKafkaConsumerGroupBatch(
            @RequestParam(value = "count", defaultValue = "9") int count) {
        return facadeService.publishKafkaConsumerGroupBatch(count);
    }

    @PostMapping("/kafka/async-batch")
    public ApiResponse<Object> publishKafkaAsyncBatch(
            @RequestParam(value = "count", defaultValue = "20") int count) {
        return facadeService.publishKafkaAsyncBatch(count);
    }

    @PostMapping("/kafka/duplicate")
    public ApiResponse<Object> publishKafkaDuplicate(
            @RequestParam(value = "messageId", required = false) String messageId) {
        return facadeService.publishKafkaDuplicate(messageId);
    }

    @PostMapping("/kafka/dead-letter")
    public ApiResponse<Object> publishKafkaDeadLetter(
            @RequestParam(value = "messageId", required = false) String messageId,
            @RequestParam(value = "key", required = false) String key) {
        return facadeService.publishKafkaDeadLetter(messageId, key);
    }

    @PostMapping("/kafka/poison")
    public ApiResponse<Object> publishKafkaPoison(
            @RequestParam(value = "key", required = false) String key) {
        return facadeService.publishKafkaPoison(key);
    }

    @PostMapping("/kafka/transaction")
    public ApiResponse<Object> publishKafkaTransaction(
            @RequestParam(value = "failAfterFirst", defaultValue = "false") boolean failAfterFirst) {
        return facadeService.publishKafkaTransaction(failAfterFirst);
    }

    @PostMapping("/kafka/consumer/pause")
    public ApiResponse<Object> pauseKafkaPrimaryConsumer() {
        return facadeService.pauseKafkaPrimaryConsumer();
    }

    @PostMapping("/kafka/consumer/resume")
    public ApiResponse<Object> resumeKafkaPrimaryConsumer() {
        return facadeService.resumeKafkaPrimaryConsumer();
    }

    @PostMapping("/kafka/dlt/incidents/{incidentId}/resolve")
    public ApiResponse<Object> resolveKafkaDltIncident(
            @org.springframework.web.bind.annotation.PathVariable("incidentId") String incidentId,
            @RequestParam(value = "resolutionNote", required = false) String resolutionNote) {
        return facadeService.resolveKafkaDltIncident(incidentId, resolutionNote);
    }

    @GetMapping("/kafka/status")
    public ApiResponse<Object> kafkaStatus() {
        return facadeService.kafkaStatus();
    }

    @PostMapping("/jvm-gc/allocate-young")
    public ApiResponse<Object> allocateYoungObjects(
            @RequestParam(value = "objects", defaultValue = "2000") int objects,
            @RequestParam(value = "sizeKb", defaultValue = "32") int sizeKb) {
        return facadeService.allocateYoungObjects(objects, sizeKb);
    }

    @PostMapping("/jvm-gc/retain-humongous")
    public ApiResponse<Object> retainHumongousObjects(
            @RequestParam(value = "objects", defaultValue = "8") int objects,
            @RequestParam(value = "sizeMb", defaultValue = "2") int sizeMb) {
        return facadeService.retainHumongousObjects(objects, sizeMb);
    }

    @PostMapping("/jvm-gc/explicit-gc")
    public ApiResponse<Object> explicitGc(
            @RequestParam(value = "times", defaultValue = "1") int times) {
        return facadeService.explicitGc(times);
    }

    @PostMapping("/jvm-gc/clear")
    public ApiResponse<Object> clearJvmGcDemoObjects() {
        return facadeService.clearJvmGcDemoObjects();
    }

    @GetMapping("/jvm-gc/status")
    public ApiResponse<Object> jvmGcDemoStatus() {
        return facadeService.jvmGcDemoStatus();
    }
}
