package com.example.hotel.mqdemo.bff.application.service;

import com.example.hotel.common.api.ApiResponse;
import com.example.hotel.mqdemo.bff.application.port.out.MessageDemoGateway;
import org.springframework.stereotype.Service;

@Service
public class MqDemoFacadeService {

    private final MessageDemoGateway messageDemoGateway;

    public MqDemoFacadeService(MessageDemoGateway messageDemoGateway) {
        this.messageDemoGateway = messageDemoGateway;
    }

    public ApiResponse<Object> bookingEvents() {
        return messageDemoGateway.bookingEvents();
    }

    public ApiResponse<Object> rabbitBookingEvents() {
        return messageDemoGateway.rabbitBookingEvents();
    }

    public ApiResponse<Object> kafkaBookingEvents() {
        return messageDemoGateway.kafkaBookingEvents();
    }

    public ApiResponse<Object> publishRabbitNormal(String messageId) {
        return messageDemoGateway.publishRabbitNormal(messageId);
    }

    public ApiResponse<Object> publishRabbitDuplicate(String messageId) {
        return messageDemoGateway.publishRabbitDuplicate(messageId);
    }

    public ApiResponse<Object> publishRabbitDeadLetter(String messageId) {
        return messageDemoGateway.publishRabbitDeadLetter(messageId);
    }

    public ApiResponse<Object> rabbitDlqIncidents(String status) {
        return messageDemoGateway.rabbitDlqIncidents(status);
    }

    public ApiResponse<Object> resolveRabbitDlqIncident(String incidentId, String compensationNote) {
        return messageDemoGateway.resolveRabbitDlqIncident(incidentId, compensationNote);
    }

    public ApiResponse<Object> publishRabbitUnroutable(String messageId) {
        return messageDemoGateway.publishRabbitUnroutable(messageId);
    }

    public ApiResponse<Object> publishRabbitTopic(String messageId, String routingKey) {
        return messageDemoGateway.publishRabbitTopic(messageId, routingKey);
    }

    public ApiResponse<Object> publishRabbitFanout(String messageId) {
        return messageDemoGateway.publishRabbitFanout(messageId);
    }

    public ApiResponse<Object> rabbitStatus() {
        return messageDemoGateway.rabbitStatus();
    }

    public ApiResponse<Object> publishKafkaNormal(String messageId, String key) {
        return messageDemoGateway.publishKafkaNormal(messageId, key);
    }

    public ApiResponse<Object> publishKafkaKeyOrder(String key, int count) {
        return messageDemoGateway.publishKafkaKeyOrder(key, count);
    }

    public ApiResponse<Object> publishKafkaConsumerGroupBatch(int count) {
        return messageDemoGateway.publishKafkaConsumerGroupBatch(count);
    }

    public ApiResponse<Object> publishKafkaDuplicate(String messageId) {
        return messageDemoGateway.publishKafkaDuplicate(messageId);
    }

    public ApiResponse<Object> publishKafkaDeadLetter(String messageId, String key) {
        return messageDemoGateway.publishKafkaDeadLetter(messageId, key);
    }

    public ApiResponse<Object> kafkaStatus() {
        return messageDemoGateway.kafkaStatus();
    }
}
