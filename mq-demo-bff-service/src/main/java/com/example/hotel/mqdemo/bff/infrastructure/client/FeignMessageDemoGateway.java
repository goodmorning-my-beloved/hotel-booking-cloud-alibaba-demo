package com.example.hotel.mqdemo.bff.infrastructure.client;

import com.example.hotel.common.api.ApiResponse;
import com.example.hotel.mqdemo.bff.application.port.out.MessageDemoGateway;
import org.springframework.stereotype.Component;

@Component
public class FeignMessageDemoGateway implements MessageDemoGateway {

    private final MessageServiceClient messageServiceClient;

    public FeignMessageDemoGateway(MessageServiceClient messageServiceClient) {
        this.messageServiceClient = messageServiceClient;
    }

    @Override
    public ApiResponse<Object> bookingEvents() {
        return messageServiceClient.bookingEvents();
    }

    @Override
    public ApiResponse<Object> rabbitBookingEvents() {
        return messageServiceClient.rabbitBookingEvents();
    }

    @Override
    public ApiResponse<Object> kafkaBookingEvents() {
        return messageServiceClient.kafkaBookingEvents();
    }

    @Override
    public ApiResponse<Object> publishRabbitNormal(String messageId) {
        return messageServiceClient.publishRabbitNormal(messageId);
    }

    @Override
    public ApiResponse<Object> publishRabbitDuplicate(String messageId) {
        return messageServiceClient.publishRabbitDuplicate(messageId);
    }

    @Override
    public ApiResponse<Object> publishRabbitDeadLetter(String messageId) {
        return messageServiceClient.publishRabbitDeadLetter(messageId);
    }

    @Override
    public ApiResponse<Object> rabbitDlqIncidents(String status) {
        return messageServiceClient.rabbitDlqIncidents(status);
    }

    @Override
    public ApiResponse<Object> resolveRabbitDlqIncident(String incidentId, String compensationNote) {
        return messageServiceClient.resolveRabbitDlqIncident(incidentId, compensationNote);
    }

    @Override
    public ApiResponse<Object> publishRabbitUnroutable(String messageId) {
        return messageServiceClient.publishRabbitUnroutable(messageId);
    }

    @Override
    public ApiResponse<Object> publishRabbitTopic(String messageId, String routingKey) {
        return messageServiceClient.publishRabbitTopic(messageId, routingKey);
    }

    @Override
    public ApiResponse<Object> publishRabbitFanout(String messageId) {
        return messageServiceClient.publishRabbitFanout(messageId);
    }

    @Override
    public ApiResponse<Object> rabbitStatus() {
        return messageServiceClient.rabbitStatus();
    }

    @Override
    public ApiResponse<Object> publishKafkaNormal(String messageId, String key) {
        return messageServiceClient.publishKafkaNormal(messageId, key);
    }

    @Override
    public ApiResponse<Object> publishKafkaKeyOrder(String key, int count) {
        return messageServiceClient.publishKafkaKeyOrder(key, count);
    }

    @Override
    public ApiResponse<Object> publishKafkaConsumerGroupBatch(int count) {
        return messageServiceClient.publishKafkaConsumerGroupBatch(count);
    }

    @Override
    public ApiResponse<Object> publishKafkaDuplicate(String messageId) {
        return messageServiceClient.publishKafkaDuplicate(messageId);
    }

    @Override
    public ApiResponse<Object> publishKafkaDeadLetter(String messageId, String key) {
        return messageServiceClient.publishKafkaDeadLetter(messageId, key);
    }

    @Override
    public ApiResponse<Object> kafkaStatus() {
        return messageServiceClient.kafkaStatus();
    }
}
