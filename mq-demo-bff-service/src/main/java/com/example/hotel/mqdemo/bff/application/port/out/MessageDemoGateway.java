package com.example.hotel.mqdemo.bff.application.port.out;

import com.example.hotel.common.api.ApiResponse;

public interface MessageDemoGateway {

    ApiResponse<Object> bookingEvents();

    ApiResponse<Object> rabbitBookingEvents();

    ApiResponse<Object> kafkaBookingEvents();

    ApiResponse<Object> publishRabbitNormal(String messageId);

    ApiResponse<Object> publishRabbitDuplicate(String messageId);

    ApiResponse<Object> publishRabbitDeadLetter(String messageId);

    ApiResponse<Object> rabbitDlqIncidents(String status);

    ApiResponse<Object> resolveRabbitDlqIncident(String incidentId, String compensationNote);

    ApiResponse<Object> publishRabbitUnroutable(String messageId);

    ApiResponse<Object> publishRabbitTopic(String messageId, String routingKey);

    ApiResponse<Object> publishRabbitFanout(String messageId);

    ApiResponse<Object> rabbitStatus();

    ApiResponse<Object> publishKafkaNormal(String messageId, String key);

    ApiResponse<Object> publishKafkaKeyOrder(String key, int count);

    ApiResponse<Object> publishKafkaConsumerGroupBatch(int count);

    ApiResponse<Object> publishKafkaDuplicate(String messageId);

    ApiResponse<Object> publishKafkaDeadLetter(String messageId, String key);

    ApiResponse<Object> kafkaStatus();
}
