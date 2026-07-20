package com.example.hotel.message;

public record RabbitMqDemoPublishResult(
        String messageId,
        String orderId,
        String exchange,
        String routingKey,
        boolean persistentMessage,
        String confirm,
        String note
) {
}
