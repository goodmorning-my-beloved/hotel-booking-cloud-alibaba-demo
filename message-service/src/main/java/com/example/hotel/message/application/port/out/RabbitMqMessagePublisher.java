package com.example.hotel.message.application.port.out;

import com.example.hotel.message.application.result.RabbitMqDemoPublishResult;
import com.example.hotel.message.domain.model.RabbitMqBookingMessage;

public interface RabbitMqMessagePublisher {

    RabbitMqDemoPublishResult publish(RabbitMqBookingMessage message,
                                      String exchangeName,
                                      String routingKey,
                                      String note);

    void retryFailures();
}
