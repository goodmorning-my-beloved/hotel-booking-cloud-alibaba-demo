package com.example.hotel.message.infrastructure.config;

import com.example.hotel.message.application.port.out.RabbitMqTopology;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public class ConfiguredRabbitMqTopology implements RabbitMqTopology {

    @Override
    public String bookingExchange() {
        return RabbitMqDemoConfig.BOOKING_EXCHANGE;
    }

    @Override
    public String bookingRoutingKey() {
        return RabbitMqDemoConfig.BOOKING_ROUTING_KEY;
    }

    @Override
    public String bookingQueue() {
        return RabbitMqDemoConfig.BOOKING_QUEUE;
    }

    @Override
    public String deadLetterExchange() {
        return RabbitMqDemoConfig.BOOKING_DLX;
    }

    @Override
    public String deadLetterQueue() {
        return RabbitMqDemoConfig.BOOKING_DLQ;
    }

    @Override
    public String topicExchange() {
        return RabbitMqDemoConfig.BOOKING_TOPIC_EXCHANGE;
    }

    @Override
    public Map<String, String> topicBindings() {
        return Map.of(
                RabbitMqDemoConfig.BOOKING_TOPIC_SINGLE_WORD_PATTERN,
                RabbitMqDemoConfig.BOOKING_TOPIC_SINGLE_WORD_QUEUE,
                RabbitMqDemoConfig.BOOKING_TOPIC_MULTI_WORD_PATTERN,
                RabbitMqDemoConfig.BOOKING_TOPIC_MULTI_WORD_QUEUE);
    }

    @Override
    public String fanoutExchange() {
        return RabbitMqDemoConfig.BOOKING_FANOUT_EXCHANGE;
    }

    @Override
    public List<String> fanoutQueues() {
        return List.of(
                RabbitMqDemoConfig.BOOKING_FANOUT_SMS_QUEUE,
                RabbitMqDemoConfig.BOOKING_FANOUT_POINTS_QUEUE);
    }
}
