package com.example.hotel.message.infrastructure.config;

import com.example.hotel.message.application.port.out.RabbitMqTopology;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public class ConfiguredRabbitMqTopology implements RabbitMqTopology {

    private final RabbitMqDemoProperties properties;

    public ConfiguredRabbitMqTopology(RabbitMqDemoProperties properties) {
        this.properties = properties;
    }

    @Override
    public String bookingExchange() {
        return properties.getBookingExchange();
    }

    @Override
    public String bookingRoutingKey() {
        return properties.getBookingRoutingKey();
    }

    @Override
    public String bookingQueue() {
        return properties.getBookingQueue();
    }

    @Override
    public String deadLetterExchange() {
        return properties.getDeadLetterExchange();
    }

    @Override
    public String deadLetterQueue() {
        return properties.getDeadLetterQueue();
    }

    @Override
    public String topicExchange() {
        return properties.getTopicExchange();
    }

    @Override
    public Map<String, String> topicBindings() {
        return Map.of(
                properties.getTopicSingleWordPattern(),
                properties.getTopicSingleWordQueue(),
                properties.getTopicMultiWordPattern(),
                properties.getTopicMultiWordQueue());
    }

    @Override
    public String fanoutExchange() {
        return properties.getFanoutExchange();
    }

    @Override
    public List<String> fanoutQueues() {
        return List.of(
                properties.getFanoutSmsQueue(),
                properties.getFanoutPointsQueue());
    }
}
