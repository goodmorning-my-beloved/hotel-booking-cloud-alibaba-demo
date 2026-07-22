package com.example.hotel.message.application.port.out;

import java.util.List;
import java.util.Map;

public interface RabbitMqTopology {

    String bookingExchange();

    String bookingRoutingKey();

    String bookingQueue();

    String deadLetterExchange();

    String deadLetterQueue();

    String topicExchange();

    Map<String, String> topicBindings();

    String fanoutExchange();

    List<String> fanoutQueues();
}
