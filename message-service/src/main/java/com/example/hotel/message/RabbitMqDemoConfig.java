package com.example.hotel.message;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@EnableRabbit
@Configuration
public class RabbitMqDemoConfig {

    public static final String BOOKING_EXCHANGE = "hotel.booking.exchange";
    public static final String BOOKING_ROUTING_KEY = "hotel.booking.created";
    public static final String BOOKING_QUEUE = "hotel.booking.created.queue";
    public static final String BOOKING_DLX = "hotel.booking.dlx";
    public static final String BOOKING_DLQ_ROUTING_KEY = "hotel.booking.created.dead";
    public static final String BOOKING_DLQ = "hotel.booking.created.dlq";

    @Bean
    DirectExchange bookingExchange() {
        return new DirectExchange(BOOKING_EXCHANGE, true, false);
    }

    @Bean
    DirectExchange bookingDeadLetterExchange() {
        return new DirectExchange(BOOKING_DLX, true, false);
    }

    @Bean
    Queue bookingQueue() {
        return QueueBuilder.durable(BOOKING_QUEUE)
                .deadLetterExchange(BOOKING_DLX)
                .deadLetterRoutingKey(BOOKING_DLQ_ROUTING_KEY)
                .build();
    }

    @Bean
    Queue bookingDeadLetterQueue() {
        return QueueBuilder.durable(BOOKING_DLQ).build();
    }

    @Bean
    Binding bookingBinding(@Qualifier("bookingQueue") Queue bookingQueue,
                           @Qualifier("bookingExchange") DirectExchange bookingExchange) {
        return BindingBuilder.bind(bookingQueue).to(bookingExchange).with(BOOKING_ROUTING_KEY);
    }

    @Bean
    Binding bookingDeadLetterBinding(@Qualifier("bookingDeadLetterQueue") Queue bookingDeadLetterQueue,
                                     @Qualifier("bookingDeadLetterExchange") DirectExchange bookingDeadLetterExchange) {
        return BindingBuilder.bind(bookingDeadLetterQueue)
                .to(bookingDeadLetterExchange)
                .with(BOOKING_DLQ_ROUTING_KEY);
    }

    @Bean
    MessageConverter messageConverter() {
        return new Jackson2JsonMessageConverter();
    }
}
