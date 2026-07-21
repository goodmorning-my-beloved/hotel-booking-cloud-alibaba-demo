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

    // 业务交换机：生产者先把消息发到 exchange，再由 exchange 根据 routing key 路由到 queue。
    public static final String BOOKING_EXCHANGE = "hotel.booking.exchange";
    // routing key 可以理解为“消息类型/路由地址”，direct exchange 要求完全匹配。
    public static final String BOOKING_ROUTING_KEY = "hotel.booking.created";
    // 正常消费队列：消费者监听这个队列来处理订单创建消息。
    public static final String BOOKING_QUEUE = "hotel.booking.created.queue";
    // 死信交换机 DLX：消费失败、TTL 过期、队列满等死信会被转发到这里。
    public static final String BOOKING_DLX = "hotel.booking.dlx";
    // 死信 routing key：正常队列把失败消息投到 DLX 时使用这个路由键。
    public static final String BOOKING_DLQ_ROUTING_KEY = "hotel.booking.created.dead";
    // 死信队列 DLQ：保存失败消息，方便在 RabbitMQ 控制台排查或后续补偿。
    public static final String BOOKING_DLQ = "hotel.booking.created.dlq";

    @Bean
    DirectExchange bookingExchange() {
        // direct exchange 按 routing key 精确匹配；durable=true 表示 Broker 重启后交换机仍然存在。
        // autoDelete=false 表示没有队列绑定时也不要自动删除，适合演示和生产里的固定拓扑。
        return new DirectExchange(BOOKING_EXCHANGE, true, false);
    }

    @Bean
    DirectExchange bookingDeadLetterExchange() {
        // DLX 本质也是一个普通 exchange，只是被队列的 x-dead-letter-exchange 参数引用。
        return new DirectExchange(BOOKING_DLX, true, false);
    }

    @Bean
    Queue bookingQueue() {
        // durable queue 只保证“队列元数据”重启后还在；消息也要设置 persistent 才能持久化。
        return QueueBuilder.durable(BOOKING_QUEUE)
                // 当前队列里的消息如果被 nack/reject 且 requeue=false，会被投递到这个 DLX。
                .deadLetterExchange(BOOKING_DLX)
                // 投递到 DLX 时使用这个 routing key，再由下面的 dead-letter binding 路由到 DLQ。
                .deadLetterRoutingKey(BOOKING_DLQ_ROUTING_KEY)
                .build();
    }

    @Bean
    Queue bookingDeadLetterQueue() {
        // 死信队列也要 durable，否则 Broker 重启后失败消息的存放位置可能丢失。
        return QueueBuilder.durable(BOOKING_DLQ).build();
    }

    @Bean
    Binding bookingBinding(@Qualifier("bookingQueue") Queue bookingQueue,
                           @Qualifier("bookingExchange") DirectExchange bookingExchange) {
        // Binding 是 exchange 到 queue 的路由规则：消息 routing key 等于 BOOKING_ROUTING_KEY 才进正常队列。
        return BindingBuilder.bind(bookingQueue).to(bookingExchange).with(BOOKING_ROUTING_KEY);
    }

    @Bean
    Binding bookingDeadLetterBinding(@Qualifier("bookingDeadLetterQueue") Queue bookingDeadLetterQueue,
                                     @Qualifier("bookingDeadLetterExchange") DirectExchange bookingDeadLetterExchange) {
        // 死信绑定把 DLX 上的失败消息转入 DLQ，控制台里能看到 hotel.booking.created.dlq 的 ready 消息。
        return BindingBuilder.bind(bookingDeadLetterQueue)
                .to(bookingDeadLetterExchange)
                .with(BOOKING_DLQ_ROUTING_KEY);
    }

    @Bean
    MessageConverter messageConverter() {
        // RabbitTemplate 和 @RabbitListener 使用同一个 JSON 转换器，Java record 会自动序列化/反序列化。
        return new Jackson2JsonMessageConverter();
    }
}
