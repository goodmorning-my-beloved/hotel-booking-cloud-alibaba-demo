package com.example.hotel.message.infrastructure.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
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
    // topic exchange 用来演示通配符路由：binding key 可以包含 * 和 #。
    public static final String BOOKING_TOPIC_EXCHANGE = "hotel.booking.topic.exchange";
    // * 只匹配 routing key 中的一个单词，例如 hotel.booking.created。
    public static final String BOOKING_TOPIC_SINGLE_WORD_PATTERN = "hotel.booking.*";
    // # 可以匹配零个或多个单词，例如 hotel.booking.created、hotel.booking.payment.timeout。
    public static final String BOOKING_TOPIC_MULTI_WORD_PATTERN = "hotel.booking.#";
    public static final String BOOKING_TOPIC_SINGLE_WORD_QUEUE = "hotel.booking.topic.single-word.queue";
    public static final String BOOKING_TOPIC_MULTI_WORD_QUEUE = "hotel.booking.topic.multi-word.queue";
    // fanout exchange 用来演示广播：它忽略 routing key，把消息复制到所有绑定队列。
    public static final String BOOKING_FANOUT_EXCHANGE = "hotel.booking.fanout.exchange";
    public static final String BOOKING_FANOUT_SMS_QUEUE = "hotel.booking.fanout.sms.queue";
    public static final String BOOKING_FANOUT_POINTS_QUEUE = "hotel.booking.fanout.points.queue";

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
    TopicExchange bookingTopicExchange() {
        // topic exchange 仍然看 routing key，但 binding key 可以写通配符。
        // 面试里常问的点是：* 匹配一个词，# 匹配零个或多个词，词之间用英文点号分隔。
        return new TopicExchange(BOOKING_TOPIC_EXCHANGE, true, false);
    }

    @Bean
    FanoutExchange bookingFanoutExchange() {
        // fanout exchange 不关心 routing key，所有绑定到它的队列都会收到一份消息副本。
        // 典型场景是订单创建后同时通知短信、积分、站内信等多个下游。
        return new FanoutExchange(BOOKING_FANOUT_EXCHANGE, true, false);
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
    Queue bookingTopicSingleWordQueue() {
        // 这个队列只接收 hotel.booking.<一个词> 形态的 routing key。
        return QueueBuilder.durable(BOOKING_TOPIC_SINGLE_WORD_QUEUE).build();
    }

    @Bean
    Queue bookingTopicMultiWordQueue() {
        // 这个队列接收所有 hotel.booking 开头的事件，包含更多层级的 routing key。
        return QueueBuilder.durable(BOOKING_TOPIC_MULTI_WORD_QUEUE).build();
    }

    @Bean
    Queue bookingFanoutSmsQueue() {
        // fanout 下每个消费者队列都有自己的消息副本；短信服务处理失败不会直接影响积分服务队列。
        return QueueBuilder.durable(BOOKING_FANOUT_SMS_QUEUE).build();
    }

    @Bean
    Queue bookingFanoutPointsQueue() {
        // 用第二个队列演示广播效果：同一条消息会同时进入 sms.queue 和 points.queue。
        return QueueBuilder.durable(BOOKING_FANOUT_POINTS_QUEUE).build();
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
    Binding bookingTopicSingleWordBinding(@Qualifier("bookingTopicSingleWordQueue") Queue bookingTopicSingleWordQueue,
                                          @Qualifier("bookingTopicExchange") TopicExchange bookingTopicExchange) {
        // hotel.booking.* 只匹配一个单词，所以能收到 hotel.booking.created，
        // 但收不到 hotel.booking.payment.timeout 这种更深层级的 routing key。
        return BindingBuilder.bind(bookingTopicSingleWordQueue)
                .to(bookingTopicExchange)
                .with(BOOKING_TOPIC_SINGLE_WORD_PATTERN);
    }

    @Bean
    Binding bookingTopicMultiWordBinding(@Qualifier("bookingTopicMultiWordQueue") Queue bookingTopicMultiWordQueue,
                                         @Qualifier("bookingTopicExchange") TopicExchange bookingTopicExchange) {
        // hotel.booking.# 可以匹配 hotel.booking 后面的零个或多个单词，
        // 适合“某个业务域的全部事件”这种订阅关系。
        return BindingBuilder.bind(bookingTopicMultiWordQueue)
                .to(bookingTopicExchange)
                .with(BOOKING_TOPIC_MULTI_WORD_PATTERN);
    }

    @Bean
    Binding bookingFanoutSmsBinding(@Qualifier("bookingFanoutSmsQueue") Queue bookingFanoutSmsQueue,
                                    @Qualifier("bookingFanoutExchange") FanoutExchange bookingFanoutExchange) {
        // fanout binding 不需要 routing key，绑定关系本身就表示“我要收到广播”。
        return BindingBuilder.bind(bookingFanoutSmsQueue).to(bookingFanoutExchange);
    }

    @Bean
    Binding bookingFanoutPointsBinding(@Qualifier("bookingFanoutPointsQueue") Queue bookingFanoutPointsQueue,
                                       @Qualifier("bookingFanoutExchange") FanoutExchange bookingFanoutExchange) {
        // 第二个绑定队列会收到同一条广播消息，便于在控制台观察一发多收。
        return BindingBuilder.bind(bookingFanoutPointsQueue).to(bookingFanoutExchange);
    }

    @Bean
    MessageConverter messageConverter() {
        // RabbitTemplate 和 @RabbitListener 使用同一个 JSON 转换器，Java record 会自动序列化/反序列化。
        return new Jackson2JsonMessageConverter();
    }
}
