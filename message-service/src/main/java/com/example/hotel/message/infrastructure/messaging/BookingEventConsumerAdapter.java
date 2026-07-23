package com.example.hotel.message.infrastructure.messaging;

import com.example.hotel.message.application.service.BookingEventApplicationService;
import com.example.hotel.message.domain.model.BookingCreatedDetails;
import com.example.hotel.message.infrastructure.messaging.dto.BookingCreatedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.function.Consumer;

/**
 * 完整下单链路的消息入站适配器。
 *
 * <p>Spring Cloud Stream 根据 application.yml 中的 binding，把 RabbitMQ/Kafka 的消息反序列化成
 * 本上下文拥有的 BookingCreatedMessage，再翻译成本地领域对象。这里不直接处理 HTTP 返回对象，也不混入 RabbitMQ demo 的
 * ACK/DLQ 教学代码，避免两个场景互相耦合。</p>
 */
@Component
public class BookingEventConsumerAdapter {

    private static final Logger log = LoggerFactory.getLogger(BookingEventConsumerAdapter.class);

    private final BookingEventApplicationService bookingEventService;

    public BookingEventConsumerAdapter(BookingEventApplicationService bookingEventService) {
        this.bookingEventService = bookingEventService;
    }

    @Bean
    public Consumer<BookingCreatedMessage> bookingCreatedRabbit() {
        return event -> record("rabbitmq", event);
    }

    @Bean
    public Consumer<BookingCreatedMessage> bookingCreatedKafka() {
        return event -> record("kafka", event);
    }

    private void record(String source, BookingCreatedMessage event) {
        bookingEventService.record(source, new BookingCreatedDetails(
                event.orderId(),
                event.userId(),
                event.roomId(),
                event.amount(),
                event.createdAt()));
        log.info("Received {} booking event: {}", source, event);
    }
}
