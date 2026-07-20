package com.example.hotel.message;

import java.util.List;

public record RabbitMqDemoStatus(
        String exchange,
        String queue,
        String deadLetterExchange,
        String deadLetterQueue,
        long published,
        long publishAcked,
        long returned,
        long consumed,
        long duplicated,
        long rejectedToDeadLetter,
        List<String> processedMessageIds,
        List<RabbitMqConsumedEvent> recentEvents
) {
}
