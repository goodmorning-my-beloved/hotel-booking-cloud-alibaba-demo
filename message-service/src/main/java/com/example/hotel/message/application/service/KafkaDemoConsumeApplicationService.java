package com.example.hotel.message.application.service;

import com.example.hotel.message.application.port.out.KafkaDemoIdempotencyStore;
import com.example.hotel.message.domain.model.KafkaDemoMessage;
import org.springframework.stereotype.Service;

/**
 * Kafka 消费用例：把数据库幂等处理和教学指标组织在一起。
 */
@Service
public class KafkaDemoConsumeApplicationService {

    private final KafkaDemoIdempotencyStore idempotencyStore;
    private final KafkaDemoMetrics metrics;

    public KafkaDemoConsumeApplicationService(KafkaDemoIdempotencyStore idempotencyStore,
                                              KafkaDemoMetrics metrics) {
        this.idempotencyStore = idempotencyStore;
        this.metrics = metrics;
    }

    public boolean process(KafkaDemoMessage message) {
        boolean firstDelivery = idempotencyStore.processOnce(message);
        if (firstDelivery) {
            metrics.consumed();
        } else {
            metrics.duplicated();
        }
        return firstDelivery;
    }
}
