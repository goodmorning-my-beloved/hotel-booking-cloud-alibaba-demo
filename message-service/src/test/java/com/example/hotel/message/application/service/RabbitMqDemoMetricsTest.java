package com.example.hotel.message.application.service;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RabbitMqDemoMetricsTest {

    @Test
    void exposesImmutableSnapshotAndIdempotencyDecision() {
        RabbitMqDemoMetrics metrics = new RabbitMqDemoMetrics();

        assertThat(metrics.markProcessed("MSG-1")).isTrue();
        assertThat(metrics.markProcessed("MSG-1")).isFalse();
        metrics.consumed();
        metrics.record("MSG-1", "ORD-1", "consumed", "done");

        RabbitMqDemoMetrics.Snapshot snapshot = metrics.snapshot();
        assertThat(snapshot.consumed()).isEqualTo(1);
        assertThat(snapshot.processedMessageIds()).containsExactly("MSG-1");
        assertThat(snapshot.recentEvents()).singleElement()
                .satisfies(event -> assertThat(event.status()).isEqualTo("consumed"));
    }
}
