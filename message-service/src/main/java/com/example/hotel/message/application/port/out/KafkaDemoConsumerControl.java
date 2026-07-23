package com.example.hotel.message.application.port.out;

/**
 * Kafka 学习实验的消费者控制端口，用于暂停消费并观察 lag。
 */
public interface KafkaDemoConsumerControl {

    void pausePrimary();

    void resumePrimary();

    boolean primaryPauseRequested();
}
