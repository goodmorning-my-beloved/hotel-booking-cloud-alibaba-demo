package com.example.hotel.message.application.port.out;

import java.util.List;

public interface KafkaDemoTopology {

    String demoTopic();

    String deadLetterTopic();

    String primaryConsumerGroup();

    String auditConsumerGroup();

    String deadLetterConsumerGroup();

    int partitionCount();

    String keyForPartition(int partition);

    List<String> demoTopics();

    List<String> demoConsumerGroups();
}
