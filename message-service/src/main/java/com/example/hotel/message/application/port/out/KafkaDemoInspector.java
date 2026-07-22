package com.example.hotel.message.application.port.out;

import com.example.hotel.message.domain.model.KafkaDemoConsumerLag;
import com.example.hotel.message.domain.model.KafkaDemoTopicInfo;

import java.util.List;

public interface KafkaDemoInspector {

    List<KafkaDemoTopicInfo> topics();

    List<KafkaDemoConsumerLag> consumerLags();
}
