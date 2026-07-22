package com.example.hotel.message.application.port.out;

import com.example.hotel.message.application.result.KafkaDemoPublishResult;
import com.example.hotel.message.domain.model.KafkaDemoMessage;

public interface KafkaDemoMessagePublisher {

    KafkaDemoPublishResult publish(String topic, KafkaDemoMessage message, String note);
}
