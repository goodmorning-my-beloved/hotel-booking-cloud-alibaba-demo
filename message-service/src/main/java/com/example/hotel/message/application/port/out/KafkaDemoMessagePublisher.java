package com.example.hotel.message.application.port.out;

import com.example.hotel.message.application.result.KafkaDemoPublishResult;
import com.example.hotel.message.application.result.KafkaDemoTransactionResult;
import com.example.hotel.message.domain.model.KafkaDemoMessage;

import java.util.List;

public interface KafkaDemoMessagePublisher {

    KafkaDemoPublishResult publish(String topic, KafkaDemoMessage message, String note);

    List<KafkaDemoPublishResult> publishAsyncBatch(String topic,
                                                   List<KafkaDemoMessage> messages,
                                                   String note);

    KafkaDemoPublishResult publishRaw(String topic,
                                      String messageId,
                                      String businessKey,
                                      byte[] payload,
                                      String note);

    KafkaDemoTransactionResult publishTransaction(String topic,
                                                  List<KafkaDemoMessage> messages,
                                                  boolean failAfterFirst);
}
