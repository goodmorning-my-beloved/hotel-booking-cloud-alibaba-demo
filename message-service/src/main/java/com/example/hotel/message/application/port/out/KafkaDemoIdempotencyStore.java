package com.example.hotel.message.application.port.out;

import com.example.hotel.message.domain.model.KafkaDemoMessage;

import java.util.List;

/**
 * 消费端业务幂等端口。实现必须把去重记录和业务副作用放在同一个本地事务中。
 */
public interface KafkaDemoIdempotencyStore {

    boolean processOnce(KafkaDemoMessage message);

    List<String> recentProcessedMessageIds(int limit);
}
