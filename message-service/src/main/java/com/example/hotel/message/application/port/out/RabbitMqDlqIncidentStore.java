package com.example.hotel.message.application.port.out;

import com.example.hotel.message.domain.model.RabbitMqDlqIncident;

import java.util.List;
import java.util.Map;

public interface RabbitMqDlqIncidentStore {

    RabbitMqDlqIncident insertPendingIfAbsent(String incidentId,
                                              String sourceKey,
                                              String queueName,
                                              String messageId,
                                              String orderId,
                                              String deadLetterReason,
                                              String payloadJson,
                                              String headersJson);

    RabbitMqDlqIncident findById(String incidentId);

    boolean markResolving(String incidentId);

    void markResolved(String incidentId, String compensationNote);

    void markFailed(String incidentId, String reason);

    List<RabbitMqDlqIncident> findByStatus(String status, int limit);

    List<RabbitMqDlqIncident> recent(int limit);

    Map<String, Long> countByStatus();
}
