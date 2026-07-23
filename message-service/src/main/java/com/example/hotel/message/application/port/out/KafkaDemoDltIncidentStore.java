package com.example.hotel.message.application.port.out;

import com.example.hotel.message.domain.model.KafkaDemoDltIncident;

import java.util.List;

/**
 * DLT 工单持久化端口。以 DLT topic/partition/offset 作为幂等来源。
 */
public interface KafkaDemoDltIncidentStore {

    KafkaDemoDltIncident saveIfAbsent(KafkaDemoDltIncident incident, String payloadBase64);

    List<KafkaDemoDltIncident> recent(int limit);

    void resolve(String incidentId, String resolutionNote);
}
