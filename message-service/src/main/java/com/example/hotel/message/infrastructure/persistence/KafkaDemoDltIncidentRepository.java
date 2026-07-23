package com.example.hotel.message.infrastructure.persistence;

import com.example.hotel.message.application.port.out.KafkaDemoDltIncidentStore;
import com.example.hotel.message.domain.model.KafkaDemoDltIncident;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

@Repository
public class KafkaDemoDltIncidentRepository implements KafkaDemoDltIncidentStore {

    public static final String STATUS_PENDING = "PENDING";
    public static final String STATUS_RESOLVED = "RESOLVED";

    private final JdbcTemplate jdbcTemplate;

    public KafkaDemoDltIncidentRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public KafkaDemoDltIncident saveIfAbsent(KafkaDemoDltIncident incident, String payloadBase64) {
        try {
            jdbcTemplate.update("""
                            INSERT INTO kafka_demo_dlt_incident
                            (incident_id, source_key, dlt_topic, dlt_partition, dlt_offset,
                             original_topic, original_partition, original_offset,
                             message_id, business_key, poison_payload, exception_type, exception_message,
                             payload_preview, payload_base64, status, resolution_note,
                             created_at, updated_at, resolved_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, NULL)
                            """,
                    incident.incidentId(),
                    incident.sourceKey(),
                    incident.dltTopic(),
                    incident.dltPartition(),
                    incident.dltOffset(),
                    incident.originalTopic(),
                    incident.originalPartition(),
                    incident.originalOffset(),
                    incident.messageId(),
                    incident.businessKey(),
                    incident.poisonPayload(),
                    abbreviate(incident.exceptionType(), 512),
                    abbreviate(incident.exceptionMessage(), 1024),
                    abbreviate(incident.payloadPreview(), 1024),
                    payloadBase64,
                    STATUS_PENDING,
                    Timestamp.from(incident.createdAt()),
                    Timestamp.from(incident.updatedAt()));
        } catch (DuplicateKeyException ignored) {
            // DLT 工单已落库但 offset 尚未提交时进程崩溃会导致重投；source_key 保证只建一张工单。
        }
        return findBySourceKey(incident.sourceKey());
    }

    @Override
    public List<KafkaDemoDltIncident> recent(int limit) {
        return jdbcTemplate.query("""
                        SELECT incident_id, source_key, dlt_topic, dlt_partition, dlt_offset,
                               original_topic, original_partition, original_offset,
                               message_id, business_key, poison_payload, exception_type, exception_message,
                               payload_preview, status, resolution_note, created_at, updated_at, resolved_at
                        FROM kafka_demo_dlt_incident
                        ORDER BY updated_at DESC
                        LIMIT ?
                        """,
                rowMapper(),
                limit);
    }

    @Override
    public void resolve(String incidentId, String resolutionNote) {
        Instant now = Instant.now();
        jdbcTemplate.update("""
                        UPDATE kafka_demo_dlt_incident
                        SET status = ?, resolution_note = ?, updated_at = ?, resolved_at = ?
                        WHERE incident_id = ?
                        """,
                STATUS_RESOLVED,
                abbreviate(resolutionNote, 512),
                Timestamp.from(now),
                Timestamp.from(now),
                incidentId);
    }

    private KafkaDemoDltIncident findBySourceKey(String sourceKey) {
        List<KafkaDemoDltIncident> rows = jdbcTemplate.query("""
                        SELECT incident_id, source_key, dlt_topic, dlt_partition, dlt_offset,
                               original_topic, original_partition, original_offset,
                               message_id, business_key, poison_payload, exception_type, exception_message,
                               payload_preview, status, resolution_note, created_at, updated_at, resolved_at
                        FROM kafka_demo_dlt_incident
                        WHERE source_key = ?
                        """,
                rowMapper(),
                sourceKey);
        return rows.isEmpty() ? null : rows.get(0);
    }

    private RowMapper<KafkaDemoDltIncident> rowMapper() {
        return (rs, rowNum) -> new KafkaDemoDltIncident(
                rs.getString("incident_id"),
                rs.getString("source_key"),
                rs.getString("dlt_topic"),
                rs.getInt("dlt_partition"),
                rs.getLong("dlt_offset"),
                rs.getString("original_topic"),
                nullableInt(rs, "original_partition"),
                nullableLong(rs, "original_offset"),
                rs.getString("message_id"),
                rs.getString("business_key"),
                rs.getBoolean("poison_payload"),
                rs.getString("exception_type"),
                rs.getString("exception_message"),
                rs.getString("payload_preview"),
                rs.getString("status"),
                rs.getString("resolution_note"),
                toInstant(rs, "created_at"),
                toInstant(rs, "updated_at"),
                toInstant(rs, "resolved_at"));
    }

    private Integer nullableInt(ResultSet rs, String column) throws SQLException {
        int value = rs.getInt(column);
        return rs.wasNull() ? null : value;
    }

    private Long nullableLong(ResultSet rs, String column) throws SQLException {
        long value = rs.getLong(column);
        return rs.wasNull() ? null : value;
    }

    private Instant toInstant(ResultSet rs, String column) throws SQLException {
        Timestamp value = rs.getTimestamp(column);
        return value == null ? null : value.toInstant();
    }

    private String abbreviate(String value, int maxLength) {
        if (value == null || value.length() <= maxLength) {
            return value;
        }
        return value.substring(0, maxLength);
    }
}
