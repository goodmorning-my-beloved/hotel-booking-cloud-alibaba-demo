package com.example.hotel.message.infrastructure.persistence;

import com.example.hotel.message.domain.model.RabbitMqDlqIncident;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Repository
public class RabbitMqDlqIncidentRepository {

    public static final String STATUS_PENDING = "PENDING";
    public static final String STATUS_RESOLVING = "RESOLVING";
    public static final String STATUS_RESOLVED = "RESOLVED";
    public static final String STATUS_FAILED = "FAILED";

    private final JdbcTemplate jdbcTemplate;

    public RabbitMqDlqIncidentRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public RabbitMqDlqIncident insertPendingIfAbsent(String incidentId,
                                                     String sourceKey,
                                                     String queueName,
                                                     String messageId,
                                                     String orderId,
                                                     String deadLetterReason,
                                                     String payloadJson,
                                                     String headersJson) {
        // source_key 是幂等保护：如果监听器落库成功但 ACK DLQ 前服务宕机，RabbitMQ 会重投；
        // 第二次监听到同一条死信时不再创建新任务，而是返回第一次创建的 incident。
        Instant now = Instant.now();
        try {
            jdbcTemplate.update("""
                            INSERT INTO mq_dlq_incident
                            (incident_id, source_key, queue_name, message_id, order_id, dead_letter_reason,
                             payload_json, headers_json, status, compensation_note, last_error,
                             created_at, updated_at, resolved_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, NULL)
                            """,
                    incidentId,
                    sourceKey,
                    queueName,
                    messageId,
                    orderId,
                    deadLetterReason,
                    payloadJson,
                    headersJson,
                    STATUS_PENDING,
                    Timestamp.from(now),
                    Timestamp.from(now));
        } catch (DuplicateKeyException ignored) {
            // 已经存在说明这条 DLQ 消息之前被登记过，直接复用原任务，避免人工看到重复工单。
        }
        return findBySourceKey(sourceKey);
    }

    public RabbitMqDlqIncident findById(String incidentId) {
        List<RabbitMqDlqIncident> rows = jdbcTemplate.query("""
                        SELECT incident_id, source_key, queue_name, message_id, order_id, dead_letter_reason,
                               status, compensation_note, last_error, created_at, updated_at, resolved_at
                        FROM mq_dlq_incident
                        WHERE incident_id = ?
                        """,
                incidentRowMapper(),
                incidentId);
        return rows.isEmpty() ? null : rows.get(0);
    }

    public RabbitMqDlqIncident findBySourceKey(String sourceKey) {
        List<RabbitMqDlqIncident> rows = jdbcTemplate.query("""
                        SELECT incident_id, source_key, queue_name, message_id, order_id, dead_letter_reason,
                               status, compensation_note, last_error, created_at, updated_at, resolved_at
                        FROM mq_dlq_incident
                        WHERE source_key = ?
                        """,
                incidentRowMapper(),
                sourceKey);
        return rows.isEmpty() ? null : rows.get(0);
    }

    public boolean markResolving(String incidentId) {
        // 只允许 PENDING 或 FAILED 的任务进入处理中，避免已完成任务被重复补偿。
        int updated = jdbcTemplate.update("""
                        UPDATE mq_dlq_incident
                        SET status = ?, updated_at = ?
                        WHERE incident_id = ? AND status IN (?, ?)
                        """,
                STATUS_RESOLVING,
                Timestamp.from(Instant.now()),
                incidentId,
                STATUS_PENDING,
                STATUS_FAILED);
        return updated > 0;
    }

    public void markResolved(String incidentId, String compensationNote) {
        Instant now = Instant.now();
        jdbcTemplate.update("""
                        UPDATE mq_dlq_incident
                        SET status = ?, compensation_note = ?, last_error = NULL,
                            updated_at = ?, resolved_at = ?
                        WHERE incident_id = ?
                        """,
                STATUS_RESOLVED,
                abbreviate(compensationNote, 512),
                Timestamp.from(now),
                Timestamp.from(now),
                incidentId);
    }

    public void markFailed(String incidentId, String reason) {
        jdbcTemplate.update("""
                        UPDATE mq_dlq_incident
                        SET status = ?, last_error = ?, updated_at = ?
                        WHERE incident_id = ?
                        """,
                STATUS_FAILED,
                abbreviate(reason, 1000),
                Timestamp.from(Instant.now()),
                incidentId);
    }

    public List<RabbitMqDlqIncident> findByStatus(String status, int limit) {
        return jdbcTemplate.query("""
                        SELECT incident_id, source_key, queue_name, message_id, order_id, dead_letter_reason,
                               status, compensation_note, last_error, created_at, updated_at, resolved_at
                        FROM mq_dlq_incident
                        WHERE status = ?
                        ORDER BY updated_at DESC
                        LIMIT ?
                        """,
                incidentRowMapper(),
                status,
                limit);
    }

    public List<RabbitMqDlqIncident> recent(int limit) {
        return jdbcTemplate.query("""
                        SELECT incident_id, source_key, queue_name, message_id, order_id, dead_letter_reason,
                               status, compensation_note, last_error, created_at, updated_at, resolved_at
                        FROM mq_dlq_incident
                        ORDER BY updated_at DESC
                        LIMIT ?
                        """,
                incidentRowMapper(),
                limit);
    }

    public Map<String, Long> countByStatus() {
        List<Map.Entry<String, Long>> rows = jdbcTemplate.query("""
                        SELECT status, COUNT(*) AS total
                        FROM mq_dlq_incident
                        GROUP BY status
                        ORDER BY status
                        """,
                (rs, rowNum) -> Map.entry(rs.getString("status"), rs.getLong("total")));
        Map<String, Long> counts = new LinkedHashMap<>();
        for (Map.Entry<String, Long> row : rows) {
            counts.put(row.getKey(), row.getValue());
        }
        return counts;
    }

    private RowMapper<RabbitMqDlqIncident> incidentRowMapper() {
        return (rs, rowNum) -> new RabbitMqDlqIncident(
                rs.getString("incident_id"),
                rs.getString("source_key"),
                rs.getString("queue_name"),
                rs.getString("message_id"),
                rs.getString("order_id"),
                rs.getString("dead_letter_reason"),
                rs.getString("status"),
                rs.getString("compensation_note"),
                rs.getString("last_error"),
                toInstant(rs, "created_at"),
                toInstant(rs, "updated_at"),
                toInstant(rs, "resolved_at"));
    }

    private Instant toInstant(ResultSet rs, String column) throws SQLException {
        Timestamp timestamp = rs.getTimestamp(column);
        return timestamp == null ? null : timestamp.toInstant();
    }

    private String abbreviate(String value, int maxLength) {
        if (value == null || value.length() <= maxLength) {
            return value;
        }
        return value.substring(0, maxLength);
    }
}
