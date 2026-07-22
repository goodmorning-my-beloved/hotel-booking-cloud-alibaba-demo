package com.example.hotel.message.infrastructure.persistence;

import com.example.hotel.message.application.port.out.RabbitMqOutboxStore;

import com.example.hotel.message.domain.model.RabbitMqBookingMessage;
import com.example.hotel.message.domain.model.RabbitMqOutboxMessage;
import com.example.hotel.message.domain.model.RabbitMqOutboxPublishTask;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
public class RabbitMqOutboxRepository implements RabbitMqOutboxStore {

    public static final String STATUS_PENDING = "PENDING";
    public static final String STATUS_WAIT_CONFIRM = "WAIT_CONFIRM";
    public static final String STATUS_SENT = "SENT";
    public static final String STATUS_RETRYING = "RETRYING";
    public static final String STATUS_RETURNED = "RETURNED";
    public static final String STATUS_FAILED = "FAILED";

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public RabbitMqOutboxRepository(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    public RabbitMqOutboxPublishTask insertNew(String outboxId,
                                               RabbitMqBookingMessage payload,
                                               String exchangeName,
                                               String routingKey,
                                               String demoNote,
                                               int maxAttempts) {
        Instant now = Instant.now();
        jdbcTemplate.update("""
                        INSERT INTO mq_outbox_message
                        (outbox_id, message_id, order_id, exchange_name, routing_key, payload_json,
                         demo_note, status, attempt_count, max_attempts, next_retry_at, last_error,
                         created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, NULL, NULL, ?, ?)
                        """,
                outboxId,
                payload.messageId(),
                payload.orderId(),
                exchangeName,
                routingKey,
                toJson(payload),
                demoNote,
                STATUS_PENDING,
                maxAttempts,
                Timestamp.from(now),
                Timestamp.from(now));
        return new RabbitMqOutboxPublishTask(outboxId, exchangeName, routingKey, payload, demoNote);
    }

    public void markWaitConfirm(String outboxId) {
        jdbcTemplate.update("""
                        UPDATE mq_outbox_message
                        SET status = ?, attempt_count = attempt_count + 1, next_retry_at = NULL,
                            last_error = NULL, updated_at = ?
                        WHERE outbox_id = ?
                        """,
                STATUS_WAIT_CONFIRM,
                Timestamp.from(Instant.now()),
                outboxId);
    }

    public void markSent(String outboxId) {
        // return callback 和 confirm callback 都是异步的。若 mandatory return 已经把状态改成 RETURNED，
        // 后到的 broker ack 只能说明 exchange 收到了消息，不能再覆盖“没有路由到队列”的结果。
        jdbcTemplate.update("""
                        UPDATE mq_outbox_message
                        SET status = ?, next_retry_at = NULL, last_error = NULL, updated_at = ?
                        WHERE outbox_id = ? AND status <> ?
                        """,
                STATUS_SENT,
                Timestamp.from(Instant.now()),
                outboxId,
                STATUS_RETURNED);
    }

    public void markReturned(String outboxId, String reason) {
        jdbcTemplate.update("""
                        UPDATE mq_outbox_message
                        SET status = ?, next_retry_at = NULL, last_error = ?, updated_at = ?
                        WHERE outbox_id = ?
                        """,
                STATUS_RETURNED,
                abbreviate(reason),
                Timestamp.from(Instant.now()),
                outboxId);
    }

    public void markRetrying(String outboxId, String reason, Instant nextRetryAt) {
        jdbcTemplate.update("""
                        UPDATE mq_outbox_message
                        SET status = ?, next_retry_at = ?, last_error = ?, updated_at = ?
                        WHERE outbox_id = ? AND status NOT IN (?, ?)
                        """,
                STATUS_RETRYING,
                Timestamp.from(nextRetryAt),
                abbreviate(reason),
                Timestamp.from(Instant.now()),
                outboxId,
                STATUS_SENT,
                STATUS_RETURNED);
    }

    public void markFailed(String outboxId, String reason) {
        jdbcTemplate.update("""
                        UPDATE mq_outbox_message
                        SET status = ?, next_retry_at = NULL, last_error = ?, updated_at = ?
                        WHERE outbox_id = ? AND status <> ?
                        """,
                STATUS_FAILED,
                abbreviate(reason),
                Timestamp.from(Instant.now()),
                outboxId,
                STATUS_SENT);
    }

    public int attemptCount(String outboxId) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT attempt_count FROM mq_outbox_message WHERE outbox_id = ?",
                Integer.class,
                outboxId);
        return count == null ? 0 : count;
    }

    public List<RabbitMqOutboxPublishTask> findRetryable(Instant now, int limit) {
        return jdbcTemplate.query("""
                        SELECT outbox_id, exchange_name, routing_key, payload_json, demo_note
                        FROM mq_outbox_message
                        WHERE status = ? AND next_retry_at <= ? AND attempt_count < max_attempts
                        ORDER BY updated_at ASC
                        LIMIT ?
                        """,
                publishTaskRowMapper(),
                STATUS_RETRYING,
                Timestamp.from(now),
                limit);
    }

    public List<RabbitMqOutboxPublishTask> findConfirmTimedOut(Instant threshold, int limit) {
        return jdbcTemplate.query("""
                        SELECT outbox_id, exchange_name, routing_key, payload_json, demo_note
                        FROM mq_outbox_message
                        WHERE status = ? AND updated_at <= ? AND attempt_count < max_attempts
                        ORDER BY updated_at ASC
                        LIMIT ?
                        """,
                publishTaskRowMapper(),
                STATUS_WAIT_CONFIRM,
                Timestamp.from(threshold),
                limit);
    }

    public RabbitMqOutboxMessage findById(String outboxId) {
        return jdbcTemplate.queryForObject("""
                        SELECT outbox_id, message_id, order_id, exchange_name, routing_key, status,
                               attempt_count, max_attempts, next_retry_at, last_error, created_at, updated_at
                        FROM mq_outbox_message
                        WHERE outbox_id = ?
                        """,
                outboxMessageRowMapper(),
                outboxId);
    }

    public List<RabbitMqOutboxMessage> recent(int limit) {
        return jdbcTemplate.query("""
                        SELECT outbox_id, message_id, order_id, exchange_name, routing_key, status,
                               attempt_count, max_attempts, next_retry_at, last_error, created_at, updated_at
                        FROM mq_outbox_message
                        ORDER BY updated_at DESC
                        LIMIT ?
                        """,
                outboxMessageRowMapper(),
                limit);
    }

    public Map<String, Long> countByStatus() {
        List<Map.Entry<String, Long>> rows = jdbcTemplate.query("""
                        SELECT status, COUNT(*) AS total
                        FROM mq_outbox_message
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

    private RowMapper<RabbitMqOutboxPublishTask> publishTaskRowMapper() {
        return (rs, rowNum) -> new RabbitMqOutboxPublishTask(
                rs.getString("outbox_id"),
                rs.getString("exchange_name"),
                rs.getString("routing_key"),
                fromJson(rs.getString("payload_json")),
                rs.getString("demo_note"));
    }

    private RowMapper<RabbitMqOutboxMessage> outboxMessageRowMapper() {
        return (rs, rowNum) -> new RabbitMqOutboxMessage(
                rs.getString("outbox_id"),
                rs.getString("message_id"),
                rs.getString("order_id"),
                rs.getString("exchange_name"),
                rs.getString("routing_key"),
                rs.getString("status"),
                rs.getInt("attempt_count"),
                rs.getInt("max_attempts"),
                toInstant(rs, "next_retry_at"),
                rs.getString("last_error"),
                toInstant(rs, "created_at"),
                toInstant(rs, "updated_at"));
    }

    private String toJson(RabbitMqBookingMessage payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Failed to serialize RabbitMQ outbox payload", ex);
        }
    }

    private RabbitMqBookingMessage fromJson(String json) {
        try {
            return objectMapper.readValue(json, RabbitMqBookingMessage.class);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Failed to deserialize RabbitMQ outbox payload", ex);
        }
    }

    private Instant toInstant(ResultSet rs, String column) throws SQLException {
        Timestamp timestamp = rs.getTimestamp(column);
        return timestamp == null ? null : timestamp.toInstant();
    }

    private String abbreviate(String value) {
        if (value == null || value.length() <= 1000) {
            return value;
        }
        return value.substring(0, 1000);
    }
}
