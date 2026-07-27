package com.example.hotel.message.infrastructure.persistence;

import com.example.hotel.message.application.port.out.KafkaDemoIdempotencyStore;
import com.example.hotel.message.domain.model.KafkaDemoMessage;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

/**
 * 用数据库唯一键演示消费端业务幂等。
 *
 * <p>去重记录和订单投影写入同一个本地事务，避免“先标记已处理、业务却没有成功”的错误窗口。</p>
 */
@Repository
public class KafkaDemoIdempotencyRepository implements KafkaDemoIdempotencyStore {

    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;

    public KafkaDemoIdempotencyRepository(JdbcTemplate jdbcTemplate,
                                          TransactionTemplate transactionTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.transactionTemplate = transactionTemplate;
    }

    @Override
    public boolean processOnce(KafkaDemoMessage message) {
        try {
            transactionTemplate.executeWithoutResult(status -> {
                Instant now = Instant.now();
                jdbcTemplate.update("""
                                INSERT INTO kafka_demo_processed_message
                                (message_id, order_id, business_key, processed_at)
                                VALUES (?, ?, ?, ?)
                                """,
                        message.messageId(),
                        message.orderId(),
                        message.businessKey(),
                        Timestamp.from(now));

                jdbcTemplate.update("""
                                INSERT INTO kafka_demo_order_projection
                                (order_id, message_id, business_key, amount, updated_at)
                                VALUES (?, ?, ?, ?, ?)
                                """,
                        message.orderId(),
                        message.messageId(),
                        message.businessKey(),
                        message.amount(),
                        Timestamp.from(now));
            });
            return true;
        } catch (DuplicateKeyException duplicate) {
            try {
                transactionTemplate.executeWithoutResult(status -> {
                    jdbcTemplate.update("""
                                    INSERT INTO kafka_demo_processed_message
                                    (message_id, order_id, business_key, processed_at)
                                    VALUES (?, ?, ?, ?)
                                    """,
                            message.messageId(),
                            message.orderId(),
                            message.businessKey(),
                            Timestamp.from(Instant.now()));
                    updateProjection(message);
                });
                return true;
            } catch (DuplicateKeyException processedDuplicate) {
                return false;
            }
        }
    }

    @Override
    public List<String> recentProcessedMessageIds(int limit) {
        return jdbcTemplate.query("""
                        SELECT message_id
                        FROM kafka_demo_processed_message
                        ORDER BY processed_at DESC
                        LIMIT ?
                        """,
                (rs, rowNum) -> rs.getString("message_id"),
                limit);
    }

    private void updateProjection(KafkaDemoMessage message) {
        jdbcTemplate.update("""
                        UPDATE kafka_demo_order_projection
                        SET message_id = ?, business_key = ?, amount = ?, updated_at = ?
                        WHERE order_id = ?
                        """,
                message.messageId(),
                message.businessKey(),
                message.amount(),
                Timestamp.from(Instant.now()),
                message.orderId());
    }
}
