package com.example.hotel.payment.infrastructure.persistence;

import com.example.hotel.payment.domain.model.Payment;
import com.example.hotel.payment.domain.model.PaymentStatus;
import com.example.hotel.payment.domain.repository.PaymentRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;

@Repository
public class JdbcPaymentRepository implements PaymentRepository {

    private final JdbcTemplate jdbcTemplate;

    public JdbcPaymentRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public Optional<Payment> findByOrderId(String orderId) {
        List<Payment> rows = jdbcTemplate.query("""
                        SELECT payment_id, order_id, user_id, amount, paid_at, status
                        FROM payment
                        WHERE order_id = ?
                        """,
                this::toPayment,
                orderId);
        return rows.stream().findFirst();
    }

    @Override
    public void save(Payment payment) {
        jdbcTemplate.update("""
                        INSERT INTO payment
                        (payment_id, order_id, user_id, amount, paid_at, status)
                        VALUES (?, ?, ?, ?, ?, ?)
                        ON DUPLICATE KEY UPDATE
                            payment_id = VALUES(payment_id),
                            order_id = VALUES(order_id),
                            user_id = VALUES(user_id),
                            amount = VALUES(amount),
                            paid_at = VALUES(paid_at),
                            status = VALUES(status)
                        """,
                payment.paymentId(),
                payment.orderId(),
                payment.userId(),
                payment.amount(),
                Timestamp.from(payment.paidAt()),
                payment.status().name());
    }

    private Payment toPayment(ResultSet rs, int rowNum) throws SQLException {
        Payment payment = Payment.paid(
                rs.getString("payment_id"),
                rs.getString("order_id"),
                rs.getLong("user_id"),
                rs.getBigDecimal("amount"),
                rs.getTimestamp("paid_at").toInstant());
        if (PaymentStatus.REFUNDED.name().equals(rs.getString("status"))) {
            payment.refund();
        }
        return payment;
    }
}
