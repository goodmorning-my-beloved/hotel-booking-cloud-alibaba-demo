package com.example.hotel.order.infrastructure.persistence;

import com.example.hotel.order.domain.model.BookingOrder;
import com.example.hotel.order.domain.model.BookingStatus;
import com.example.hotel.order.domain.model.StayPeriod;
import com.example.hotel.order.domain.repository.OrderRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;

@Repository
public class JdbcOrderRepository implements OrderRepository {

    private final JdbcTemplate jdbcTemplate;

    public JdbcOrderRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void save(BookingOrder order) {
        jdbcTemplate.update("""
                        INSERT INTO booking_order
                        (order_id, user_id, room_id, check_in, check_out, amount, status, payment_id, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON DUPLICATE KEY UPDATE
                            user_id = VALUES(user_id),
                            room_id = VALUES(room_id),
                            check_in = VALUES(check_in),
                            check_out = VALUES(check_out),
                            amount = VALUES(amount),
                            status = VALUES(status),
                            payment_id = VALUES(payment_id),
                            created_at = VALUES(created_at)
                        """,
                order.orderId(),
                order.userId(),
                order.roomId(),
                order.stayPeriod().checkIn(),
                order.stayPeriod().checkOut(),
                order.amount(),
                order.status().name(),
                order.paymentId(),
                Timestamp.from(order.createdAt()));
    }

    @Override
    public Optional<BookingOrder> findById(String orderId) {
        List<BookingOrder> rows = jdbcTemplate.query("""
                        SELECT order_id, user_id, room_id, check_in, check_out, amount,
                               status, payment_id, created_at
                        FROM booking_order
                        WHERE order_id = ?
                        """,
                this::toOrder,
                orderId);
        return rows.stream().findFirst();
    }

    @Override
    public List<BookingOrder> findAll() {
        return jdbcTemplate.query("""
                        SELECT order_id, user_id, room_id, check_in, check_out, amount,
                               status, payment_id, created_at
                        FROM booking_order
                        ORDER BY created_at DESC
                        """,
                this::toOrder);
    }

    @Override
    public void deleteById(String orderId) {
        jdbcTemplate.update("DELETE FROM booking_order WHERE order_id = ?", orderId);
    }

    private BookingOrder toOrder(ResultSet rs, int rowNum) throws SQLException {
        BookingOrder order = BookingOrder.createPending(
                rs.getString("order_id"),
                rs.getLong("user_id"),
                rs.getLong("room_id"),
                new StayPeriod(rs.getDate("check_in").toLocalDate(), rs.getDate("check_out").toLocalDate()),
                rs.getBigDecimal("amount"),
                rs.getTimestamp("created_at").toInstant());
        if (BookingStatus.PAID.name().equals(rs.getString("status"))) {
            order.confirmPayment(rs.getString("payment_id"));
        }
        return order;
    }
}
