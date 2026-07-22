package com.example.hotel.order.domain.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;

/**
 * 订单聚合根。状态迁移只能通过聚合行为完成，避免应用层随意拼装状态。
 */
public final class BookingOrder {

    private final String orderId;
    private final Long userId;
    private final Long roomId;
    private final StayPeriod stayPeriod;
    private final BigDecimal amount;
    private final Instant createdAt;
    private BookingStatus status;
    private String paymentId;

    private BookingOrder(String orderId,
                         Long userId,
                         Long roomId,
                         StayPeriod stayPeriod,
                         BigDecimal amount,
                         Instant createdAt) {
        this.orderId = requireText(orderId, "orderId");
        this.userId = Objects.requireNonNull(userId, "userId is required");
        this.roomId = Objects.requireNonNull(roomId, "roomId is required");
        this.stayPeriod = Objects.requireNonNull(stayPeriod, "stayPeriod is required");
        this.amount = requirePositive(amount);
        this.createdAt = Objects.requireNonNull(createdAt, "createdAt is required");
        this.status = BookingStatus.PENDING_PAYMENT;
    }

    public static BookingOrder createPending(String orderId,
                                             Long userId,
                                             Long roomId,
                                             StayPeriod stayPeriod,
                                             BigDecimal amount,
                                             Instant createdAt) {
        return new BookingOrder(orderId, userId, roomId, stayPeriod, amount, createdAt);
    }

    public void confirmPayment(String paymentId) {
        if (status != BookingStatus.PENDING_PAYMENT) {
            throw new IllegalStateException("Only a pending order can confirm payment.");
        }
        this.paymentId = requireText(paymentId, "paymentId");
        this.status = BookingStatus.PAID;
    }

    public String orderId() {
        return orderId;
    }

    public Long userId() {
        return userId;
    }

    public Long roomId() {
        return roomId;
    }

    public StayPeriod stayPeriod() {
        return stayPeriod;
    }

    public BigDecimal amount() {
        return amount;
    }

    public Instant createdAt() {
        return createdAt;
    }

    public BookingStatus status() {
        return status;
    }

    public String paymentId() {
        return paymentId;
    }

    private static String requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " is required");
        }
        return value;
    }

    private static BigDecimal requirePositive(BigDecimal amount) {
        if (amount == null || amount.signum() <= 0) {
            throw new IllegalArgumentException("amount must be positive");
        }
        return amount;
    }
}
