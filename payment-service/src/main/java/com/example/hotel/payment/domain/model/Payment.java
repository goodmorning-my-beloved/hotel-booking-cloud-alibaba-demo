package com.example.hotel.payment.domain.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;

/**
 * 支付聚合根。退款是显式状态迁移，不再只是一次无记录的接口返回。
 */
public final class Payment {

    private final String paymentId;
    private final String orderId;
    private final Long userId;
    private final BigDecimal amount;
    private final Instant paidAt;
    private PaymentStatus status;

    private Payment(String paymentId, String orderId, Long userId, BigDecimal amount, Instant paidAt) {
        this.paymentId = requireText(paymentId, "paymentId");
        this.orderId = requireText(orderId, "orderId");
        this.userId = Objects.requireNonNull(userId, "userId is required");
        if (amount == null || amount.signum() <= 0) {
            throw new IllegalArgumentException("amount must be positive");
        }
        this.amount = amount;
        this.paidAt = Objects.requireNonNull(paidAt, "paidAt is required");
        this.status = PaymentStatus.PAID;
    }

    public static Payment paid(String paymentId,
                               String orderId,
                               Long userId,
                               BigDecimal amount,
                               Instant paidAt) {
        return new Payment(paymentId, orderId, userId, amount, paidAt);
    }

    public void refund() {
        status = PaymentStatus.REFUNDED;
    }

    public String paymentId() {
        return paymentId;
    }

    public String orderId() {
        return orderId;
    }

    public Long userId() {
        return userId;
    }

    public BigDecimal amount() {
        return amount;
    }

    public Instant paidAt() {
        return paidAt;
    }

    public PaymentStatus status() {
        return status;
    }

    private static String requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " is required");
        }
        return value;
    }
}
