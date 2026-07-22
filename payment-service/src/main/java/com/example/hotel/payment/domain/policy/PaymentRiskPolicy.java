package com.example.hotel.payment.domain.policy;

import com.example.hotel.payment.domain.exception.PaymentRejectedException;

import java.math.BigDecimal;

public final class PaymentRiskPolicy {

    private static final BigDecimal SINGLE_PAYMENT_LIMIT = new BigDecimal("1500.00");

    private PaymentRiskPolicy() {
    }

    public static void check(BigDecimal amount) {
        if (amount == null || amount.signum() <= 0) {
            throw new PaymentRejectedException("Payment amount must be positive.");
        }
        if (amount.compareTo(SINGLE_PAYMENT_LIMIT) > 0) {
            throw new PaymentRejectedException("Payment risk control rejected amount: " + amount);
        }
    }
}
