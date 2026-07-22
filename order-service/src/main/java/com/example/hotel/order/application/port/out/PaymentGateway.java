package com.example.hotel.order.application.port.out;

import java.math.BigDecimal;

public interface PaymentGateway {

    PaymentReceipt pay(String orderId, Long userId, BigDecimal amount);

    void refund(String orderId);

    record PaymentReceipt(String paymentId, String status) {
    }
}
