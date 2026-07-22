package com.example.hotel.payment.application.service;

import com.example.hotel.payment.application.command.PayOrderCommand;
import com.example.hotel.payment.application.port.out.PaymentIdGenerator;
import com.example.hotel.payment.domain.exception.PaymentRejectedException;
import com.example.hotel.payment.infrastructure.persistence.InMemoryPaymentRepository;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PaymentApplicationServiceTest {

    private final PaymentApplicationService service = new PaymentApplicationService(
            new InMemoryPaymentRepository(), fixedId());

    @Test
    void paymentIsIdempotentByOrderIdAndCanBeRefunded() {
        PayOrderCommand command = new PayOrderCommand("ORD-1", 1L, new BigDecimal("100.00"));

        var first = service.pay(command);
        var duplicate = service.pay(command);
        var refunded = service.refund("ORD-1");

        assertThat(duplicate.paymentId()).isEqualTo(first.paymentId());
        assertThat(refunded.status()).isEqualTo("REFUNDED");
    }

    @Test
    void riskPolicyRejectsAmountAboveLimit() {
        assertThatThrownBy(() -> service.pay(
                new PayOrderCommand("ORD-2", 1L, new BigDecimal("1500.01"))))
                .isInstanceOf(PaymentRejectedException.class);
    }

    private PaymentIdGenerator fixedId() {
        return () -> "PAY-1";
    }
}
