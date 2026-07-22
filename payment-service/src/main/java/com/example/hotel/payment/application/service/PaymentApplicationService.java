package com.example.hotel.payment.application.service;

import com.example.hotel.payment.application.command.PayOrderCommand;
import com.example.hotel.payment.application.port.in.PaymentUseCase;
import com.example.hotel.payment.application.port.out.PaymentIdGenerator;
import com.example.hotel.payment.application.result.PaymentView;
import com.example.hotel.payment.domain.model.Payment;
import com.example.hotel.payment.domain.policy.PaymentRiskPolicy;
import com.example.hotel.payment.domain.repository.PaymentRepository;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Service
public class PaymentApplicationService implements PaymentUseCase {

    private final PaymentRepository paymentRepository;
    private final PaymentIdGenerator paymentIdGenerator;

    public PaymentApplicationService(PaymentRepository paymentRepository, PaymentIdGenerator paymentIdGenerator) {
        this.paymentRepository = paymentRepository;
        this.paymentIdGenerator = paymentIdGenerator;
    }

    @Override
    public PaymentView pay(PayOrderCommand command) {
        if (command == null) {
            throw new IllegalArgumentException("Payment command is required.");
        }
        return paymentRepository.findByOrderId(command.orderId())
                .map(this::toView)
                .orElseGet(() -> createPayment(command));
    }

    @Override
    public PaymentView refund(String orderId) {
        Payment payment = paymentRepository.findByOrderId(orderId)
                .orElseThrow(() -> new IllegalArgumentException("Payment not found for order: " + orderId));
        payment.refund();
        paymentRepository.save(payment);
        return toView(payment);
    }

    private PaymentView createPayment(PayOrderCommand command) {
        PaymentRiskPolicy.check(command.amount());
        Payment payment = Payment.paid(
                paymentIdGenerator.nextId(), command.orderId(), command.userId(), command.amount(), Instant.now());
        paymentRepository.save(payment);
        return toView(payment);
    }

    private PaymentView toView(Payment payment) {
        return new PaymentView(payment.paymentId(), payment.orderId(), payment.status().name());
    }
}
