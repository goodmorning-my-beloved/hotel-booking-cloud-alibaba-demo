package com.example.hotel.payment.infrastructure.persistence;

import com.example.hotel.payment.domain.model.Payment;
import com.example.hotel.payment.domain.repository.PaymentRepository;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryPaymentRepository implements PaymentRepository {

    private final Map<String, Payment> paymentsByOrderId = new ConcurrentHashMap<>();

    @Override
    public Optional<Payment> findByOrderId(String orderId) {
        return Optional.ofNullable(paymentsByOrderId.get(orderId));
    }

    @Override
    public void save(Payment payment) {
        paymentsByOrderId.put(payment.orderId(), payment);
    }
}
