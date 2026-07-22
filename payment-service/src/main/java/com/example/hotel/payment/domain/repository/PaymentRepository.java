package com.example.hotel.payment.domain.repository;

import com.example.hotel.payment.domain.model.Payment;

import java.util.Optional;

public interface PaymentRepository {

    Optional<Payment> findByOrderId(String orderId);

    void save(Payment payment);
}
