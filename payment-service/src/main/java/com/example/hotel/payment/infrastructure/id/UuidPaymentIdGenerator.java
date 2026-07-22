package com.example.hotel.payment.infrastructure.id;

import com.example.hotel.payment.application.port.out.PaymentIdGenerator;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class UuidPaymentIdGenerator implements PaymentIdGenerator {

    @Override
    public String nextId() {
        return "PAY-" + UUID.randomUUID().toString().substring(0, 8);
    }
}
