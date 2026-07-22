package com.example.hotel.order.infrastructure.id;

import com.example.hotel.order.application.port.out.OrderIdGenerator;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class UuidOrderIdGenerator implements OrderIdGenerator {

    @Override
    public String nextId() {
        return "ORD-" + UUID.randomUUID().toString().substring(0, 8);
    }
}
