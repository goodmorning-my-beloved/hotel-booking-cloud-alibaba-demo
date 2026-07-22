package com.example.hotel.order.application.port.out;

import com.example.hotel.order.domain.model.StayPeriod;

import java.math.BigDecimal;

public interface RoomInventoryGateway {

    RoomOffer getRequiredRoom(Long roomId);

    void reserve(Long roomId, String orderId, StayPeriod stayPeriod);

    void release(Long roomId, String orderId);

    record RoomOffer(Long id, BigDecimal price) {
    }
}
