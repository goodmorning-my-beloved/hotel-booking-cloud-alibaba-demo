package com.example.hotel.hotel.domain.model;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 房型库存聚合根。订单号是预订幂等键，库存约束由聚合统一维护。
 */
public final class Room {

    private final Long id;
    private final String hotelName;
    private final String roomType;
    private final BigDecimal pricePerNight;
    private final int capacity;
    private final Map<String, StayPeriod> reservations = new LinkedHashMap<>();

    public Room(Long id, String hotelName, String roomType, BigDecimal pricePerNight, int capacity) {
        if (id == null || hotelName == null || hotelName.isBlank() || roomType == null || roomType.isBlank()) {
            throw new IllegalArgumentException("Room identity and names are required.");
        }
        if (pricePerNight == null || pricePerNight.signum() <= 0 || capacity <= 0) {
            throw new IllegalArgumentException("Room price and capacity must be positive.");
        }
        this.id = id;
        this.hotelName = hotelName;
        this.roomType = roomType;
        this.pricePerNight = pricePerNight;
        this.capacity = capacity;
    }

    public synchronized void reserve(String orderId, StayPeriod stayPeriod) {
        requireOrderId(orderId);
        if (reservations.containsKey(orderId)) {
            return;
        }
        if (reservations.size() >= capacity) {
            throw new IllegalStateException("No room available for the requested stay: " + id);
        }
        reservations.put(orderId, stayPeriod);
    }

    public synchronized void release(String orderId) {
        requireOrderId(orderId);
        reservations.remove(orderId);
    }

    public synchronized int available() {
        return capacity - reservations.size();
    }

    public Long id() {
        return id;
    }

    public String hotelName() {
        return hotelName;
    }

    public String roomType() {
        return roomType;
    }

    public BigDecimal pricePerNight() {
        return pricePerNight;
    }

    private void requireOrderId(String orderId) {
        if (orderId == null || orderId.isBlank()) {
            throw new IllegalArgumentException("orderId is required.");
        }
    }
}
