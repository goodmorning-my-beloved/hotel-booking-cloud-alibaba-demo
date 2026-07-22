package com.example.hotel.user.domain.model;

public record HotelUser(Long id, String name, MembershipLevel membershipLevel) {

    public HotelUser {
        if (id == null || name == null || name.isBlank() || membershipLevel == null) {
            throw new IllegalArgumentException("User identity, name and membership level are required.");
        }
    }
}
