package com.example.hotel.order.application.port.out;

public interface UserProfileGateway {

    UserProfile getRequiredUser(Long userId);

    record UserProfile(Long id, String name, String level) {
    }
}
