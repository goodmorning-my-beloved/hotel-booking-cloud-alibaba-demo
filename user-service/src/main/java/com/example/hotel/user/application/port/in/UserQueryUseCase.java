package com.example.hotel.user.application.port.in;

import com.example.hotel.user.application.result.UserView;

import java.util.Optional;

public interface UserQueryUseCase {

    Optional<UserView> getUser(Long userId);
}
