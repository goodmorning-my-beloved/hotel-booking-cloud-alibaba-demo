package com.example.hotel.user.domain.repository;

import com.example.hotel.user.domain.model.HotelUser;

import java.util.Optional;

public interface UserRepository {

    Optional<HotelUser> findById(Long userId);
}
