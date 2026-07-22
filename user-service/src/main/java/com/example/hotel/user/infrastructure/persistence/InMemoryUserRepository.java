package com.example.hotel.user.infrastructure.persistence;

import com.example.hotel.user.domain.model.HotelUser;
import com.example.hotel.user.domain.model.MembershipLevel;
import com.example.hotel.user.domain.repository.UserRepository;
import org.springframework.stereotype.Repository;

import java.util.Map;
import java.util.Optional;

@Repository
public class InMemoryUserRepository implements UserRepository {

    private final Map<Long, HotelUser> users = Map.of(
            1L, new HotelUser(1L, "Alice", MembershipLevel.GOLD),
            2L, new HotelUser(2L, "Bob", MembershipLevel.SILVER)
    );

    @Override
    public Optional<HotelUser> findById(Long userId) {
        return Optional.ofNullable(users.get(userId));
    }
}
