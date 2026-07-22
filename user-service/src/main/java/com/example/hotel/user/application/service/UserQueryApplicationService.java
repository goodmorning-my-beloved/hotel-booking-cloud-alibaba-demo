package com.example.hotel.user.application.service;

import com.example.hotel.user.application.port.in.UserQueryUseCase;
import com.example.hotel.user.application.result.UserView;
import com.example.hotel.user.domain.repository.UserRepository;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
public class UserQueryApplicationService implements UserQueryUseCase {

    private final UserRepository userRepository;

    public UserQueryApplicationService(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    @Override
    public Optional<UserView> getUser(Long userId) {
        return userRepository.findById(userId)
                .map(user -> new UserView(user.id(), user.name(), user.membershipLevel().name()));
    }
}
