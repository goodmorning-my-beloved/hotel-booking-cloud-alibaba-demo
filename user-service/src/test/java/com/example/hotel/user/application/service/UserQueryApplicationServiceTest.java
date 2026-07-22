package com.example.hotel.user.application.service;

import com.example.hotel.user.infrastructure.persistence.InMemoryUserRepository;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class UserQueryApplicationServiceTest {

    private final UserQueryApplicationService service =
            new UserQueryApplicationService(new InMemoryUserRepository());

    @Test
    void returnsApplicationViewInsteadOfProtocolDto() {
        var user = service.getUser(1L);

        assertThat(user).isPresent();
        assertThat(user.orElseThrow().membershipLevel()).isEqualTo("GOLD");
    }
}
