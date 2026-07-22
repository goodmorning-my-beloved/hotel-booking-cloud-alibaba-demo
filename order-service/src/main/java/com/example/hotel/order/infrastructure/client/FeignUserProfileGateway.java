package com.example.hotel.order.infrastructure.client;

import com.example.hotel.common.dto.UserDto;
import com.example.hotel.order.application.port.out.UserProfileGateway;
import org.springframework.stereotype.Component;

@Component
public class FeignUserProfileGateway implements UserProfileGateway {

    private final UserClient userClient;

    public FeignUserProfileGateway(UserClient userClient) {
        this.userClient = userClient;
    }

    @Override
    public UserProfile getRequiredUser(Long userId) {
        UserDto user = RemoteResponse.requireData(userClient.findById(userId), "User check failed");
        return new UserProfile(user.id(), user.name(), user.level());
    }
}
