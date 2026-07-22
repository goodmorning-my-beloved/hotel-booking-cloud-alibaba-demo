package com.example.hotel.user.interfaces.rest;

import com.alibaba.csp.sentinel.annotation.SentinelResource;
import com.alibaba.csp.sentinel.slots.block.BlockException;
import com.example.hotel.common.api.ApiResponse;
import com.example.hotel.common.dto.UserDto;
import com.example.hotel.user.application.port.in.UserQueryUseCase;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/users")
public class UserController {

    private final UserQueryUseCase userQueryUseCase;

    public UserController(UserQueryUseCase userQueryUseCase) {
        this.userQueryUseCase = userQueryUseCase;
    }

    @GetMapping("/{id}")
    public ApiResponse<UserDto> findById(@PathVariable("id") Long id) {
        return userQueryUseCase.getUser(id)
                .map(user -> ApiResponse.ok(new UserDto(user.id(), user.name(), user.membershipLevel())))
                .orElseGet(() -> ApiResponse.fail("User not found: " + id));
    }

    @GetMapping("/lab/sentinel")
    @SentinelResource(value = "userSentinelLab", blockHandler = "sentinelLabBlocked")
    public ApiResponse<String> sentinelLab() {
        return ApiResponse.ok("userSentinelLab passed");
    }

    public ApiResponse<String> sentinelLabBlocked(BlockException ex) {
        return ApiResponse.fail("Sentinel blocked userSentinelLab");
    }
}
