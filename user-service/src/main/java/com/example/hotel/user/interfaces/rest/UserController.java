package com.example.hotel.user.interfaces.rest;

import com.alibaba.csp.sentinel.annotation.SentinelResource;
import com.alibaba.csp.sentinel.slots.block.BlockException;
import com.example.hotel.common.api.ApiResponse;
import com.example.hotel.common.dto.UserDto;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/users")
public class UserController {

    private final Map<Long, UserDto> users = Map.of(
            1L, new UserDto(1L, "Alice", "GOLD"),
            2L, new UserDto(2L, "Bob", "SILVER")
    );

    @GetMapping("/{id}")
    public ApiResponse<UserDto> findById(@PathVariable("id") Long id) {
        UserDto user = users.get(id);
        if (user == null) {
            return ApiResponse.fail("User not found: " + id);
        }
        return ApiResponse.ok(user);
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
