package com.example.hotel.order.infrastructure.client;

import com.example.hotel.common.api.ApiResponse;
import com.example.hotel.order.infrastructure.client.dto.UserApiResponse;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

@FeignClient(name = "user-service")
public interface UserClient {

    @GetMapping("/users/{id}")
    ApiResponse<UserApiResponse> findById(@PathVariable("id") Long id);
}
