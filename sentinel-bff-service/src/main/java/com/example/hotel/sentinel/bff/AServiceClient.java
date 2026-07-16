package com.example.hotel.sentinel.bff;

import com.example.hotel.common.api.ApiResponse;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;

import java.util.Map;

@FeignClient(name = "sentinel-a-service", fallback = AServiceClientFallback.class)
public interface AServiceClient {

    @GetMapping("/a/work")
    ApiResponse<Map<String, Object>> work();
}
