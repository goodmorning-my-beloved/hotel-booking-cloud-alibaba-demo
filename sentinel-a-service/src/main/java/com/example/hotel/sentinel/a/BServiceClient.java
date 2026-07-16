package com.example.hotel.sentinel.a;

import com.example.hotel.common.api.ApiResponse;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;

import java.util.Map;

@FeignClient(name = "sentinel-b-service", fallback = BServiceClientFallback.class)
public interface BServiceClient {

    @GetMapping("/b/work")
    ApiResponse<Map<String, Object>> work();
}
