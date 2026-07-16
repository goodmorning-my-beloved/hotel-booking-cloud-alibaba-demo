package com.example.hotel.sentinel.a;

import com.example.hotel.common.api.ApiResponse;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class BServiceClientFallback implements BServiceClient {

    @Override
    public ApiResponse<Map<String, Object>> work() {
        return ApiResponse.fail("Sentinel fallback from sentinel-a-service to sentinel-b-service");
    }
}
