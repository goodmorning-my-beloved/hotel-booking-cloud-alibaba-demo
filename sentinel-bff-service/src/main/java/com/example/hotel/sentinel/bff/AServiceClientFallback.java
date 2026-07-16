package com.example.hotel.sentinel.bff;

import com.example.hotel.common.api.ApiResponse;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class AServiceClientFallback implements AServiceClient {

    @Override
    public ApiResponse<Map<String, Object>> work() {
        return ApiResponse.fail("Sentinel fallback from bff to sentinel-a-service");
    }
}
