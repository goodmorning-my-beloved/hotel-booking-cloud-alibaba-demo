package com.example.hotel.sentinel.a;

import com.example.hotel.common.api.ApiResponse;
import org.springframework.cloud.openfeign.FallbackFactory;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class BServiceClientFallbackFactory implements FallbackFactory<BServiceClient> {

    @Override
    public BServiceClient create(Throwable cause) {
        return (slowMs, fail) -> ApiResponse.fail("FallbackFactory from sentinel-a-service to sentinel-b-service: "
                + cause.getClass().getSimpleName() + " - " + cause.getMessage());
    }
}
