package com.example.hotel.sentinel.bff;

import com.example.hotel.common.api.ApiResponse;
import org.springframework.cloud.openfeign.FallbackFactory;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class AServiceClientFallbackFactory implements FallbackFactory<AServiceClient> {

    @Override
    public AServiceClient create(Throwable cause) {
        return (slowMs, fail) -> ApiResponse.fail("FallbackFactory from bff to sentinel-a-service: "
                + cause.getClass().getSimpleName() + " - " + cause.getMessage());
    }
}
