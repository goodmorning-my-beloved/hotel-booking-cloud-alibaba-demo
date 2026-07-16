package com.example.hotel.sentinel.bff;

import com.example.hotel.common.api.ApiResponse;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.Map;

@FeignClient(name = "sentinel-a-service", fallbackFactory = AServiceClientFallbackFactory.class)
public interface AServiceClient {

    @GetMapping("/a/work")
    ApiResponse<Map<String, Object>> work(@RequestParam(name = "slowMs", required = false) Long slowMs,
                                          @RequestParam(name = "fail", required = false) Boolean fail);
}
