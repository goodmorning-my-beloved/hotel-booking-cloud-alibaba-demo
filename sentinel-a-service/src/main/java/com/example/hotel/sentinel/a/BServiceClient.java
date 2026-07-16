package com.example.hotel.sentinel.a;

import com.example.hotel.common.api.ApiResponse;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.Map;

@FeignClient(name = "sentinel-b-service", fallbackFactory = BServiceClientFallbackFactory.class)
public interface BServiceClient {

    @GetMapping("/b/work")
    ApiResponse<Map<String, Object>> work(@RequestParam(name = "slowMs", required = false) Long slowMs,
                                          @RequestParam(name = "fail", required = false) Boolean fail,
                                          @RequestParam(name = "decodeFail", required = false) Boolean decodeFail);
}
