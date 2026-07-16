package com.example.hotel.sentinel.b;

import com.alibaba.csp.sentinel.annotation.SentinelResource;
import com.alibaba.csp.sentinel.slots.block.BlockException;
import com.example.hotel.common.api.ApiResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

@RestController
@RequestMapping("/b")
public class BController {

    @GetMapping("/work")
    @SentinelResource(value = "chainBWork", blockHandler = "workBlocked")
    public ApiResponse<Map<String, Object>> work() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("service", "sentinel-b-service");
        body.put("bTime", Instant.now().toString());
        return ApiResponse.ok(body);
    }

    public ApiResponse<Map<String, Object>> workBlocked(BlockException ex) {
        return ApiResponse.fail("Sentinel blocked chainBWork");
    }
}
