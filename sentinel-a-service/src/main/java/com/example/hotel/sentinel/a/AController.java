package com.example.hotel.sentinel.a;

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
@RequestMapping("/a")
public class AController {

    private final BServiceClient bServiceClient;

    public AController(BServiceClient bServiceClient) {
        this.bServiceClient = bServiceClient;
    }

    @GetMapping("/work")
    @SentinelResource(value = "chainAWork", blockHandler = "workBlocked")
    public ApiResponse<Map<String, Object>> work() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("service", "sentinel-a-service");
        body.put("aTime", Instant.now().toString());
        body.put("bResponse", bServiceClient.work());
        return ApiResponse.ok(body);
    }

    public ApiResponse<Map<String, Object>> workBlocked(BlockException ex) {
        return ApiResponse.fail("Sentinel blocked chainAWork");
    }
}
