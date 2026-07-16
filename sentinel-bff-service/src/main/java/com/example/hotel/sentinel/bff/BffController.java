package com.example.hotel.sentinel.bff;

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
@RequestMapping("/lab-chain")
public class BffController {

    private final AServiceClient aServiceClient;

    public BffController(AServiceClient aServiceClient) {
        this.aServiceClient = aServiceClient;
    }

    @GetMapping("/entry")
    @SentinelResource(value = "chainBffEntry", blockHandler = "entryBlocked")
    public ApiResponse<Map<String, Object>> entry() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("path", "gateway -> sentinel-bff-service -> sentinel-a-service -> sentinel-b-service");
        body.put("bffTime", Instant.now().toString());
        body.put("aResponse", aServiceClient.work());
        return ApiResponse.ok(body);
    }

    public ApiResponse<Map<String, Object>> entryBlocked(BlockException ex) {
        return ApiResponse.fail("Sentinel blocked chainBffEntry");
    }
}
