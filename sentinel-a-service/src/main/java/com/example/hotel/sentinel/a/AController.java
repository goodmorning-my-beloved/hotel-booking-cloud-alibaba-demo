package com.example.hotel.sentinel.a;

import com.alibaba.csp.sentinel.annotation.SentinelResource;
import com.alibaba.csp.sentinel.slots.block.BlockException;
import com.example.hotel.common.api.ApiResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
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
    public ApiResponse<Map<String, Object>> work(@RequestParam(name = "slowMs", required = false) Long slowMs,
                                                 @RequestParam(name = "fail", required = false) Boolean fail,
                                                 @RequestParam(name = "decodeFail", required = false) Boolean decodeFail) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("service", "sentinel-a-service");
        body.put("aTime", Instant.now().toString());
        body.put("slowMs", slowMs);
        body.put("fail", fail);
        body.put("decodeFail", decodeFail);
        body.put("bResponse", bServiceClient.work(slowMs, fail, decodeFail));
        return ApiResponse.ok(body);
    }

    public ApiResponse<Map<String, Object>> workBlocked(Long slowMs, Boolean fail, Boolean decodeFail,
                                                        BlockException ex) {
        return ApiResponse.fail("Sentinel blocked chainAWork: " + ex.getClass().getSimpleName());
    }
}
