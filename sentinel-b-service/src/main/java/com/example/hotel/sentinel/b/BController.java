package com.example.hotel.sentinel.b;

import com.alibaba.csp.sentinel.annotation.SentinelResource;
import com.alibaba.csp.sentinel.slots.block.BlockException;
import com.example.hotel.common.api.ApiResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

@RestController
@RequestMapping("/b")
public class BController {

    @GetMapping("/work")
    @SentinelResource(value = "chainBWork", blockHandler = "workBlocked")
    public ApiResponse<Map<String, Object>> work(@RequestParam(name = "slowMs", required = false) Long slowMs,
                                                 @RequestParam(name = "fail", required = false) Boolean fail,
                                                 @RequestParam(name = "decodeFail", required = false) Boolean decodeFail)
            throws InterruptedException, IOException {
        if (slowMs != null && slowMs > 0) {
            Thread.sleep(Math.min(slowMs, 5000L));
        }
        if (Boolean.TRUE.equals(fail)) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Demo failure from sentinel-b-service");
        }
        if (Boolean.TRUE.equals(decodeFail)) {
            var attributes = (ServletRequestAttributes) RequestContextHolder.currentRequestAttributes();
            var response = attributes.getResponse();
            if (response != null) {
                response.setStatus(HttpStatus.OK.value());
                response.setContentType(MediaType.APPLICATION_JSON_VALUE);
                response.getWriter().write("{\"success\":true,\"message\":\"OK\",\"data\":");
                response.flushBuffer();
            }
            return null;
        }

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("service", "sentinel-b-service");
        body.put("bTime", Instant.now().toString());
        body.put("slowMs", slowMs);
        body.put("fail", fail);
        body.put("decodeFail", decodeFail);
        return ApiResponse.ok(body);
    }

    public ApiResponse<Map<String, Object>> workBlocked(Long slowMs, Boolean fail, Boolean decodeFail,
                                                        BlockException ex) {
        return ApiResponse.fail("Sentinel blocked chainBWork: " + ex.getClass().getSimpleName());
    }
}
