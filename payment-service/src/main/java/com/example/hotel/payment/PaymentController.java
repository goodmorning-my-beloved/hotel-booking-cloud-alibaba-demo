package com.example.hotel.payment;

import com.alibaba.csp.sentinel.annotation.SentinelResource;
import com.alibaba.csp.sentinel.slots.block.BlockException;
import com.example.hotel.common.api.ApiResponse;
import com.example.hotel.common.dto.PaymentRequest;
import com.example.hotel.common.dto.PaymentResponse;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.UUID;

@RestController
@RequestMapping("/payments")
public class PaymentController {

    @PostMapping("/pay")
    @SentinelResource(value = "payOrder", blockHandler = "payBlocked")
    public ApiResponse<PaymentResponse> pay(@RequestBody PaymentRequest request) {
        if (request.amount().compareTo(new BigDecimal("1500.00")) > 0) {
            return ApiResponse.fail("Payment risk control rejected amount: " + request.amount());
        }
        String paymentId = "PAY-" + UUID.randomUUID().toString().substring(0, 8);
        return ApiResponse.ok(new PaymentResponse(paymentId, "PAID"));
    }

    public ApiResponse<PaymentResponse> payBlocked(PaymentRequest request, BlockException ex) {
        return ApiResponse.fail("Sentinel blocked payOrder, please retry later.");
    }
}
