package com.example.hotel.order.infrastructure.client;

import com.example.hotel.common.api.ApiResponse;
import com.example.hotel.common.dto.PaymentRequest;
import com.example.hotel.common.dto.PaymentResponse;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

@FeignClient(name = "payment-service")
public interface PaymentClient {

    @PostMapping("/payments/pay")
    ApiResponse<PaymentResponse> pay(@RequestBody PaymentRequest request);
}
