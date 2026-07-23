package com.example.hotel.order.infrastructure.client;

import com.example.hotel.common.api.ApiResponse;
import com.example.hotel.order.infrastructure.client.dto.PaymentApiRequest;
import com.example.hotel.order.infrastructure.client.dto.PaymentApiResponse;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.PathVariable;

@FeignClient(name = "payment-service")
public interface PaymentClient {

    @PostMapping("/payments/pay")
    ApiResponse<PaymentApiResponse> pay(@RequestBody PaymentApiRequest request);

    @PostMapping("/payments/{orderId}/refund")
    ApiResponse<PaymentApiResponse> refund(@PathVariable("orderId") String orderId);
}
