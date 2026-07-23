package com.example.hotel.payment.interfaces.rest;

import com.alibaba.csp.sentinel.annotation.SentinelResource;
import com.alibaba.csp.sentinel.slots.block.BlockException;
import com.example.hotel.common.api.ApiResponse;
import com.example.hotel.payment.application.command.PayOrderCommand;
import com.example.hotel.payment.application.port.in.PaymentUseCase;
import com.example.hotel.payment.application.result.PaymentView;
import com.example.hotel.payment.interfaces.rest.dto.PayOrderRequest;
import com.example.hotel.payment.interfaces.rest.dto.PaymentResponse;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/payments")
public class PaymentController {

    private final PaymentUseCase paymentUseCase;

    public PaymentController(PaymentUseCase paymentUseCase) {
        this.paymentUseCase = paymentUseCase;
    }

    @PostMapping("/pay")
    @SentinelResource(value = "payOrder", blockHandler = "payBlocked")
    public ApiResponse<PaymentResponse> pay(@RequestBody PayOrderRequest request) {
        PaymentView payment = paymentUseCase.pay(
                new PayOrderCommand(request.orderId(), request.userId(), request.amount()));
        return ApiResponse.ok(toResponse(payment));
    }

    @PostMapping("/{orderId}/refund")
    public ApiResponse<PaymentResponse> refund(@PathVariable("orderId") String orderId) {
        return ApiResponse.ok(toResponse(paymentUseCase.refund(orderId)));
    }

    public ApiResponse<PaymentResponse> payBlocked(PayOrderRequest request, BlockException ex) {
        return ApiResponse.fail("Sentinel blocked payOrder, please retry later.");
    }

    private PaymentResponse toResponse(PaymentView payment) {
        return new PaymentResponse(payment.paymentId(), payment.status());
    }
}
