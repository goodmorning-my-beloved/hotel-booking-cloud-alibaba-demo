package com.example.hotel.order.infrastructure.client;

import com.example.hotel.common.dto.PaymentRequest;
import com.example.hotel.common.dto.PaymentResponse;
import com.example.hotel.order.application.port.out.PaymentGateway;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Component
public class FeignPaymentGateway implements PaymentGateway {

    private final PaymentClient paymentClient;

    public FeignPaymentGateway(PaymentClient paymentClient) {
        this.paymentClient = paymentClient;
    }

    @Override
    public PaymentReceipt pay(String orderId, Long userId, BigDecimal amount) {
        PaymentResponse response = RemoteResponse.requireData(
                paymentClient.pay(new PaymentRequest(orderId, userId, amount)), "Payment failed");
        return new PaymentReceipt(response.paymentId(), response.status());
    }

    @Override
    public void refund(String orderId) {
        RemoteResponse.requireData(paymentClient.refund(orderId), "Payment refund failed");
    }
}
