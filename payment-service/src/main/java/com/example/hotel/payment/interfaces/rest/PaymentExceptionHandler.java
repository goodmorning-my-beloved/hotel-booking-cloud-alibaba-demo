package com.example.hotel.payment.interfaces.rest;

import com.example.hotel.common.api.ApiResponse;
import com.example.hotel.payment.domain.exception.PaymentRejectedException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class PaymentExceptionHandler {

    @ExceptionHandler(PaymentRejectedException.class)
    public ApiResponse<Void> paymentRejected(PaymentRejectedException exception) {
        return ApiResponse.fail(exception.getMessage());
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ApiResponse<Void> invalidPayment(IllegalArgumentException exception) {
        return ApiResponse.fail(exception.getMessage());
    }
}
