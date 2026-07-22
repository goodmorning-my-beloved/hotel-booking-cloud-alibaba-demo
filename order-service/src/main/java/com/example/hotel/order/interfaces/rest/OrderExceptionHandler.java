package com.example.hotel.order.interfaces.rest;

import com.example.hotel.common.api.ApiResponse;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class OrderExceptionHandler {

    @ExceptionHandler({IllegalArgumentException.class, IllegalStateException.class})
    public ApiResponse<Void> useCaseFailed(RuntimeException exception) {
        return ApiResponse.fail(exception.getMessage());
    }
}
