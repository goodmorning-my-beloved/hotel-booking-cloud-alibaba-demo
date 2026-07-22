package com.example.hotel.hotel.interfaces.rest;

import com.example.hotel.common.api.ApiResponse;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class HotelExceptionHandler {

    @ExceptionHandler({IllegalArgumentException.class, IllegalStateException.class})
    public ApiResponse<Void> inventoryOperationFailed(RuntimeException exception) {
        return ApiResponse.fail(exception.getMessage());
    }
}
