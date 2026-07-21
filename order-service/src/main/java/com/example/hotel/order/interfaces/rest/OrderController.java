package com.example.hotel.order.interfaces.rest;

import com.example.hotel.common.api.ApiResponse;
import com.example.hotel.common.dto.BookingRequest;
import com.example.hotel.common.dto.BookingResponse;
import com.example.hotel.order.application.service.OrderService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collection;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping("/book")
    public ApiResponse<BookingResponse> book(@RequestBody BookingRequest request) {
        return ApiResponse.ok(orderService.book(request));
    }

    @GetMapping("/{orderId}")
    public ApiResponse<BookingResponse> findById(@PathVariable("orderId") String orderId) {
        BookingResponse order = orderService.findById(orderId);
        if (order == null) {
            return ApiResponse.fail("Order not found: " + orderId);
        }
        return ApiResponse.ok(order);
    }

    @GetMapping
    public ApiResponse<Collection<BookingResponse>> list() {
        return ApiResponse.ok(orderService.list());
    }
}
