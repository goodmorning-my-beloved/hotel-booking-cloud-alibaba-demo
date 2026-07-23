package com.example.hotel.order.interfaces.rest;

import com.example.hotel.common.api.ApiResponse;
import com.example.hotel.order.application.command.BookRoomCommand;
import com.example.hotel.order.application.port.in.BookRoomUseCase;
import com.example.hotel.order.application.port.in.OrderQueryUseCase;
import com.example.hotel.order.application.result.BookingView;
import com.example.hotel.order.interfaces.rest.dto.BookRoomRequest;
import com.example.hotel.order.interfaces.rest.dto.BookingResponse;
import com.alibaba.csp.sentinel.annotation.SentinelResource;
import com.alibaba.csp.sentinel.slots.block.BlockException;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final BookRoomUseCase bookRoomUseCase;
    private final OrderQueryUseCase orderQueryUseCase;

    public OrderController(BookRoomUseCase bookRoomUseCase, OrderQueryUseCase orderQueryUseCase) {
        this.bookRoomUseCase = bookRoomUseCase;
        this.orderQueryUseCase = orderQueryUseCase;
    }

    @PostMapping("/book")
    @SentinelResource(value = "bookRoom", blockHandler = "bookBlocked")
    public ApiResponse<BookingResponse> book(@RequestBody BookRoomRequest request) {
        BookingView booking = bookRoomUseCase.bookRoom(new BookRoomCommand(
                request.userId(), request.roomId(), request.checkIn(), request.checkOut()));
        return ApiResponse.ok(toResponse(booking));
    }

    public ApiResponse<BookingResponse> bookBlocked(BookRoomRequest request, BlockException ex) {
        return ApiResponse.fail("Booking is temporarily throttled, please retry later.");
    }

    @GetMapping("/{orderId}")
    public ApiResponse<BookingResponse> findById(@PathVariable("orderId") String orderId) {
        BookingView order = orderQueryUseCase.getOrder(orderId).orElse(null);
        if (order == null) {
            return ApiResponse.fail("Order not found: " + orderId);
        }
        return ApiResponse.ok(toResponse(order));
    }

    @GetMapping
    public ApiResponse<List<BookingResponse>> list() {
        return ApiResponse.ok(orderQueryUseCase.listOrders().stream().map(this::toResponse).toList());
    }

    private BookingResponse toResponse(BookingView booking) {
        return new BookingResponse(
                booking.orderId(), booking.userId(), booking.roomId(), booking.amount(), booking.status());
    }
}
