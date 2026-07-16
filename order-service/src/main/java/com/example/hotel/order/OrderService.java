package com.example.hotel.order;

import com.alibaba.csp.sentinel.annotation.SentinelResource;
import com.alibaba.csp.sentinel.slots.block.BlockException;
import com.example.hotel.common.api.ApiResponse;
import com.example.hotel.common.dto.BookingCreatedEvent;
import com.example.hotel.common.dto.BookingRequest;
import com.example.hotel.common.dto.BookingResponse;
import com.example.hotel.common.dto.PaymentRequest;
import com.example.hotel.common.dto.PaymentResponse;
import com.example.hotel.common.dto.ReserveRoomRequest;
import com.example.hotel.common.dto.RoomDto;
import com.example.hotel.common.dto.UserDto;
import com.example.hotel.order.client.HotelClient;
import com.example.hotel.order.client.PaymentClient;
import com.example.hotel.order.client.UserClient;
import io.seata.spring.annotation.GlobalTransactional;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class OrderService {

    private final UserClient userClient;
    private final HotelClient hotelClient;
    private final PaymentClient paymentClient;
    private final StreamBridge streamBridge;
    private final Map<String, BookingResponse> orders = new ConcurrentHashMap<>();

    public OrderService(UserClient userClient, HotelClient hotelClient, PaymentClient paymentClient, StreamBridge streamBridge) {
        this.userClient = userClient;
        this.hotelClient = hotelClient;
        this.paymentClient = paymentClient;
        this.streamBridge = streamBridge;
    }

    @GlobalTransactional(name = "hotel-booking-create", rollbackFor = Exception.class)
    @SentinelResource(value = "bookRoom", blockHandler = "bookBlocked")
    public BookingResponse book(BookingRequest request) {
        validateStay(request);
        UserDto user = requireOk(userClient.findById(request.userId()), "User check failed");
        RoomDto room = requireOk(hotelClient.findRoom(request.roomId()), "Room check failed");

        String orderId = "ORD-" + UUID.randomUUID().toString().substring(0, 8);
        boolean roomReserved = false;
        try {
            requireOk(hotelClient.reserve(request.roomId(),
                    new ReserveRoomRequest(orderId, request.checkIn(), request.checkOut())), "Reserve room failed");
            roomReserved = true;

            PaymentResponse payment = requireOk(paymentClient.pay(
                    new PaymentRequest(orderId, user.id(), room.price())), "Payment failed");

            BookingResponse response = new BookingResponse(orderId, user.id(), room.id(), room.price(), payment.status());
            orders.put(orderId, response);
            BookingCreatedEvent event = new BookingCreatedEvent(orderId, user.id(), room.id(), room.price(), Instant.now());
            streamBridge.send("bookingCreatedRabbit-out-0", event);
            streamBridge.send("bookingCreatedKafka-out-0", event);
            return response;
        } catch (RuntimeException ex) {
            if (roomReserved) {
                hotelClient.release(request.roomId());
            }
            throw ex;
        }
    }

    public BookingResponse bookBlocked(BookingRequest request, BlockException ex) {
        return new BookingResponse("BLOCKED", request.userId(), request.roomId(), null, "SENTINEL_BLOCKED");
    }

    public BookingResponse findById(String orderId) {
        return orders.get(orderId);
    }

    public Collection<BookingResponse> list() {
        return orders.values();
    }

    private void validateStay(BookingRequest request) {
        if (request.userId() == null || request.roomId() == null || request.checkIn() == null || request.checkOut() == null) {
            throw new IllegalArgumentException("userId, roomId, checkIn and checkOut are required.");
        }
        long nights = ChronoUnit.DAYS.between(request.checkIn(), request.checkOut());
        if (nights <= 0 || nights > 30) {
            throw new IllegalArgumentException("Stay length must be between 1 and 30 nights.");
        }
    }

    private <T> T requireOk(ApiResponse<T> response, String prefix) {
        if (response == null || !response.success()) {
            String message = response == null ? "empty response" : response.message();
            throw new IllegalStateException(prefix + ": " + message);
        }
        return response.data();
    }
}
