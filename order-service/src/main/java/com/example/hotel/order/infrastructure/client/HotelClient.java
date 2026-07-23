package com.example.hotel.order.infrastructure.client;

import com.example.hotel.common.api.ApiResponse;
import com.example.hotel.order.infrastructure.client.dto.ReserveRoomApiRequest;
import com.example.hotel.order.infrastructure.client.dto.RoomApiResponse;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

@FeignClient(name = "hotel-service")
public interface HotelClient {

    @GetMapping("/rooms/{roomId}")
    ApiResponse<RoomApiResponse> findRoom(@PathVariable("roomId") Long roomId);

    @PostMapping("/rooms/{roomId}/reserve")
    ApiResponse<RoomApiResponse> reserve(@PathVariable("roomId") Long roomId,
                                         @RequestBody ReserveRoomApiRequest request);

    @PostMapping("/rooms/{roomId}/release")
    ApiResponse<RoomApiResponse> release(@PathVariable("roomId") Long roomId,
                                         @RequestParam("orderId") String orderId);
}
