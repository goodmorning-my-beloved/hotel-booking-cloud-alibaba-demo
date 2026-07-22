package com.example.hotel.hotel.interfaces.rest;

import com.alibaba.csp.sentinel.annotation.SentinelResource;
import com.alibaba.csp.sentinel.slots.block.BlockException;
import com.example.hotel.common.api.ApiResponse;
import com.example.hotel.common.dto.ReserveRoomRequest;
import com.example.hotel.common.dto.RoomDto;
import com.example.hotel.hotel.application.command.ReserveRoomCommand;
import com.example.hotel.hotel.application.port.in.RoomInventoryUseCase;
import com.example.hotel.hotel.application.result.RoomView;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping
public class HotelController {

    private final RoomInventoryUseCase roomInventoryUseCase;

    public HotelController(RoomInventoryUseCase roomInventoryUseCase) {
        this.roomInventoryUseCase = roomInventoryUseCase;
    }

    @GetMapping("/hotels")
    public ApiResponse<List<RoomDto>> listRooms() {
        return ApiResponse.ok(roomInventoryUseCase.listRooms().stream().map(this::toResponse).toList());
    }

    @GetMapping("/rooms/{roomId}")
    public ApiResponse<RoomDto> findRoom(@PathVariable("roomId") Long roomId) {
        return roomInventoryUseCase.getRoom(roomId)
                .map(room -> ApiResponse.ok(toResponse(room)))
                .orElseGet(() -> ApiResponse.fail("Room not found: " + roomId));
    }

    @PostMapping("/rooms/{roomId}/reserve")
    @SentinelResource(value = "reserveRoom", blockHandler = "reserveBlocked")
    public ApiResponse<RoomDto> reserve(@PathVariable("roomId") Long roomId,
                                        @RequestBody ReserveRoomRequest request) {
        RoomView room = roomInventoryUseCase.reserve(new ReserveRoomCommand(
                roomId, request.orderId(), request.checkIn(), request.checkOut()));
        return ApiResponse.ok(toResponse(room));
    }

    @PostMapping("/rooms/{roomId}/release")
    public ApiResponse<RoomDto> release(@PathVariable("roomId") Long roomId,
                                        @RequestParam("orderId") String orderId) {
        return ApiResponse.ok(toResponse(roomInventoryUseCase.release(roomId, orderId)));
    }

    public ApiResponse<RoomDto> reserveBlocked(Long roomId, ReserveRoomRequest request, BlockException ex) {
        return ApiResponse.fail("Sentinel blocked reserveRoom, please retry later.");
    }

    private RoomDto toResponse(RoomView room) {
        return new RoomDto(
                room.id(), room.hotelName(), room.roomType(), room.pricePerNight(), room.available());
    }
}
