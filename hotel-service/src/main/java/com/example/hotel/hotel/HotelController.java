package com.example.hotel.hotel;

import com.alibaba.csp.sentinel.annotation.SentinelResource;
import com.alibaba.csp.sentinel.slots.block.BlockException;
import com.example.hotel.common.api.ApiResponse;
import com.example.hotel.common.dto.ReserveRoomRequest;
import com.example.hotel.common.dto.RoomDto;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@RestController
@RequestMapping
public class HotelController {

    private final Map<Long, RoomDto> rooms = new ConcurrentHashMap<>(Map.of(
            101L, new RoomDto(101L, "Nacos Resort", "Lake View King", new BigDecimal("688.00"), 5),
            102L, new RoomDto(102L, "Sentinel Inn", "Business Twin", new BigDecimal("428.00"), 8),
            201L, new RoomDto(201L, "Seata Grand Hotel", "Cloud Terrace Suite", new BigDecimal("1288.00"), 2),
            301L, new RoomDto(301L, "RabbitMQ Chalet", "Forest Family Room", new BigDecimal("858.00"), 4),
            302L, new RoomDto(302L, "Kafka Vista", "Mountain View Twin", new BigDecimal("758.00"), 6),
            401L, new RoomDto(401L, "Gateway Lodge", "Private Onsen Villa", new BigDecimal("1688.00"), 1)
    ));

    @GetMapping("/hotels")
    public ApiResponse<List<RoomDto>> listRooms() {
        List<RoomDto> result = new ArrayList<>(rooms.values());
        result.sort(Comparator.comparing(RoomDto::id));
        return ApiResponse.ok(result);
    }

    @GetMapping("/rooms/{roomId}")
    public ApiResponse<RoomDto> findRoom(@PathVariable("roomId") Long roomId) {
        RoomDto room = rooms.get(roomId);
        if (room == null) {
            return ApiResponse.fail("Room not found: " + roomId);
        }
        return ApiResponse.ok(room);
    }

    @PostMapping("/rooms/{roomId}/reserve")
    @SentinelResource(value = "reserveRoom", blockHandler = "reserveBlocked")
    public ApiResponse<RoomDto> reserve(@PathVariable("roomId") Long roomId, @RequestBody ReserveRoomRequest request) {
        RoomDto updated = rooms.compute(roomId, (id, room) -> {
            if (room == null) {
                throw new IllegalArgumentException("Room not found: " + roomId);
            }
            if (room.available() <= 0) {
                throw new IllegalStateException("No room available: " + roomId);
            }
            return new RoomDto(room.id(), room.hotelName(), room.roomType(), room.price(), room.available() - 1);
        });
        return ApiResponse.ok(updated);
    }

    @PostMapping("/rooms/{roomId}/release")
    public ApiResponse<RoomDto> release(@PathVariable("roomId") Long roomId) {
        RoomDto updated = rooms.compute(roomId, (id, room) -> {
            if (room == null) {
                throw new IllegalArgumentException("Room not found: " + roomId);
            }
            return new RoomDto(room.id(), room.hotelName(), room.roomType(), room.price(), room.available() + 1);
        });
        return ApiResponse.ok(updated);
    }

    public ApiResponse<RoomDto> reserveBlocked(Long roomId, ReserveRoomRequest request, BlockException ex) {
        return ApiResponse.fail("Sentinel blocked reserveRoom, please retry later.");
    }
}
