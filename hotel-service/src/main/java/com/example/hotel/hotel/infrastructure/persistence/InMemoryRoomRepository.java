package com.example.hotel.hotel.infrastructure.persistence;

import com.example.hotel.hotel.domain.model.Room;
import com.example.hotel.hotel.domain.repository.RoomRepository;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryRoomRepository implements RoomRepository {

    private final Map<Long, Room> rooms = new ConcurrentHashMap<>(Map.of(
            101L, new Room(101L, "Nacos Resort", "Lake View King", new BigDecimal("688.00"), 5),
            102L, new Room(102L, "Sentinel Inn", "Business Twin", new BigDecimal("428.00"), 8),
            201L, new Room(201L, "Seata Grand Hotel", "Cloud Terrace Suite", new BigDecimal("1288.00"), 2),
            301L, new Room(301L, "RabbitMQ Chalet", "Forest Family Room", new BigDecimal("858.00"), 4),
            302L, new Room(302L, "Kafka Vista", "Mountain View Twin", new BigDecimal("758.00"), 6),
            401L, new Room(401L, "Gateway Lodge", "Private Onsen Villa", new BigDecimal("1688.00"), 1)
    ));

    @Override
    public Optional<Room> findById(Long roomId) {
        return Optional.ofNullable(rooms.get(roomId));
    }

    @Override
    public List<Room> findAll() {
        return rooms.values().stream().sorted(Comparator.comparing(Room::id)).toList();
    }

    @Override
    public void save(Room room) {
        rooms.put(room.id(), room);
    }
}
