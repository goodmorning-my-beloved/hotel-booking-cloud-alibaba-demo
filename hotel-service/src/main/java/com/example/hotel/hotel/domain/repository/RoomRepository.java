package com.example.hotel.hotel.domain.repository;

import com.example.hotel.hotel.domain.model.Room;

import java.util.List;
import java.util.Optional;

public interface RoomRepository {

    Optional<Room> findById(Long roomId);

    List<Room> findAll();

    void save(Room room);
}
